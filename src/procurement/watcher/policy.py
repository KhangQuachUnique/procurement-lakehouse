"""Policy and extraction rules for late bid opening watch."""
import json
from datetime import UTC, date, datetime, timedelta
from typing import Any

from procurement.common.catalog import get_resource
from procurement.common.dates import VIETNAM_TZ
from procurement.common.settings import settings
from procurement.ingestion.engine.metadata import calculate_content_hash
from procurement.quality.contracts import load_config, lookup, validate_detail
from procurement.storage.committed import CommittedDay, iter_committed_records

NOTICE_ROOTS = {
    "notify_contractor_standard_detail": ("bidoNotifyContractorM", "bidNoContractorResponse.bidNotification"),
    "notify_contractor_vk_adb_detail": ("bidoNotifyContractorP",),
    "notify_contractor_reoffer_detail": ("",),
}


def namespace() -> str:
    """Namespace hash derived from current object storage endpoint and bucket."""
    return calculate_content_hash([settings.OBJECT_STORAGE_ENDPOINT, settings.OBJECT_STORAGE_BUCKET])


def selection_for(fs: Any, resource: str, manifest: Any) -> CommittedDay:
    """Build CommittedDay partition selection for given resource and manifest."""
    definition = get_resource(resource)
    files = []
    for table in definition.tables:
        prefix = (
            f"{settings.OBJECT_STORAGE_BUCKET}/bronze/{definition.identity.source}/{table}/"
            f"source_date={manifest.source_date}/run_id={manifest.run_id}"
        )
        files.extend((table, key) for key in sorted(fs.glob(f"{prefix}/*.parquet")))
    return CommittedDay(manifest.source_date, manifest.run_id, manifest.bronze_records, tuple(files))


def opening_states(fs: Any, manifest: Any) -> dict[tuple[Any, Any, Any], str]:
    """Extract validated collection statuses for committed bid openings."""
    if manifest is None:
        return {}
    config = load_config(resource="bid_opening")
    states: dict[tuple[Any, Any, Any], str] = {}
    for _, record in iter_committed_records(
        fs, (selection_for(fs, "bid_opening", manifest),), verify_hash=True,
    ):
        payload = record["payload"]
        if isinstance(payload, str):
            payload = json.loads(payload)
        root = lookup(payload, "notify.bidNoContractorResponse.bidNotification") or {}
        context = {
            "id": root.get("id"),
            "notifyNo": record["source_id"],
            "notifyVersion": record.get("source_version"),
        }
        result = validate_detail(payload, context, config.contracts["bid_opening"])
        if result["status"] not in {"pass", "warn"}:
            raise ValueError("Committed bid opening failed assembly validation")
        key = (context["id"], context["notifyNo"], context["notifyVersion"])
        if key in states:
            raise ValueError("Duplicate committed bid opening identity")
        states[key] = result["collection_status"]
    return states


def notice_context(table: str, record: dict[str, Any], evidence: list[dict[str, Any]]) -> dict[str, Any]:
    """Extract and validate normalized contractor notice context from record and evidence."""
    payload = record["payload"]
    if isinstance(payload, str):
        payload = json.loads(payload)
    roots = [lookup(payload, p) for p in NOTICE_ROOTS[table]]
    roots = [r for r in roots if isinstance(r, dict) and r]
    matches = [
        c for c in evidence
        if c.get("notifyNo") == record["source_id"] and c.get("notifyVersion") == record.get("source_version")
    ]
    candidates = roots + matches
    context: dict[str, Any] = {}
    for field in ("id", "notifyNo", "notifyVersion", "publicDate", "isInternet", "bidOpenDate", "bidCloseDate"):
        aliases = (field,)
        if table == "notify_contractor_reoffer_detail":
            alias = {
                "notifyNo": "reofferNo",
                "notifyVersion": "reofferVersion",
                "bidOpenDate": "reofferOpenDate",
                "bidCloseDate": "reofferCloseDate",
            }.get(field)
            if alias:
                aliases += (alias,)
        values = {str(c[name]) for c in candidates for name in aliases if c.get(name) is not None}
        if field == "isInternet":
            values = {"1" if v in {"True", "1"} else "0" if v in {"False", "0"} else v for v in values}
        if len(values) > 1:
            raise ValueError(f"Conflicting notice {field}")
        context[field] = next(iter(values), None)
    if (
        not context["id"]
        or not context["notifyNo"]
        or context["notifyVersion"] is None
        or not context["publicDate"]
        or context["isInternet"] not in {"0", "1"}
    ):
        raise ValueError("Missing notice identity/date/type")
    if context["notifyNo"] != record["source_id"] or context["notifyVersion"] != record.get("source_version"):
        raise ValueError("Notice identity differs from Bronze envelope")
    if date.fromisoformat(context["publicDate"][:10]) != record["source_date"]:
        raise ValueError("Notice publicDate differs from source_date")
    return context


def calculate_due_date(context: dict[str, Any], now: datetime) -> datetime:
    """Calculate the due datetime for a notice based on scheduled bid open or close date."""
    due = now
    scheduled = context.get("bidOpenDate") or context.get("bidCloseDate")
    if scheduled:
        parsed = datetime.fromisoformat(scheduled)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=VIETNAM_TZ)
        due = max(now, parsed.astimezone(UTC) + timedelta(hours=1))
    return due
