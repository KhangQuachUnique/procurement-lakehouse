"""Route by reviewed evidence and validate detail before accepting a Bronze record."""

from collections.abc import Iterator
from datetime import date
from typing import Any, Protocol

import httpx

from procurement.common.errors import build_error_record
from procurement.common.resources import ResourceIdentity
from procurement.ingestion.engine.models import BronzeItem
from procurement.ingestion.engine.records import build_bronze_item
from procurement.ingestion.engine.stats import PageStats
from procurement.models.errors import ErrorRecord
from procurement.quality.contracts import (
    ENDPOINTS,
    TABLES,
    WORKFLOW_FIELDS,
    DetailValidationError,
    load_config,
    resolve_route,
    validate_detail,
)

STANDARD_TABLE = TABLES["standard"]
REOFFER_TABLE = TABLES["reoffer"]
VK_ADB_TABLE = TABLES["vk_adb"]
ROUTING_STAGE = "detail_routing"
STANDARD_DETAIL_STAGE = "standard_detail"
REOFFER_DETAIL_STAGE = "reoffer_detail"
DETAIL_EXCEPTIONS = (httpx.HTTPError, KeyError, TypeError, ValueError)


class NotifyContractorDetailApi(Protocol):
    def get_standard_detail(self, notice_id: str) -> dict[str, Any]: ...
    def get_reoffer_detail(self, notice_id: str) -> dict[str, Any]: ...
    def get_vk_adb_detail(self, notice_id: str) -> dict[str, Any]: ...


def iter_notify_contractor_records(
    client: NotifyContractorDetailApi,
    *,
    identity: ResourceIdentity,
    search_items: list[dict[str, Any]],
    run_id: str,
    source_date: date,
    search_page: int,
    errors: list[ErrorRecord],
    stats: PageStats,
    config=None,
) -> Iterator[BronzeItem]:
    config = config or load_config()
    for item in search_items:
        stage = ROUTING_STAGE
        kind = "routing"
        observation = {
            "context": {key: item.get(key) for key in (
                "id", "notifyNo", "notifyVersion", "publicDate", *WORKFLOW_FIELDS,
            )},
            "config_hash": config.fingerprint, "rule_version": config.version,
        }
        try:
            if not item.get("id"):
                raise KeyError("id")
            route = resolve_route(item, config)
            kind = route.contract
            stage = f"{kind}_detail"
            observation.update(contract=kind, endpoint=ENDPOINTS[kind], rule=route.name,
                               evidence=route.evidence)
            payload = getattr(client, f"get_{kind}_detail")(str(item["id"]))
            stage = "detail_validation"
            result = validate_detail(payload, item, config.contracts[kind],
                                     thresholds=config.thresholds)
            observation["result"] = result
            if result["status"] in {"fail", "unresolved"}:
                raise DetailValidationError(
                    ",".join(finding["code"] for finding in result["issues"])
                )
            detail_identity = result["identity"]
            record = build_bronze_item(
                table=TABLES[kind], source_id=detail_identity["notifyNo"],
                source_version=detail_identity["notifyVersion"], payload=payload,
                run_id=run_id, source_date=source_date,
            )
        except DETAIL_EXCEPTIONS as exc:
            stats.error(kind)
            errors.append(build_error_record(
                identity=identity, run_id=run_id, stage=stage, source_date=source_date,
                page_number=search_page, exc=exc, source_id=item.get("notifyNo") or item.get("id"),
            ))
            observation.setdefault("result", {"status": "unresolved", "error": type(exc).__name__})
            stats.quality_observations.append(observation)
            continue
        stats.quality_observations.append(observation)
        stats.record(kind)
        yield record
