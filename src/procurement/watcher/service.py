"""Watcher service orchestrating late bid opening checks and seeds."""
import json
from collections.abc import Callable
from contextlib import nullcontext
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

from procurement.common.catalog import get_resource
from procurement.common.dates import api_day_window, today_vn
from procurement.common.errors import sanitize_error_message
from procurement.common.file_lock import exclusive_file_lock
from procurement.common.settings import settings
from procurement.ingestion.coverage import read_coverage
from procurement.ingestion.engine.metadata import calculate_content_hash
from procurement.ingestion.engine.pagination import iter_search_pages
from procurement.ingestion.sources.muasamcong.bid_opening.resource import BidOpeningApi
from procurement.ingestion.sources.muasamcong.client import MuasamcongClient
from procurement.ingestion.sources.muasamcong.concurrency import RequestBudget
from procurement.quality.contracts import load_config, lookup, validate_detail
from procurement.quality.storage import read_quality_contexts
from procurement.storage.committed import CommittedDay, iter_committed_records
from procurement.storage.control import list_run_manifests, read_day_manifest
from procurement.watcher.policy import NOTICE_ROOTS, calculate_due_date
from procurement.watcher.store import WatchStore, namespace

# Export NOTICE_ROOTS and namespace for backwards compatibility
__all__ = [
    "NOTICE_ROOTS",
    "WatchStore",
    "WatcherService",
    "calculate_due_date",
    "check",
    "due_days",
    "iter_committed_records",
    "list_run_manifests",
    "namespace",
    "notice_context",
    "opening_states",
    "read_coverage",
    "read_day_manifest",
    "read_quality_contexts",
    "read_status",
    "run_watch",
    "seed",
    "selection_for",
]


def selection_for(fs: Any, resource: str, manifest: Any) -> CommittedDay:
    """Build CommittedDay partition selection for given resource and manifest."""
    if hasattr(manifest, "files") and manifest.files:
        files = tuple((getattr(f, "table_name", f[0]), getattr(f, "object_key", f[1])) for f in manifest.files)
        run_id = getattr(manifest, "run_id", str(getattr(manifest, "id", "")))
        records = getattr(manifest, "bronze_records", getattr(manifest, "record_count", 0))
        return CommittedDay(manifest.source_date, run_id, records, files)
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


def seed(
    fs: Any,
    store: WatchStore,
    *,
    start: date | None = None,
    end: date | None = None,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Seed uncompleted bid notices from committed TBMT into watch store."""
    now = now or datetime.now(UTC)
    identity = get_resource("notify_contractor").identity
    runs = list_run_manifests(fs, identity, workers=settings.OPS_SYNC_WORKERS)
    if not runs and (start is None or end is None):
        return {"seeded_days": 0, "coverage": "no_tbmt_manifests"}
    start = start or min(r.start_date for r in runs)
    end = min(end or max(r.end_date for r in runs), today_vn() - timedelta(days=1))
    if start > end:
        raise ValueError("No closed dates in watch range")
    notices = read_coverage(fs, identity, start, end, runs=runs, workers=settings.OPS_SYNC_WORKERS)
    openings = {
        c.source_date: c.effective
        for c in read_coverage(
            fs, get_resource("bid_opening").identity, start, end, workers=settings.OPS_SYNC_WORKERS
        )
    }
    seeded = 0
    for day in notices:
        key = str(day.source_date)
        if day.effective is None:
            with store.db:
                store.db.execute("INSERT OR REPLACE INTO gaps VALUES (?, ?)", (key, day.status))
            continue
        opening = openings[day.source_date]
        opening_run = opening.run_id if opening else None
        old = store.db.execute("SELECT run_id, opening_run FROM seeds WHERE day=?", (key,)).fetchone()
        if old and tuple(old) == (day.effective.run_id, opening_run):
            continue
        captured = {key for key, status in opening_states(fs, opening).items() if status == "complete"}
        evidence = read_quality_contexts(fs, identity, day.effective.run_id, day.source_date)
        staged = []
        for table, record in iter_committed_records(
            fs, (selection_for(fs, "notify_contractor", day.effective),), verify_hash=True,
        ):
            record["source_date"] = date.fromisoformat(str(record["source_date"])[:10])
            context = {"notifyNo": record["source_id"], "notifyVersion": record.get("source_version")}
            status, error, due = "pending", None, now
            try:
                context = notice_context(table, record, evidence)
                if context["isInternet"] == "0":
                    continue
                identity_key = (context["id"], context["notifyNo"], context["notifyVersion"])
                if identity_key in captured:
                    status = "captured"
                due = calculate_due_date(context, now)
            except (KeyError, ValueError, TypeError) as exc:
                status, error = "unresolved", str(exc)
            row_key = calculate_content_hash(
                [key, context.get("id"), record["source_id"], record.get("source_version")]
            )
            staged.append(
                (
                    row_key,
                    key,
                    json.dumps(context),
                    status,
                    due.isoformat() if status == "pending" else None,
                    error,
                )
            )
        # Publish this seed only after the iterator's final count/hash checks succeeded.
        with store.db:
            staged_keys = {r[0] for r in staged}
            for old_row in store.db.execute("SELECT key FROM notices WHERE day=?", (key,)).fetchall():
                if old_row[0] not in staged_keys:
                    store.db.execute(
                        "UPDATE notices SET status='unresolved',next_check=NULL,error='absent_from_current_tbmt' WHERE key=?",
                        (old_row[0],),
                    )
            for row in staged:
                store.db.execute(
                    """INSERT INTO notices(key,day,context,status,next_check,error)
                    VALUES (?,?,?,?,?,?) ON CONFLICT(key) DO UPDATE SET
                    context=excluded.context,status=excluded.status,
                    next_check=CASE WHEN excluded.status='pending' AND notices.error IS NULL
                                   AND notices.context=excluded.context
                                   THEN coalesce(notices.next_check,excluded.next_check)
                                   ELSE excluded.next_check END,error=excluded.error""",
                    row,
                )
            store.db.execute("INSERT OR REPLACE INTO seeds VALUES (?,?,?)", (key, day.effective.run_id, opening_run))
            store.db.execute("DELETE FROM gaps WHERE day=?", (key,))
        seeded += 1
    return {"seeded_days": seeded, **store.status(now, start=start, end=end)}


def check(
    fs: Any,
    store: WatchStore,
    client: Any,
    *,
    start: date | None = None,
    end: date | None = None,
    max_days: int = 31,
    dry_run: bool = False,
    now: datetime | None = None,
    run_day: Callable[..., Any] | None = None,
    page_size: int = 50,
    request_budget: Any = None,
    detail_workers: int | None = None,
) -> dict[str, Any]:
    """Check due closed dates for late bid opening publications and refresh if found."""
    if run_day is None:
        def _default_run_day(
            resource: str,
            source_date: date,
            *,
            page_size: int = 50,
            **kwargs: Any,
        ) -> Any:
            from procurement.bootstrap import get_ingestion_service
            from procurement.ingestion.contracts import MaterializeDayRequest

            service = get_ingestion_service()
            result = service.materialize_day(
                MaterializeDayRequest(
                    source_date=source_date,
                    resource=resource,
                    refresh=True,
                    page_size=page_size,
                )
            )
            if result.status != "success":
                raise ValueError(f"Bid opening refresh did not commit SUCCESS: status={result.status}")
            return str(result.commit_id or result.attempt_id or "success")

        run_day = _default_run_day
    now = now or datetime.now(UTC)
    rows = store.due_days(now=now, start=start, end=end, limit=max_days)
    if dry_run:
        return {"planned_days": [r["day"] for r in rows], **store.status(now, start=start, end=end)}
    results = []
    identity = get_resource("bid_opening").identity
    coverage_by_day = (
        {
            item.source_date: item
            for item in read_coverage(
                fs,
                identity,
                min(date.fromisoformat(r["day"]) for r in rows),
                max(date.fromisoformat(r["day"]) for r in rows),
                workers=settings.OPS_SYNC_WORKERS,
            )
        }
        if rows
        else {}
    )
    for row in rows:
        day = date.fromisoformat(row["day"])
        try:
            coverage = coverage_by_day[day]
            if coverage.active_run_ids:
                raise ValueError("Another bid opening attempt is active")
            states = opening_states(fs, coverage.effective)
            window_from, window_to = api_day_window(day)
            pages = list(
                iter_search_pages(
                    BidOpeningApi(client).search,
                    window_from=window_from,
                    window_to=window_to,
                    page_size=page_size,
                    search_key=lambda item: item.get("id"),
                )
            )
            found = {
                (i.get("notifyId") or i.get("id"), i.get("notifyNo"), i.get("notifyVersion"))
                for _, response in pages
                for i in response["page"]["content"]
            }
            refresh = bool(found - states.keys())
            # Same notice/version can acquire its financial opening later. Search identity
            # alone cannot detect that transition; probe round metadata for pending parts.
            due_contexts = [
                json.loads(item[0])
                for item in store.db.execute(
                    "SELECT context FROM notices WHERE day=? AND status='pending' AND next_check<=?",
                    (str(day), now.isoformat()),
                )
            ]
            available_financial = set()
            for context in due_contexts:
                key = (context.get("id"), context.get("notifyNo"), context.get("notifyVersion"))
                if (
                    key in found
                    and states.get(key) == "awaiting_financial"
                    and BidOpeningApi(client).financial_available(context)
                ):
                    available_financial.add(key)
                    refresh = True
            if refresh:
                run = run_day(
                    "bid_opening",
                    day,
                    page_size=page_size,
                    request_budget=request_budget,
                    search_pages=pages,
                    bid_opening_detail_workers=detail_workers,
                )
                try:
                    manifest = read_day_manifest(fs, identity, run, day)
                except Exception:  # noqa: BLE001 -- fallback to metadata service when S3 manifest is absent
                    manifest = None
                if manifest is None:
                    try:
                        from types import SimpleNamespace

                        from procurement.bootstrap import get_metadata_service
                        from procurement.metadata.models import PartitionIdentity

                        meta_svc = get_metadata_service()
                        snapshot = meta_svc.get_snapshot(
                            [PartitionIdentity(source=identity.source, resource=identity.resource, source_date=day)]
                        )
                        part_commit = snapshot.partitions.get((identity.source, identity.resource, day))
                        if part_commit and part_commit.commit:
                            c = part_commit.commit
                            manifest = SimpleNamespace(
                                run_id=str(c.id),
                                source_date=day,
                                bronze_records=c.record_count,
                                status=SimpleNamespace(value="success"),
                                files=c.files,
                            )
                    except Exception:  # noqa: BLE001, S110 -- best-effort metadata service lookup
                        pass
                if manifest is None or (hasattr(manifest, "status") and manifest.status.value != "success"):
                    raise ValueError("Bid opening refresh did not commit SUCCESS")
                states = opening_states(fs, manifest)
                if not found <= states.keys():
                    raise ValueError("Committed refresh is missing searched identities")
                if any(states[key] != "complete" for key in available_financial):
                    raise ValueError("Committed refresh is missing published financial openings")
            captured = {key for key, status in states.items() if status == "complete"}
            with store.db:
                for item in store.db.execute(
                    "SELECT * FROM notices WHERE day=? AND status='pending'", (str(day),)
                ).fetchall():
                    context = json.loads(item["context"])
                    key = (context.get("id"), context.get("notifyNo"), context.get("notifyVersion"))
                    if key not in captured and item["next_check"] > now.isoformat():
                        continue
                    first = item["first_checked"] or now.isoformat()
                    interval = 7 if now - datetime.fromisoformat(first) >= timedelta(days=30) else 1
                    status = "captured" if key in captured else "pending"
                    store.db.execute(
                        "UPDATE notices SET status=?,first_checked=?,next_check=?,error=NULL WHERE key=?",
                        (
                            status,
                            first,
                            (now + timedelta(days=interval)).isoformat() if status == "pending" else None,
                            item["key"],
                        ),
                    )
            results.append({"day": str(day), "status": "checked"})
        except Exception as exc:
            error = sanitize_error_message(str(exc))
            with store.db:
                store.db.execute(
                    "UPDATE notices SET error=?,next_check=? WHERE day=? AND status='pending' AND next_check<=?",
                    (error, (now + timedelta(days=1)).isoformat(), str(day), now.isoformat()),
                )
            results.append({"day": str(day), "status": "error", "error": error})
            # Auth/storage/uncertain commits must stop this scheduler too.
            raise
    return {"days": results, **store.status(now, start=start, end=end)}


def read_status(path: Path | str | None = None) -> dict[str, Any]:
    """Read current status summary without acquiring write locks."""
    path = Path(path or settings.BID_OPENING_WATCH_PATH)
    store = WatchStore(path, read_only=True)
    try:
        return store.status()
    finally:
        store.close()


def due_days(path: Path | str | None = None, limit: int | None = None) -> list[dict[str, Any]]:
    """Read due closed dates from watch store."""
    path = Path(path or settings.BID_OPENING_WATCH_PATH)
    store = WatchStore(path, read_only=True)
    try:
        return store.due_days(limit=limit or settings.BID_OPENING_WATCH_MAX_DAYS)
    finally:
        store.close()


def run_watch(
    fs: Any,
    *,
    mode: str = "check",
    path: Path | str | None = None,
    start: date | None = None,
    end: date | None = None,
    max_days: int | None = None,
    dry_run: bool = False,
    request_budget: Any = None,
    detail_workers: int | None = None,
    seed_before_check: bool = True,
) -> dict[str, Any]:
    """Execute watch operation with file lock and client lifecycle."""
    workers = settings.BID_OPENING_DETAIL_WORKERS if detail_workers is None else detail_workers
    if not 1 <= workers <= 32:
        raise ValueError("bid opening detail_workers must be between 1 and 32")
    path = Path(path or settings.BID_OPENING_WATCH_PATH)
    readonly = dry_run or mode == "status"
    lock = nullcontext() if readonly else exclusive_file_lock(path.with_suffix(".lock"))
    with lock:
        store = WatchStore(path, dry_run=readonly, read_only=mode == "status")
        try:
            if mode == "status":
                return store.status(start=start, end=end)
            report: dict[str, Any] = {}
            if mode == "seed" or seed_before_check:
                report["seed"] = seed(fs, store, start=start, end=end)
            if mode == "check":
                budget = request_budget or RequestBudget(
                    settings.BID_OPENING_MAX_INFLIGHT,
                    min_interval=settings.BID_OPENING_REQUEST_INTERVAL_SECONDS,
                )
                if dry_run:
                    report["check"] = check(
                        fs,
                        store,
                        None,
                        start=start,
                        end=end,
                        max_days=max_days or settings.BID_OPENING_WATCH_MAX_DAYS,
                        dry_run=True,
                    )
                else:
                    with MuasamcongClient(
                        token=settings.MUASAMCONG_TOKEN or "",
                        max_attempts=settings.MUASAMCONG_MAX_ATTEMPTS,
                        max_retry_delay=settings.MUASAMCONG_MAX_RETRY_DELAY_SECONDS,
                        request_budget=budget,
                    ) as client:
                        report["check"] = check(
                            fs,
                            store,
                            client,
                            start=start,
                            end=end,
                            max_days=max_days or settings.BID_OPENING_WATCH_MAX_DAYS,
                            request_budget=budget,
                            detail_workers=workers,
                        )
            return report
        finally:
            store.close()


class WatcherService:
    """Service wrapping watch operations and persistence."""

    def __init__(self, fs: Any = None, path: Path | str | None = None) -> None:
        self.fs = fs
        self.path = Path(path or settings.BID_OPENING_WATCH_PATH)

    def due_days(self, limit: int | None = None) -> list[dict[str, Any]]:
        return due_days(self.path, limit=limit)

    def read_status(self) -> dict[str, Any]:
        return read_status(self.path)

    def seed(
        self,
        *,
        start: date | None = None,
        end: date | None = None,
        now: datetime | None = None,
    ) -> dict[str, Any]:
        with exclusive_file_lock(self.path.with_suffix(".lock")):
            store = WatchStore(self.path)
            try:
                return seed(self.fs, store, start=start, end=end, now=now)
            finally:
                store.close()

    def check(
        self,
        client: Any,
        *,
        start: date | None = None,
        end: date | None = None,
        max_days: int = 31,
        dry_run: bool = False,
        now: datetime | None = None,
        run_day: Callable[..., Any] | None = None,
        page_size: int = 50,
        request_budget: Any = None,
        detail_workers: int | None = None,
    ) -> dict[str, Any]:
        readonly = dry_run
        lock = nullcontext() if readonly else exclusive_file_lock(self.path.with_suffix(".lock"))
        with lock:
            store = WatchStore(self.path, dry_run=readonly)
            try:
                return check(
                    self.fs,
                    store,
                    client,
                    start=start,
                    end=end,
                    max_days=max_days,
                    dry_run=dry_run,
                    now=now,
                    run_day=run_day,
                    page_size=page_size,
                    request_budget=request_budget,
                    detail_workers=detail_workers,
                )
            finally:
                store.close()

    def run(
        self,
        *,
        mode: str = "check",
        start: date | None = None,
        end: date | None = None,
        max_days: int | None = None,
        dry_run: bool = False,
        request_budget: Any = None,
        detail_workers: int | None = None,
        seed_before_check: bool = True,
    ) -> dict[str, Any]:
        return run_watch(
            self.fs,
            mode=mode,
            path=self.path,
            start=start,
            end=end,
            max_days=max_days,
            dry_run=dry_run,
            request_budget=request_budget,
            detail_workers=detail_workers,
            seed_before_check=seed_before_check,
        )
