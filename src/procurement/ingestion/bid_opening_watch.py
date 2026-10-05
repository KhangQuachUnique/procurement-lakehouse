"""Rebuildable, namespace-bound scheduler for late bid openings; Bronze remains authoritative."""
import json
import sqlite3
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

from procurement.common.catalog import get_resource
from procurement.common.dates import VIETNAM_TZ, api_day_window, today_vn
from procurement.common.errors import sanitize_error_message
from procurement.common.settings import settings
from procurement.ingestion.coverage import read_coverage
from procurement.ingestion.engine.metadata import calculate_content_hash
from procurement.ingestion.engine.pagination import iter_search_pages
from procurement.ingestion.sources.muasamcong.bid_opening.resource import BidOpeningApi
from procurement.quality.contracts import load_config, lookup, validate_detail
from procurement.quality.storage import read_quality_contexts
from procurement.storage.committed import CommittedDay, iter_committed_records
from procurement.storage.control import list_run_manifests, read_day_manifest


def namespace():
    return calculate_content_hash([settings.OBJECT_STORAGE_ENDPOINT, settings.OBJECT_STORAGE_BUCKET])


class WatchStore:
    def __init__(self, path, *, dry_run=False, read_only=False):
        path = Path(path)
        if read_only and path.exists():
            self.db = sqlite3.connect(path.resolve().as_uri() + "?mode=ro", uri=True)
            self.db.row_factory = sqlite3.Row
            existing = self.db.execute("SELECT value FROM metadata WHERE name='namespace'").fetchone()
            if not existing or existing[0] != namespace():
                self.db.close()
                raise ValueError("Watch storage namespace changed; use a different watch database")
            return
        if dry_run or read_only:
            self.db = sqlite3.connect(":memory:")
            if path.exists():
                with sqlite3.connect(path.resolve().as_uri() + "?mode=ro", uri=True) as source:
                    source.backup(self.db)
        else:
            path.parent.mkdir(parents=True, exist_ok=True)
            self.db = sqlite3.connect(path)
        self.db.row_factory = sqlite3.Row
        self.db.executescript("""
            CREATE TABLE IF NOT EXISTS metadata (name TEXT PRIMARY KEY, value TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS seeds (
                day TEXT PRIMARY KEY, run_id TEXT NOT NULL, opening_run TEXT);
            CREATE TABLE IF NOT EXISTS gaps (day TEXT PRIMARY KEY, reason TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS notices (
                key TEXT PRIMARY KEY, day TEXT NOT NULL, context TEXT NOT NULL,
                status TEXT NOT NULL, next_check TEXT, first_checked TEXT, error TEXT);
            CREATE INDEX IF NOT EXISTS due_notices ON notices(status, next_check, day);
        """)
        existing = self.db.execute("SELECT value FROM metadata WHERE name='namespace'").fetchone()
        if existing and existing[0] != namespace():
            self.db.close()
            raise ValueError("Watch storage namespace changed; use a different watch database")
        with self.db:
            self.db.execute("INSERT OR IGNORE INTO metadata VALUES ('namespace', ?)", (namespace(),))
            version = self.db.execute("SELECT value FROM metadata WHERE name='phase_rules'").fetchone()
            if not version or version[0] != "2":
                # Re-evaluate captured states under the new phase contract on the next seed.
                self.db.execute("DELETE FROM seeds")
                self.db.execute("INSERT OR REPLACE INTO metadata VALUES ('phase_rules', '2')")

    def close(self):
        self.db.close()

    def status(self, now=None, *, start=None, end=None):
        now = now or datetime.now(UTC)
        bounds = (str(start or date.min), str(end or date.max))
        result = {key: 0 for key in ("pending", "captured", "unresolved")}
        result.update(dict(self.db.execute("SELECT status, count(*) FROM notices WHERE day BETWEEN ? AND ? GROUP BY status", bounds)))
        result["errors"] = self.db.execute("SELECT count(*) FROM notices WHERE error IS NOT NULL AND day BETWEEN ? AND ?", bounds).fetchone()[0]
        result["coverage_gaps"] = self.db.execute("SELECT count(*) FROM gaps WHERE day BETWEEN ? AND ?", bounds).fetchone()[0]
        result["due_days"] = self.db.execute(
            "SELECT count(DISTINCT day) FROM notices WHERE status='pending' AND next_check<=? AND day BETWEEN ? AND ?",
            (now.isoformat(), *bounds),
        ).fetchone()[0]
        return result


def selection_for(fs, resource, manifest):
    definition = get_resource(resource)
    files = []
    for table in definition.tables:
        prefix = (f"{settings.OBJECT_STORAGE_BUCKET}/bronze/{definition.identity.source}/{table}/"
                  f"source_date={manifest.source_date}/run_id={manifest.run_id}")
        files.extend((table, key) for key in sorted(fs.glob(f"{prefix}/*.parquet")))
    return CommittedDay(manifest.source_date, manifest.run_id, manifest.bronze_records, tuple(files))


def opening_states(fs, manifest):
    if manifest is None:
        return {}
    config = load_config(resource="bid_opening")
    states = {}
    for _, record in iter_committed_records(
        fs, (selection_for(fs, "bid_opening", manifest),), verify_hash=True,
    ):
        payload = record["payload"]
        if isinstance(payload, str):
            payload = json.loads(payload)
        root = lookup(payload, "notify.bidNoContractorResponse.bidNotification") or {}
        context = {"id": root.get("id"), "notifyNo": record["source_id"],
                   "notifyVersion": record.get("source_version")}
        result = validate_detail(payload, context, config.contracts["bid_opening"])
        if result["status"] not in {"pass", "warn"}:
            raise ValueError("Committed bid opening failed assembly validation")
        key = (context["id"], context["notifyNo"], context["notifyVersion"])
        if key in states:
            raise ValueError("Duplicate committed bid opening identity")
        states[key] = result["collection_status"]
    return states


NOTICE_ROOTS = {
    "notify_contractor_standard_detail": ("bidoNotifyContractorM", "bidNoContractorResponse.bidNotification"),
    "notify_contractor_vk_adb_detail": ("bidoNotifyContractorP",),
    "notify_contractor_reoffer_detail": ("",),
}


def notice_context(table, record, evidence):
    payload = record["payload"]
    if isinstance(payload, str):
        payload = json.loads(payload)
    roots = [lookup(payload, p) for p in NOTICE_ROOTS[table]]
    roots = [r for r in roots if isinstance(r, dict) and r]
    matches = [c for c in evidence if c.get("notifyNo") == record["source_id"]
               and c.get("notifyVersion") == record.get("source_version")]
    candidates = roots + matches
    context = {}
    for field in ("id", "notifyNo", "notifyVersion", "publicDate", "isInternet", "bidOpenDate", "bidCloseDate"):
        aliases = (field,)
        if table == "notify_contractor_reoffer_detail":
            alias = {"notifyNo": "reofferNo", "notifyVersion": "reofferVersion",
                     "bidOpenDate": "reofferOpenDate", "bidCloseDate": "reofferCloseDate"}.get(field)
            if alias:
                aliases += (alias,)
        values = {str(c[name]) for c in candidates for name in aliases if c.get(name) is not None}
        if field == "isInternet":
            values = {"1" if v in {"True", "1"} else "0" if v in {"False", "0"} else v for v in values}
        if len(values) > 1:
            raise ValueError(f"Conflicting notice {field}")
        context[field] = next(iter(values), None)
    if (not context["id"] or not context["notifyNo"] or context["notifyVersion"] is None
            or not context["publicDate"] or context["isInternet"] not in {"0", "1"}):
        raise ValueError("Missing notice identity/date/type")
    if context["notifyNo"] != record["source_id"] or context["notifyVersion"] != record.get("source_version"):
        raise ValueError("Notice identity differs from Bronze envelope")
    if date.fromisoformat(context["publicDate"][:10]) != record["source_date"]:
        raise ValueError("Notice publicDate differs from source_date")
    return context


def seed(fs, store, *, start=None, end=None, now=None):
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
    openings = {c.source_date: c.effective for c in read_coverage(fs, get_resource("bid_opening").identity, start, end, workers=settings.OPS_SYNC_WORKERS)}
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
                scheduled = context.get("bidOpenDate") or context.get("bidCloseDate")
                if scheduled:
                    parsed = datetime.fromisoformat(scheduled)
                    if parsed.tzinfo is None:
                        parsed = parsed.replace(tzinfo=VIETNAM_TZ)
                    due = max(now, parsed.astimezone(UTC) + timedelta(hours=1))
            except (KeyError, ValueError, TypeError) as exc:
                status, error = "unresolved", str(exc)
            row_key = calculate_content_hash([key, context.get("id"), record["source_id"], record.get("source_version")])
            staged.append((row_key, key, json.dumps(context), status,
                           due.isoformat() if status == "pending" else None, error))
        # Publish this seed only after the iterator's final count/hash checks succeeded.
        with store.db:
            staged_keys = {r[0] for r in staged}
            for old_row in store.db.execute("SELECT key FROM notices WHERE day=?", (key,)).fetchall():
                if old_row[0] not in staged_keys:
                    store.db.execute("UPDATE notices SET status='unresolved',next_check=NULL,error='absent_from_current_tbmt' WHERE key=?", (old_row[0],))
            for row in staged:
                store.db.execute("""INSERT INTO notices(key,day,context,status,next_check,error)
                    VALUES (?,?,?,?,?,?) ON CONFLICT(key) DO UPDATE SET
                    context=excluded.context,status=excluded.status,
                    next_check=CASE WHEN excluded.status='pending' AND notices.error IS NULL
                                   AND notices.context=excluded.context
                                   THEN coalesce(notices.next_check,excluded.next_check)
                                   ELSE excluded.next_check END,error=excluded.error""", row)
            store.db.execute("INSERT OR REPLACE INTO seeds VALUES (?,?,?)", (key, day.effective.run_id, opening_run))
            store.db.execute("DELETE FROM gaps WHERE day=?", (key,))
        seeded += 1
    return {"seeded_days": seeded, **store.status(now, start=start, end=end)}


def check(fs, store, client, *, start=None, end=None, max_days=31, dry_run=False, now=None,
          run_day=None, page_size=50, request_budget=None, detail_workers=None):
    from procurement.jobs.runner import run_resource_day
    run_day = run_day or run_resource_day
    now = now or datetime.now(UTC)
    rows = store.db.execute("""SELECT day,min(next_check) AS due FROM notices
        WHERE status='pending' AND next_check<=? AND day>=? AND day<=?
        GROUP BY day ORDER BY due,day LIMIT ?""",
        (now.isoformat(), str(start or date.min), str(end or today_vn() - timedelta(days=1)), max_days)).fetchall()
    if dry_run:
        return {"planned_days": [r["day"] for r in rows], **store.status(now, start=start, end=end)}
    results = []
    identity = get_resource("bid_opening").identity
    coverage_by_day = {item.source_date: item for item in read_coverage(
        fs, identity, min(date.fromisoformat(r["day"]) for r in rows),
        max(date.fromisoformat(r["day"]) for r in rows), workers=settings.OPS_SYNC_WORKERS)} if rows else {}
    for row in rows:
        day = date.fromisoformat(row["day"])
        try:
            coverage = coverage_by_day[day]
            if coverage.active_run_ids:
                raise ValueError("Another bid opening attempt is active")
            states = opening_states(fs, coverage.effective)
            window_from, window_to = api_day_window(day)
            pages = list(iter_search_pages(BidOpeningApi(client).search,
                window_from=window_from, window_to=window_to, page_size=page_size,
                search_key=lambda item: item.get("id")))
            found = {(i.get("notifyId") or i.get("id"), i.get("notifyNo"), i.get("notifyVersion"))
                     for _, response in pages for i in response["page"]["content"]}
            refresh = bool(found - states.keys())
            # Same notice/version can acquire its financial opening later. Search identity
            # alone cannot detect that transition; probe round metadata for pending parts.
            due_contexts = [json.loads(item[0]) for item in store.db.execute(
                "SELECT context FROM notices WHERE day=? AND status='pending' AND next_check<=?",
                (str(day), now.isoformat()))]
            available_financial = set()
            for context in due_contexts:
                key = (context.get("id"), context.get("notifyNo"), context.get("notifyVersion"))
                if (key in found and states.get(key) == "awaiting_financial"
                        and BidOpeningApi(client).financial_available(context)):
                    available_financial.add(key)
                    refresh = True
            if refresh:
                run = run_day("bid_opening", day, page_size=page_size,
                              request_budget=request_budget, search_pages=pages,
                              bid_opening_detail_workers=detail_workers)
                manifest = read_day_manifest(fs, identity, run, day)
                if manifest is None or manifest.status.value != "success":
                    raise ValueError("Bid opening refresh did not commit SUCCESS")
                states = opening_states(fs, manifest)
                if not found <= states.keys():
                    raise ValueError("Committed refresh is missing searched identities")
                if any(states[key] != "complete" for key in available_financial):
                    raise ValueError("Committed refresh is missing published financial openings")
            captured = {key for key, status in states.items() if status == "complete"}
            with store.db:
                for item in store.db.execute("SELECT * FROM notices WHERE day=? AND status='pending'", (str(day),)).fetchall():
                    context = json.loads(item["context"])
                    key = (context.get("id"), context.get("notifyNo"), context.get("notifyVersion"))
                    if key not in captured and item["next_check"] > now.isoformat():
                        continue
                    first = item["first_checked"] or now.isoformat()
                    interval = 7 if now - datetime.fromisoformat(first) >= timedelta(days=30) else 1
                    status = "captured" if key in captured else "pending"
                    store.db.execute("UPDATE notices SET status=?,first_checked=?,next_check=?,error=NULL WHERE key=?",
                        (status, first, (now + timedelta(days=interval)).isoformat() if status == "pending" else None, item["key"]))
            results.append({"day": str(day), "status": "checked"})
        except Exception as exc:
            error = sanitize_error_message(str(exc))
            with store.db:
                store.db.execute("UPDATE notices SET error=?,next_check=? WHERE day=? AND status='pending' AND next_check<=?",
                                 (error, (now + timedelta(days=1)).isoformat(), str(day), now.isoformat()))
            results.append({"day": str(day), "status": "error", "error": error})
            # Auth/storage/uncertain commits must stop this scheduler too.
            raise
    return {"days": results, **store.status(now, start=start, end=end)}


def read_status(path=None):
    path = Path(path or settings.BID_OPENING_WATCH_PATH)
    store = WatchStore(path, read_only=True)
    try:
        return store.status()
    finally:
        store.close()
