import json
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest

from procurement.ingestion import bid_opening_watch as watch

DAY = date(2025, 8, 3)
NOW = datetime(2025, 8, 22, tzinfo=UTC)
CTX = {"id": "uuid", "notifyNo": "IB1", "notifyVersion": "00", "isInternet": 1,
       "publicDate": "2025-08-03T12:00:00", "bidOpenDate": "2025-08-21T15:00:00"}


@pytest.fixture
def store(tmp_path):
    result = watch.WatchStore(tmp_path / "watch.sqlite3")
    yield result
    result.close()


def setup_seed(monkeypatch, *, broken=False):
    manifest = SimpleNamespace(run_id="notice-run", source_date=DAY, bronze_records=1)
    coverage = SimpleNamespace(source_date=DAY, effective=manifest, active_run_ids=(), status="success")
    monkeypatch.setattr(watch, "list_run_manifests", lambda *_, **__: [SimpleNamespace(start_date=DAY, end_date=DAY)])
    monkeypatch.setattr(watch, "read_coverage", lambda fs, identity, start, end, **_: [coverage] if identity.resource == "notify_contractor" else [SimpleNamespace(source_date=DAY, effective=None)])
    monkeypatch.setattr(watch, "selection_for", lambda *_: None)
    monkeypatch.setattr(watch, "read_quality_contexts", lambda *_: [])
    def records(*_, **__):
        yield "notify_contractor_standard_detail", {"source_id": "IB1", "source_version": "00", "source_date": DAY,
                                                   "payload": {"bidoNotifyContractorM": CTX}}
        if broken:
            raise ValueError("Bronze count mismatch")
    monkeypatch.setattr(watch, "iter_committed_records", records)
    return coverage


def test_seed_existing_committed_notices_idempotent(monkeypatch, store):
    setup_seed(monkeypatch)
    assert watch.seed(None, store, now=NOW)["seeded_days"] == 1
    assert watch.seed(None, store, now=NOW)["seeded_days"] == 0
    assert store.status(NOW)["pending"] == 1
    assert store.status(NOW)["due_days"] == 1


def test_failed_verification_never_publishes_seed(monkeypatch, store):
    setup_seed(monkeypatch, broken=True)
    with pytest.raises(ValueError, match="count mismatch"):
        watch.seed(None, store, now=NOW)
    assert store.status(NOW)["pending"] == 0
    assert store.db.execute("SELECT count(*) FROM seeds").fetchone()[0] == 0


def test_missing_tbmt_is_coverage_gap(monkeypatch, store):
    coverage = setup_seed(monkeypatch)
    coverage.effective, coverage.status = None, "no_attempt"
    assert watch.seed(None, store, now=NOW)["coverage_gaps"] == 1


def add_pending(store):
    with store.db:
        store.db.execute("INSERT INTO notices VALUES (?,?,?,?,?,?,?)",
                         ("key", str(DAY), json.dumps(CTX), "pending", NOW.isoformat(), None, None))


@pytest.mark.parametrize("success", [True, False])
def test_late_opening_refresh_whole_day_before_capture(monkeypatch, store, success):
    add_pending(store)
    monkeypatch.setattr(watch, "read_coverage", lambda *_, **__: [SimpleNamespace(source_date=DAY, active_run_ids=(), effective=None)])
    monkeypatch.setattr(watch, "opening_states", lambda fs, manifest: {(CTX["id"], "IB1", "00"): "complete"} if manifest else {})
    manifest = SimpleNamespace(status=SimpleNamespace(value="success" if success else "failed"))
    monkeypatch.setattr(watch, "read_day_manifest", lambda *_: manifest)
    calls = []
    class Client:
        def post(self, path, body):
            calls.append("search")
            return {"page": {"totalElements": 1, "content": [CTX]}}
    def run(resource, day, **kwargs):
        assert resource == "bid_opening" and day == DAY
        assert kwargs["search_pages"][0][1]["page"]["content"] == [CTX]
        calls.append("refresh")
        return "new-run"
    if success:
        watch.check(None, store, Client(), now=NOW, run_day=run)
        assert store.status(NOW)["captured"] == 1
    else:
        with pytest.raises(ValueError, match="did not commit"):
            watch.check(None, store, Client(), now=NOW, run_day=run)
        assert store.status(NOW)["pending"] == store.status(NOW)["errors"] == 1
    assert calls == ["search", "refresh"]


def test_empty_search_reschedules_without_losing_pending(monkeypatch, store):
    add_pending(store)
    with store.db:
        store.db.execute("UPDATE notices SET first_checked=?", ((NOW-timedelta(days=31)).isoformat(),))
    monkeypatch.setattr(watch, "read_coverage", lambda *_, **__: [SimpleNamespace(source_date=DAY, active_run_ids=(), effective=None)])
    class Client:
        def post(self, *_):
            return {"page": {"totalElements": 0, "content": []}}
    watch.check(None, store, Client(), now=NOW)
    assert store.db.execute("SELECT next_check FROM notices").fetchone()[0] == (NOW+timedelta(days=7)).isoformat()
    assert store.status(NOW)["pending"] == 1


def test_dry_run_database_is_unchanged(tmp_path):
    path = tmp_path / "state.db"
    initial = watch.WatchStore(path)
    initial.close()
    before = path.read_bytes()
    dry = watch.WatchStore(path, dry_run=True)
    add_pending(dry)
    assert watch.check(None, dry, None, now=NOW, dry_run=True)["planned_days"] == [str(DAY)]
    dry.close()
    assert path.read_bytes() == before


def test_namespace_mismatch_rejected(tmp_path, monkeypatch):
    path = tmp_path / "state.db"
    store = watch.WatchStore(path)
    store.close()
    monkeypatch.setattr(watch, "namespace", lambda: "different")
    with pytest.raises(ValueError, match="namespace"):
        watch.WatchStore(path, dry_run=True)


def test_changed_effective_run_reseeds_changed_opening_schedule(monkeypatch, store):
    coverage = setup_seed(monkeypatch)
    watch.seed(None, store, now=NOW)
    coverage.effective.run_id = "notice-run-repaired"
    monkeypatch.setitem(CTX, "bidOpenDate", "2025-09-01T15:00:00")
    assert watch.seed(None, store, now=NOW)["seeded_days"] == 1
    row = store.db.execute("SELECT * FROM notices").fetchone()
    assert row["next_check"] == "2025-09-01T09:00:00+00:00"
    assert store.status(NOW)["due_days"] == 0
    assert store.status(NOW)["pending"] == 1


def test_check_keeps_future_notice_schedule_when_same_day_is_due(monkeypatch, store):
    add_pending(store)
    future = (NOW + timedelta(days=15)).isoformat()
    with store.db:
        store.db.execute("INSERT INTO notices VALUES (?,?,?,?,?,?,?)",
                         ("future", str(DAY), json.dumps({**CTX, "id": "other", "notifyNo": "IB2"}),
                          "pending", future, None, None))
    monkeypatch.setattr(watch, "read_coverage", lambda *_, **__: [SimpleNamespace(source_date=DAY, active_run_ids=(), effective=None)])
    class Client:
        def post(self, *_):
            return {"page": {"totalElements": 0, "content": []}}
    watch.check(None, store, Client(), now=NOW)
    row = store.db.execute("SELECT * FROM notices WHERE key='future'").fetchone()
    assert row["next_check"] == future
    assert row["first_checked"] is None


def test_seed_existing_opening_is_captured_without_network(monkeypatch, store):
    setup_seed(monkeypatch)
    monkeypatch.setattr(watch, "opening_states", lambda *_: {(CTX["id"], "IB1", "00"): "complete"})
    assert watch.seed(None, store, now=NOW)["captured"] == 1
    assert watch.check(None, store, None, now=NOW)["days"] == []


def test_due_limit_prioritizes_oldest_check_not_oldest_source_date(store):
    with store.db:
        for index in range(40):
            source_date = date(2025, 1, 1) + timedelta(days=index)
            due = NOW - timedelta(days=index)
            store.db.execute("INSERT INTO notices VALUES (?,?,?,?,?,?,?)",
                             (str(index), str(source_date), json.dumps(CTX), "pending", due.isoformat(), None, None))
    planned = watch.check(None, store, None, now=NOW, dry_run=True)["planned_days"]
    assert len(planned) == 31
    assert planned[0] == "2025-02-09"
    assert planned[-1] == "2025-01-10"


def test_reoffer_aliases_preserve_version_and_schedule():
    root = {**CTX}
    for field, alias in (("notifyNo", "reofferNo"), ("notifyVersion", "reofferVersion"),
                         ("bidOpenDate", "reofferOpenDate")):
        root[alias] = root.pop(field)
    record = {"source_id": "IB1", "source_version": "00", "source_date": DAY, "payload": root}
    context = watch.notice_context("notify_contractor_reoffer_detail", record, [])
    assert context["notifyVersion"] == "00"
    assert context["bidOpenDate"] == CTX["bidOpenDate"]
    root["notifyVersion"] = "01"
    with pytest.raises(ValueError, match="Conflicting"):
        watch.notice_context("notify_contractor_reoffer_detail", record, [])


@pytest.mark.parametrize("published", [False, True])
def test_same_identity_financial_arrival_triggers_refresh(monkeypatch, store, published):
    add_pending(store)
    key = (CTX["id"], "IB1", "00")
    old = SimpleNamespace(source_date=DAY, active_run_ids=(), effective="old")
    monkeypatch.setattr(watch, "read_coverage", lambda *_, **__: [old])
    monkeypatch.setattr(watch, "opening_states", lambda fs, manifest: {key: "awaiting_financial" if manifest == "old" else "complete"})
    monkeypatch.setattr(watch, "read_day_manifest", lambda *_: SimpleNamespace(status=SimpleNamespace(value="success")))
    calls = []
    class Client:
        def post(self, path, body):
            if path.endswith("smart/search"):
                calls.append("search")
                return {"page": {"totalElements": 1, "content": [CTX]}}
            assert path.endswith("/roundmng")
            calls.append("round")
            return {"bidoBidroundMngViewDTO": {**CTX, "bidMode": "1_HTHS",
                "successBidOpenDateTc": "2025-08-22T10:00:00" if published else None}}
    def run(*_, **__):
        calls.append("refresh")
        return "new"
    watch.check(None, store, Client(), now=NOW, run_day=run)
    assert calls == (["search", "round", "refresh"] if published else ["search", "round"])
    assert store.status(NOW)["captured"] == int(published)
    assert store.status(NOW)["pending"] == int(not published)


def test_seed_keeps_technical_only_opening_pending(monkeypatch, store):
    setup_seed(monkeypatch)
    monkeypatch.setattr(watch, "opening_states", lambda *_: {(CTX["id"], "IB1", "00"): "awaiting_financial"})
    status = watch.seed(None, store, now=NOW)
    assert status["pending"] == 1 and status["captured"] == 0


@pytest.mark.parametrize("published", [True, False])
def test_committed_phase_state_is_derived_from_verified_payload(monkeypatch, published):
    payload = json.loads((Path(__file__).parent / "muasamcong/fixtures/bid_opening_dual.json").read_text(encoding="utf-8"))
    if not published:
        payload["roundmng"]["bidoBidroundMngViewDTO"]["successBidOpenDateTc"] = None
        del payload["bid_open_financial"], payload["lot_open_detail_financial"]
    root = payload["notify"]["bidNoContractorResponse"]["bidNotification"]
    monkeypatch.setattr(watch, "selection_for", lambda *_: None)
    monkeypatch.setattr(watch, "iter_committed_records", lambda *_, **__: iter([
        ("bid_opening_detail", {"source_id": root["notifyNo"], "source_version": root["notifyVersion"], "payload": payload})]))
    assert list(watch.opening_states(None, object()).values()) == ["complete" if published else "awaiting_financial"]


def test_existing_watch_state_is_reseeded_after_phase_upgrade(tmp_path):
    path = tmp_path / "watch.sqlite3"
    old = watch.WatchStore(path)
    with old.db:
        old.db.execute("DELETE FROM metadata WHERE name='phase_rules'")
        old.db.execute("INSERT INTO seeds VALUES (?,?,?)", (str(DAY), "tbmt", "opening"))
    old.close()
    upgraded = watch.WatchStore(path)
    assert upgraded.db.execute("SELECT count(*) FROM seeds").fetchone()[0] == 0
    upgraded.close()
