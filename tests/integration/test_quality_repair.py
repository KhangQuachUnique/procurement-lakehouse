"""Real Parquet and manifest tests for a selective two-record day repair."""

from datetime import UTC, date, datetime

import pytest

from procurement.common.catalog import get_resource
from procurement.common.settings import settings
from procurement.ingestion.engine.daily_runner import _create_pipeline
from procurement.ingestion.engine.metadata import calculate_content_hash
from procurement.ingestion.engine.models import ResourceSpec
from procurement.models.bronze import BronzeRecord
from procurement.models.control import DayManifest, DayStatus, RunManifest, RunStatus
from procurement.quality.audit import audit_day, frozen_selection
from procurement.quality.contracts import ENDPOINTS, TABLES, Route, load_config
from procurement.quality.files import read_json, write_json
from procurement.quality.repair import apply_plan, create_plan
from procurement.storage.bronze import DltBronzeWriter
from procurement.storage.committed import iter_committed_records, select_committed_days
from procurement.storage.control import commit_day_manifest, write_run_manifest

pytestmark = pytest.mark.integration
DAY = date(2025, 1, 2)


def setup_case(fs, tmp_path, monkeypatch):
    monkeypatch.setattr(settings, "INGESTION_LOCK_DIR", str(tmp_path / "locks"))
    definition = get_resource("notify_contractor")
    identity = definition.identity
    config = load_config()
    contexts = [{"id": key, "notifyNo": f"IB-{key}", "notifyVersion": "00"} for key in ("good", "bad")]
    config.routes = [Route(name=ctx["id"], contract=kind, evidence="integration fixture", match=ctx)
                     for ctx, kind in zip(contexts, ("standard", "vk_adb"), strict=True)]
    records = [BronzeRecord(
        source_id=ctx["notifyNo"], source_version="00", source_date=DAY, run_id="baseline",
        ingested_at=datetime(2025, 1, 3, tzinfo=UTC),
        payload=payload, content_hash=calculate_content_hash(payload),
    ) for ctx, payload in zip(contexts, ({"bidoNotifyContractorM": contexts[0]}, {}), strict=True)]
    spec = ResourceSpec(identity, "quality_test", "muasamcong", lambda **_: {}, lambda **_: iter(()))
    writer = DltBronzeWriter(lambda: _create_pipeline(spec, DAY, "baseline"))
    writer.write_page({TABLES["standard"]: records})
    write_run_manifest(fs, identity, RunManifest(
        run_id="baseline", source=identity.source, resource=identity.resource,
        start_date=DAY, end_date=DAY, total_dates=1, success_dates=1,
        status=RunStatus.SUCCESS, started_at=datetime(2025, 1, 3, tzinfo=UTC),
    ))
    commit_day_manifest(fs, identity, DayManifest(
        run_id="baseline", source=identity.source, resource=identity.resource, source_date=DAY,
        status=DayStatus.SUCCESS, bronze_records=2, search_items=2,
        started_at=datetime(2025, 1, 3, tzinfo=UTC),
    ))
    selection = frozen_selection(fs, identity.resource, DAY, DAY)[0]
    day = audit_day(fs, selection, config, contexts)
    assert [row["result"]["status"] for row in day["rows"]] == ["pass", "fail"]
    audit_dir = tmp_path / "audit"
    write_json(audit_dir / "days" / f"{DAY}.json", day)
    write_json(audit_dir / "selection.json", {
        "status": "complete", "resource": identity.resource, "year": 2025,
        "config_hash": config.fingerprint, "selection": [selection],
        "completed": {str(DAY): calculate_content_hash(day)},
        "storage_namespace": calculate_content_hash([
            settings.OBJECT_STORAGE_ENDPOINT, settings.OBJECT_STORAGE_BUCKET,
        ]),
    })
    plan = create_plan(audit_dir, config, tmp_path / "plan.json")
    return definition, contexts, config, plan, records


def test_local_selective_repair_preserves_good_payload_and_moves_bad_table(store, tmp_path, monkeypatch):
    fs, _ = store
    definition, contexts, config, plan, records = setup_case(fs, tmp_path, monkeypatch)
    calls = []
    class Client:
        def post(self, endpoint, body):
            calls.append((endpoint, body))
            return {"bidoNotifyContractorP": contexts[1]}
    work = tmp_path / "repair"
    result = apply_plan(fs, Client(), plan, config, work)
    assert calls == [(ENDPOINTS["vk_adb"], {"id": "bad"})]
    assert result[0]["counts"] == {"copied": 1, "refetched": 1, "moved": 1}
    selected = select_committed_days(fs, definition, DAY, DAY)
    assert selected[0].run_id != "baseline"
    repaired = list(iter_committed_records(fs, selected, verify_hash=True))
    good = next(record for _, record in repaired if record["source_id"] == "IB-good")
    assert good["content_hash"] == records[0].content_hash
    assert good["ingested_at"] == records[0].ingested_at
    assert {table for table, _ in repaired} == {TABLES["standard"], TABLES["vk_adb"]}
    apply_plan(fs, Client(), plan, config, work)
    assert len(calls) == 1
    assert read_json(work / "summary.json")["status"] == "complete"


def test_local_repair_rejects_changed_version_without_committing(store, tmp_path, monkeypatch):
    fs, _ = store
    definition, contexts, config, plan, _ = setup_case(fs, tmp_path, monkeypatch)
    class Client:
        def post(self, *_):
            return {"bidoNotifyContractorP": {**contexts[1], "notifyVersion": "01"}}
    with pytest.raises(ValueError, match="source_changed"):
        apply_plan(fs, Client(), plan, config, tmp_path / "repair")
    assert select_committed_days(fs, definition, DAY, DAY)[0].run_id == "baseline"


def test_local_sidecar_failure_and_cached_refetch_recovery(store, tmp_path, monkeypatch):
    from procurement.quality import repair

    fs, _ = store
    definition, contexts, config, plan, _ = setup_case(fs, tmp_path, monkeypatch)
    calls = []
    class Client:
        def post(self, *_):
            calls.append(1)
            return {"bidoNotifyContractorP": contexts[1]}
    original = repair.save_quality_page
    def fail(*args, **kwargs):
        raise OSError("Sidecar write failed")
    monkeypatch.setattr(repair, "save_quality_page", fail)
    work = tmp_path / "repair"
    with pytest.raises(OSError, match="Sidecar"):
        apply_plan(fs, Client(), plan, config, work)
    assert select_committed_days(fs, definition, DAY, DAY)[0].run_id == "baseline"
    monkeypatch.setattr(repair, "save_quality_page", original)
    apply_plan(fs, Client(), plan, config, work)
    assert len(calls) == 1


def test_local_lost_commit_ack_does_not_downgrade_success(store, tmp_path, monkeypatch):
    from procurement.quality import repair
    from procurement.storage.control import DayCommitUncertainError

    fs, _ = store
    definition, contexts, config, plan, _ = setup_case(fs, tmp_path, monkeypatch)
    class Client:
        def post(self, *_):
            return {"bidoNotifyContractorP": contexts[1]}
    original = repair.commit_day_manifest
    def lost_ack(*args):
        original(*args)
        raise DayCommitUncertainError("Lost acknowledgement")
    monkeypatch.setattr(repair, "commit_day_manifest", lost_ack)
    work = tmp_path / "repair"
    with pytest.raises(DayCommitUncertainError):
        apply_plan(fs, Client(), plan, config, work)
    assert select_committed_days(fs, definition, DAY, DAY)[0].run_id != "baseline"
    assert read_json(work / f"{DAY}.json")["status"] == "commit_uncertain"
    monkeypatch.setattr(repair, "commit_day_manifest", original)
    assert apply_plan(fs, Client(), plan, config, work)[0]["status"] == "success"


def test_local_new_active_run_blocks_repair_before_publication(store, tmp_path, monkeypatch):
    fs, _ = store
    definition, contexts, config, plan, _ = setup_case(fs, tmp_path, monkeypatch)
    identity = definition.identity
    class Client:
        def post(self, *_):
            write_run_manifest(fs, identity, RunManifest(
                run_id="concurrent-worker", source=identity.source, resource=identity.resource,
                start_date=DAY, end_date=DAY, total_dates=1, status=RunStatus.RUNNING,
                started_at=datetime.now(UTC),
            ))
            return {"bidoNotifyContractorP": contexts[1]}
    with pytest.raises(ValueError, match="another worker"):
        apply_plan(fs, Client(), plan, config, tmp_path / "repair")
    assert select_committed_days(fs, definition, DAY, DAY)[0].run_id == "baseline"


def test_local_audit_freezes_selection_and_resume_does_not_double_count(store, tmp_path, monkeypatch):
    from procurement.quality import audit

    fs, _ = store
    definition, contexts, config, plan, _ = setup_case(fs, tmp_path, monkeypatch)
    selection = plan["days"][0]["selection"]
    monkeypatch.setattr(audit, "frozen_selection", lambda *args: [selection])
    output = tmp_path / "resumable-audit"
    kwargs = {"resource": definition.identity.resource, "year": 2025, "config": config,
              "output": output, "search_days": {str(DAY): contexts}}
    first = audit.run_audit(fs, **kwargs)
    monkeypatch.setattr(audit, "frozen_selection", lambda *args: pytest.fail("Selection was refrozen"))
    second = audit.run_audit(fs, **kwargs, resume=True)
    assert first["records"] == second["records"] == 2
    assert second["quality_status"] == {"pass": 1, "fail": 1}
    checkpoint = output / "days" / f"{DAY}.json"
    checkpoint.write_text("{}", encoding="utf-8")
    with pytest.raises(ValueError, match="checkpoint changed"):
        audit.run_audit(fs, **kwargs, resume=True)


def test_one_command_resumes_after_audit_failure_without_refetching(store, tmp_path, monkeypatch):
    from procurement.quality import audit, workflow

    fs, _ = store
    definition, contexts, config, _plan, _ = setup_case(fs, tmp_path, monkeypatch)
    write_json(tmp_path / "audit/summary.json", {
        "status": "complete", "records": 2, "quality_status": {"pass": 1, "fail": 1},
        "days": {"complete": 1}, "tables": {TABLES["standard"]: 2}, "fully_verified": False,
    })
    monkeypatch.setattr(workflow, "load_config", lambda *_: config)
    snapshot_dir = tmp_path / "snapshot"
    index = {"status": "complete", "year": 2025}
    write_json(snapshot_dir / "index.json", index)
    monkeypatch.setattr(workflow, "load_snapshot", lambda *_: (index, {str(DAY): contexts}))
    original_selection = audit.frozen_selection
    monkeypatch.setattr(audit, "frozen_selection", lambda fs, resource, *_: original_selection(fs, resource, DAY, DAY))
    directory = tmp_path / "job"
    workflow.initialize_job(directory, 2025, existing={
        "snapshot": snapshot_dir, "before": tmp_path / "audit", "plan": tmp_path / "plan.json",
        "repair": tmp_path / "repair",
    })
    calls = []
    class Client:
        def post(self, *_):
            calls.append(1)
            return {"bidoNotifyContractorP": contexts[1]}
    original_audit = workflow.run_audit
    monkeypatch.setattr(workflow, "run_audit", lambda *_, **__: (_ for _ in ()).throw(OSError("Audit interrupted")))
    with pytest.raises(OSError, match="Audit interrupted"):
        workflow.run_workflow(fs, Client(), year=2025, directory=directory)
    state = read_json(directory / "job.json")
    assert state["stage"] == "after_audit" and state["status"] == "failed"
    assert select_committed_days(fs, definition, DAY, DAY)[0].run_id != "baseline"
    monkeypatch.setattr(workflow, "run_audit", original_audit)
    result = workflow.run_workflow(fs, Client(), year=2025, directory=directory)
    assert result["status"] == "complete"
    assert result["counts"] == {"copied": 1, "refetched": 1, "moved": 1, "resolved_failures": 1}
    assert calls == [1]
