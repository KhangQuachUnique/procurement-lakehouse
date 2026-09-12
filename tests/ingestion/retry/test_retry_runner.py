from collections.abc import Iterator
from datetime import date
from types import SimpleNamespace
from typing import Any

from procurement.common.resources import ResourceIdentity
from procurement.ingestion.engine.models import ResourceSpec
from procurement.ingestion.retry import runner


def _spec() -> ResourceSpec:
    identity = ResourceIdentity("muasamcong", "khlcnt")

    def retry_error(*, error: dict[str, Any], retry_run_id: str, source_date: date) -> Iterator[dict[str, Any]]:
        yield {"_resource": "khlcnt_plan_detail", "_source_id": error["source_id"], "_run_id": retry_run_id}

    return ResourceSpec(identity=identity, pipeline_name="p", dataset_name="d", fetch_page=lambda **_: {}, build_query_definition=lambda **_: {}, iter_records=lambda **_: iter(()), retry_error=retry_error)


def test_retry_success_marks_resolution_and_keeps_original_error_immutable(monkeypatch) -> None:
    error = {"error_id": "err-1", "run_id": "run-a", "source": "muasamcong", "resource": "khlcnt", "source_date": "2026-09-11", "source_id": "plan-1", "stage": "plan_detail", "retryable": True}
    states: list[dict[str, Any]] = []
    monkeypatch.setattr(runner, "list_error_records", lambda *_args, **_kwargs: [error])
    monkeypatch.setattr(runner, "read_error_resolution", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(runner, "write_error_resolution", lambda *_args, **_kwargs: states.append(dict(_args[-1])) or "s3://state")
    monkeypatch.setattr(runner, "write_retry_manifest", lambda *_args, **_kwargs: "s3://retry")
    monkeypatch.setattr(runner, "create_bronze_destination", lambda **_: object())
    monkeypatch.setattr(runner, "create_bronze_resource", lambda records, **_: records)

    class Pipeline:
        def run(self, records):
            assert list(records)[0]["_source_id"] == "plan-1"
            return SimpleNamespace(loads_ids=["load-1"])

    monkeypatch.setattr(runner.dlt, "pipeline", lambda **_: Pipeline())
    runner.retry_resource_errors(fs=object(), spec=_spec(), source_date=date(2026, 9, 11))  # type: ignore[arg-type]
    assert error.get("retry_success") is None
    assert states[-1]["status"] == "recovered"
    assert states[-1]["attempts"] == 1
    assert states[-1]["original_run_id"] == "run-a"
