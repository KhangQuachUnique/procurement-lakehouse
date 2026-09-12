from procurement.observability.service import OpsService


class StubRuns:
    def list(self, *_args, **_kwargs):
        return [self.manifest()]

    def get(self, *_args, **_kwargs):
        return self.manifest()

    @staticmethod
    def manifest():
        return {
            "run_id": "run-a",
            "source": "muasamcong",
            "resource": "khlcnt",
            "start_date": "2026-09-11",
            "end_date": "2026-09-13",
            "status": "partial_failed",
            "total_dates": 3,
            "success_dates": 2,
            "failed_dates": 1,
        }


class StubErrors:
    def list(self, *_args, **_kwargs):
        return [{"error_id": "e1", "run_id": "run-a"}]


def test_ops_service_returns_persisted_run_without_retry_projection() -> None:
    service = OpsService(StubRuns(), StubErrors())  # type: ignore[arg-type]
    run = service.get_run(source="muasamcong", resource="khlcnt", run_id="run-a")

    assert run is not None
    assert run["status"] == "partial_failed"
    assert "current_health" not in run
    assert "recovered_errors" not in run

    errors = service.list_errors(source="muasamcong", resource="khlcnt", run_id="run-a")
    assert errors == [{"error_id": "e1", "run_id": "run-a"}]
