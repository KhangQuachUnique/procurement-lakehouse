from datetime import date

from procurement.common.resources import ResourceIdentity
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
            "source_date": "2026-09-11",
            "status": "completed_with_errors",
            "total_errors": 2,
        }


class StubErrors:
    def list(self, identity: ResourceIdentity, **_kwargs):
        return [
            {"error_id": "e1", "resolution_status": "recovered"},
            {"error_id": "e2", "resolution_status": "pending"},
        ]


def test_run_health_is_derived_without_mutating_historical_status() -> None:
    service = OpsService(StubRuns(), StubErrors())  # type: ignore[arg-type]
    run = service.get_run(
        source="muasamcong",
        resource="khlcnt",
        source_date=date(2026, 9, 11),
        run_id="run-a",
    )
    assert run is not None
    assert run["historical_status"] == "completed_with_errors"
    assert run["current_health"] == "partial"
    assert run["recovered_errors"] == 1
    assert run["unresolved_errors"] == 1
