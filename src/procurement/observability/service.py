from datetime import date
from typing import Any

from procurement.common.resources import ResourceIdentity
from procurement.observability.repositories import ErrorRepository, RunRepository


class OpsService:
    def __init__(self, runs: RunRepository, errors: ErrorRepository) -> None:
        self._runs = runs
        self._errors = errors

    @staticmethod
    def identity(source: str, resource: str) -> ResourceIdentity:
        return ResourceIdentity(source=source, resource=resource)

    def list_runs(
        self,
        *,
        source: str,
        resource: str,
        start_date: date | None = None,
        end_date: date | None = None,
    ) -> list[dict[str, Any]]:
        return self._runs.list(
            self.identity(source, resource),
            start_date=start_date,
            end_date=end_date,
        )

    def get_run(self, *, source: str, resource: str, run_id: str) -> dict[str, Any] | None:
        return self._runs.get(self.identity(source, resource), run_id)

    def list_errors(
        self,
        *,
        source: str,
        resource: str,
        source_date: date | None = None,
        run_id: str | None = None,
    ) -> list[dict[str, Any]]:
        return self._errors.list(
            self.identity(source, resource),
            source_date=source_date,
            run_id=run_id,
        )
