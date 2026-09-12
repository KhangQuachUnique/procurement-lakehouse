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
        identity = self.identity(source, resource)
        return [
            self._with_current_health(identity, manifest)
            for manifest in self._runs.list(
                identity, start_date=start_date, end_date=end_date
            )
        ]

    def get_run(
        self, *, source: str, resource: str, source_date: date, run_id: str
    ) -> dict[str, Any] | None:
        identity = self.identity(source, resource)
        manifest = self._runs.get(identity, source_date, run_id)
        return None if manifest is None else self._with_current_health(identity, manifest)

    def list_errors(
        self,
        *,
        source: str,
        resource: str,
        source_date: date | None = None,
        run_id: str | None = None,
        retryable: bool | None = None,
        resolution_status: str | None = None,
    ) -> list[dict[str, Any]]:
        identity = self.identity(source, resource)
        errors = self._errors.list(
            identity,
            source_date=source_date,
            run_id=run_id,
            retryable=retryable,
        )
        if resolution_status is not None:
            errors = [
                error
                for error in errors
                if error.get("resolution_status") == resolution_status
            ]
        return errors

    def _with_current_health(
        self, identity: ResourceIdentity, manifest: dict[str, Any]
    ) -> dict[str, Any]:
        source_date = date.fromisoformat(str(manifest["source_date"]))
        run_id = str(manifest["run_id"])
        errors = self._errors.list(identity, source_date=source_date, run_id=run_id)

        recovered = sum(error["resolution_status"] == "recovered" for error in errors)
        pending = sum(
            error["resolution_status"] in {"pending", "retrying"} for error in errors
        )
        dead_letter = sum(error["resolution_status"] == "dead_letter" for error in errors)
        non_retryable = sum(
            error["resolution_status"] == "not_retryable" for error in errors
        )
        unresolved = pending + dead_letter + non_retryable

        historical_status = str(manifest.get("status", "unknown"))
        if historical_status == "failed":
            health = "failed"
        elif not errors:
            health = "healthy"
        elif unresolved == 0:
            health = "recovered"
        else:
            health = "partial"

        view = dict(manifest)
        view.update(
            {
                "historical_status": historical_status,
                "current_health": health,
                "original_errors": len(errors),
                "recovered_errors": recovered,
                "pending_errors": pending,
                "dead_letter_errors": dead_letter,
                "non_retryable_errors": non_retryable,
                "unresolved_errors": unresolved,
            }
        )
        return view
