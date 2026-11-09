"""Single warehouse execution entrypoint shared by CLI and scheduler adapters."""

from pathlib import Path

from .state import StateStore


def execute_run(run_id, root, *, allow_isolated_publication=False, on_phase=None):
    if not allow_isolated_publication:
        raise ValueError("Explicit isolated-publication opt-in required")
    root = Path(root)
    if not (root / "state.duckdb").is_file():
        raise ValueError("No existing Gold plan/state")
    with StateStore(root) as state:
        plan = state.plan(run_id)
        if state.phase(run_id).value == "COMMITTED":
            return state.status(run_id)
    from procurement.common.settings import Settings
    from procurement.storage.iceberg import RestCatalog, connect

    from procurement.gold.adapters.driven.iceberg.iceberg_publisher import IcebergPublisher
    from procurement.gold.adapters.driven.silver.iceberg_reader import IcebergReader
    from .backend import PipelineBackend
    from .gold_service import GoldService

    config = Settings.from_environment().iceberg_config()
    with (
        # Local computation separately owns the frozen engine budget. Sharing
        # that same budget with a second connection would double peak RAM.
        connect(config, max_memory="512MB", threads=plan.budget.threads) as remote,
        RestCatalog(config) as catalog,
    ):
        backend = PipelineBackend(
            IcebergReader(remote, catalog, plan.source.namespace), IcebergPublisher(remote, catalog)
        )
        return GoldService(root, backend).resume(run_id, on_phase=on_phase)
