"""Top-level facade for the Gold Dimensional Layer (Hexagonal Architecture).

Provides direct access to GoldService, domain models, schema, and application services.
"""

from procurement.gold.application.backend import PipelineBackend
from procurement.gold.application.gold_service import GoldService
from procurement.gold.application.reconciliation import ReconciliationReport, reconcile_gold
from procurement.gold.application.state import StateStore
from procurement.gold.application.warehouse import ensure_gold_tables, promote_views, seed_geography
from procurement.gold.domain.models import (
    Phase,
    ResourceBudget,
    RunPlan,
    SilverReadyInput,
    Snapshot,
)
from procurement.gold.domain.schema import DIMENSIONS, FACTS, FOREIGN_KEYS, TABLES, Column, Table

__all__ = [
    "DIMENSIONS",
    "FACTS",
    "FOREIGN_KEYS",
    "TABLES",
    "Column",
    "GoldService",
    "Phase",
    "PipelineBackend",
    "ReconciliationReport",
    "ResourceBudget",
    "RunPlan",
    "SilverReadyInput",
    "Snapshot",
    "StateStore",
    "Table",
    "ensure_gold_tables",
    "promote_views",
    "reconcile_gold",
    "seed_geography",
]
