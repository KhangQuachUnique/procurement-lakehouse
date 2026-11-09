"""Gold Dimensional Layer for Public Procurement Analytics.

Organized under Hexagonal Architecture (Ports and Adapters):
- domain: Pure business models, Star Schema contracts, 63 provinces dictionary, and policies
- ports: Inbound (use case) and outbound (storage, reader, state) interfaces
- application: GoldService lifecycle, warehouse provisioning, computation, and reconciliation
- adapters: Driven (Iceberg, Silver) and driving (Typer CLI) implementations
- service: High-level application facade
- v2: Backward-compatibility namespace for existing consumers and Dagster jobs
"""

from .domain.schema import DIMENSIONS, FACTS, FOREIGN_KEYS, TABLES
from .service import GoldService, ensure_gold_tables, promote_views, reconcile_gold

__all__ = [
    "DIMENSIONS",
    "FACTS",
    "FOREIGN_KEYS",
    "TABLES",
    "GoldService",
    "ensure_gold_tables",
    "promote_views",
    "reconcile_gold",
]
