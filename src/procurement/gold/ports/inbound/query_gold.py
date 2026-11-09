"""Inbound Port for querying Gold analytical datasets."""

from typing import Any, Protocol


class QueryGoldPort(Protocol):
    """Port for querying dimensions and rolled-up facts in lake.gold_v2."""

    def query_facts(self, db: Any, fact_name: str, **kwargs: Any) -> Any:
        ...

    def get_dim_geography(self, db: Any) -> list[dict[str, Any]]:
        ...
