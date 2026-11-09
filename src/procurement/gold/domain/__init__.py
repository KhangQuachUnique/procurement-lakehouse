"""Gold Domain Layer.

Contains purely domain models, schema specifications, policies, and geographic dictionaries.
Free from all DB engine, file storage, network or CLI dependencies.
"""

from .geography import VIETNAM_63_PROVINCES, ProvinceInfo, get_geography_records, normalize_province
from .models import (
    Phase,
    ResourceBudget,
    RunPlan,
    SilverReadyInput,
    Snapshot,
    canonical,
    compute_gold_logic_version,
    digest,
    identifier,
)
from .policies import Geography, ReferencePolicy, ValueBand, apply, apply_dictionary
from .schema import (
    DIMENSIONS,
    FACTS,
    FOREIGN_KEYS,
    TABLES,
    Column,
    Table,
    check_schema,
    create_tables,
    group_hash,
)
from .validation import validate

__all__ = [
    "DIMENSIONS",
    "FACTS",
    "FOREIGN_KEYS",
    "TABLES",
    "VIETNAM_63_PROVINCES",
    "Column",
    "Contract",
    "Geography",
    "Phase",
    "ProvinceInfo",
    "ReferencePolicy",
    "ResourceBudget",
    "RunPlan",
    "SilverReadyInput",
    "Snapshot",
    "Table",
    "ValueBand",
    "apply",
    "apply_dictionary",
    "canonical",
    "check_schema",
    "compute_gold_logic_version",
    "create_tables",
    "digest",
    "get_geography_records",
    "group_hash",
    "identifier",
    "normalize_province",
    "validate",
]
