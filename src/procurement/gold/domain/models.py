"""Immutable request domain models and contracts for the Gold dimensional layer.

Zero clock, SQL engine, or external network dependencies.
"""

import hashlib
import json
import re
from datetime import date, datetime
from enum import StrEnum
from pathlib import Path
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

from procurement.common.silver_gold_contract import INPUT_TABLES, PROJECTION_VERSION
from procurement.source_quality import NullSourceException, SourceException, validate_policy

SCHEMA_VERSION = "gold_compact_v2"


class Contract(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


def identifier(value: str) -> str:
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", value):
        raise ValueError(f"Unsafe identifier: {value!r}")
    return value


def canonical(value) -> str:
    if isinstance(value, BaseModel):
        value = value.model_dump(mode="json")
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def digest(value) -> str:
    return hashlib.sha256(canonical(value).encode()).hexdigest()


class Snapshot(Contract):
    table: str
    table_uuid: str = Field(min_length=1)
    snapshot_id: int | None = Field(default=None, ge=0)
    rows: int = Field(ge=0)

    @model_validator(mode="after")
    def valid(self):
        if self.table not in INPUT_TABLES:
            raise ValueError("Unexpected Silver input table")
        if self.snapshot_id is None and self.rows:
            raise ValueError("Nonempty input requires a committed snapshot")
        return self


class SilverReadyInput(Contract):
    """Producer attestation, not a readiness conclusion inferred by Gold.

    A production adapter must also match UUIDs/snapshots against the catalog.
    A trusted producer supplies coverage/quality; syntactic validation alone
    cannot establish that a receipt is truthful.
    """

    receipt_version: Literal[1, 2, 3] = 1
    input_id: str = Field(min_length=1)
    cohort_id: str = Field(min_length=1)
    namespace: str
    mapping_version: str = Field(min_length=1)
    projection_version: Literal["silver_gold_projection_v1"] = PROJECTION_VERSION
    completed_at: datetime
    source_start: date
    source_end: date
    source_complete: Literal[True]
    blocking_issues: int = Field(ge=0, strict=True)
    quarantined: int = Field(ge=0, strict=True)
    approved_exceptions: tuple[SourceException | NullSourceException, ...] = ()
    approval_digest: str | None = None
    quality_status: Literal["STRICT", "ACCEPTED_WITH_EXPLICIT_SOURCE_EXCEPTION"] = "STRICT"
    tables: tuple[Snapshot, ...]

    @model_validator(mode="after")
    def valid(self):
        validate_policy(
            self.receipt_version,
            self.quarantined,
            self.blocking_issues,
            self.approved_exceptions,
            self.approval_digest,
            self.quality_status,
        )
        identifier(self.namespace)
        if self.source_start > self.source_end:
            raise ValueError("Inverted source coverage")
        if self.completed_at.utcoffset() is None:
            raise ValueError("Completion timestamp requires timezone")
        if len(self.tables) != len(INPUT_TABLES) or {t.table for t in self.tables} != set(
            INPUT_TABLES
        ):
            raise ValueError("Readiness requires exactly eight unique Silver tables")
        return self


class ResourceBudget(Contract):
    memory_limit: str = "1GB"
    threads: int = Field(default=1, ge=1, strict=True)
    batch_rows: int = Field(default=10000, ge=1, strict=True)

    @model_validator(mode="after")
    def valid(self):
        if not re.fullmatch(r"[1-9][0-9]*(?:MB|GB)", self.memory_limit):
            raise ValueError("Explicit MB or GB memory budget required")
        return self


class RunPlan(Contract):
    run_id: str
    mode: Literal["backfill", "incremental"]
    schema_version: Literal["gold_compact_v2"] = SCHEMA_VERSION
    logic_version: str = Field(pattern=r"^[0-9a-f]{64}$")
    reference_version: str = Field(min_length=1)
    references_json: str = "{}"
    change_journal: str | None = None
    target_namespace: str
    base_release: str | None = None
    source: SilverReadyInput
    source_cutoff: date
    event_start: date
    event_end: date
    observation_day: date
    budget: ResourceBudget = ResourceBudget()

    @model_validator(mode="after")
    def valid(self):
        from .policies import ReferencePolicy

        policy = ReferencePolicy.model_validate_json(self.references_json)
        if policy.version != self.reference_version:
            raise ValueError("Reference policy content/version mismatch")
        identifier(self.run_id)
        identifier(self.target_namespace)
        if not self.target_namespace.startswith("gold_v2_"):
            raise ValueError("V2 candidate requires an explicit gold_v2_* target")
        if self.target_namespace == self.source.namespace:
            raise ValueError("Input and target must be isolated")
        if (
            self.source_cutoff > self.source.source_end
            or self.source_cutoff < self.source.source_start
        ):
            raise ValueError("Source cutoff outside accepted source coverage")
        if self.observation_day < self.source_cutoff:
            raise ValueError("Observation day precedes source cutoff")
        if self.event_start > self.event_end:
            raise ValueError("Inverted event scope")
        if (self.mode == "incremental") != (self.base_release is not None):
            raise ValueError("Only incremental runs require an accepted base release")
        return self

    @property
    def references(self):
        return json.loads(self.references_json)

    @property
    def identity(self) -> str:
        return digest(self)


class Phase(StrEnum):
    PLANNED = "PLANNED"
    INPUTS_FROZEN = "INPUTS_FROZEN"
    CALCULATED = "CALCULATED"
    PREPARED = "PREPARED"
    PUBLISHED_VERIFIED = "PUBLISHED_VERIFIED"
    ACTIVE = "ACTIVE"
    COMMITTED = "COMMITTED"


def compute_gold_logic_version() -> str:
    """Hash all runtime contracts, SQL and projection/selection code, not CLI text."""
    v2_root = Path(__file__).parents[1] / "v2"
    if v2_root.exists():
        root = v2_root
    else:
        root = Path(__file__).parents[1]
    files = sorted(
        p for p in root.rglob("*") if p.suffix in {".py", ".sql", ".json"} and p.name != "cli.py"
    )
    project = Path(__file__).parents[3]
    shared = [
        project / "common/silver_gold_contract.py",
        project / "silver/application/change_journal.py",
        project / "source_quality.py",
        project / "readiness_with_exception.py",
    ]
    entries = [
        (str(p.relative_to(root)), hashlib.sha256(p.read_bytes()).hexdigest()) for p in files
    ]
    entries += [
        (str(p.relative_to(project)), hashlib.sha256(p.read_bytes()).hexdigest())
        for p in shared
        if p.exists()
    ]
    return digest(entries)
