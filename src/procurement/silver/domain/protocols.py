"""Protocols and standardized data structures for Silver layer domain entities.

Pure domain definitions with zero I/O or storage dependencies.
"""

from dataclasses import asdict, dataclass, field
from datetime import datetime
from typing import Any, Protocol, runtime_checkable


@dataclass(frozen=True)
class RawRecordEnvelope:
    """Incoming physical observation from Bronze."""

    namespace: str
    table_name: str
    resource: str
    source_date: str
    run_id: str
    row_ordinal: int
    file_key: str
    file_sha256: str
    source_id: str
    source_version: str | None
    content_hash: str
    ingested_at: datetime
    payload: dict[str, Any]


@dataclass(frozen=True)
class EntityIdentity:
    """Canonical identity of a domain entity."""

    entity_id: str
    namespace: str
    entity_type: str
    id_scheme: str
    source_identity: str


@dataclass(frozen=True)
class EntityRevision:
    """Versioned immutable state of an entity."""

    revision_id: str
    entity_id: str
    source_version: str | None
    semantic_hash: str
    mapping_version: str
    payload_json: str | None = None


@dataclass(frozen=True)
class ExtractedReference:
    """Foreign key link pointing from this entity revision to a target entity."""

    relationship_id: str
    from_revision_id: str
    target_type: str
    id_scheme: str
    target_identity: str | None
    field_name: str


@dataclass(frozen=True)
class ExtractedChild:
    """Sub-entity or component (e.g. lot, location, reoffer submission)."""

    child_id: str
    revision_id: str
    kind: str
    role: str
    path: str
    ordinal: int
    source_id: str | None
    payload_json: str | None = None
    typed_table: str | None = None
    typed_attributes: dict[str, Any] | None = None


@dataclass(frozen=True)
class QualityIssue:
    """Audit quality issue or warning observed during transformation."""

    issue_id: str
    observation_id: str | None
    code: str
    path: str
    severity: str = "warn"  # "warn" | "error"


@dataclass(frozen=True)
class QuarantineRecord:
    """Record that cannot be emitted as a valid entity due to missing essential identity/root."""

    observation_id: str
    reason: str


@dataclass
class TransformedRecord:
    """Unified result output from transforming a single raw Bronze record."""

    observation: dict[str, Any]
    entity: EntityIdentity | None = None
    revision: EntityRevision | None = None
    typed_attributes: dict[str, Any] | None = None
    references: list[ExtractedReference] = field(default_factory=list)
    children: list[ExtractedChild] = field(default_factory=list)
    issues: list[QualityIssue] = field(default_factory=list)
    quarantine: QuarantineRecord | None = None

    def to_dict(self) -> dict[str, Any]:
        """Serialize the canonical assembler/cache contract without recursive asdict() overhead."""
        entity = self.entity
        revision = self.revision
        quarantine = self.quarantine
        return {
            "observation": self.observation,
            "entity": {
                "entity_id": entity.entity_id,
                "namespace": entity.namespace,
                "entity_type": entity.entity_type,
                "id_scheme": entity.id_scheme,
                "source_identity": entity.source_identity,
            }
            if entity is not None
            else None,
            "revision": {
                "revision_id": revision.revision_id,
                "entity_id": revision.entity_id,
                "source_version": revision.source_version,
                "semantic_hash": revision.semantic_hash,
                "mapping_version": revision.mapping_version,
                "payload_json": revision.payload_json,
            }
            if revision is not None
            else None,
            "typed": self.typed_attributes,
            "references": [
                {
                    "relationship_id": r.relationship_id,
                    "from_revision_id": r.from_revision_id,
                    "target_type": r.target_type,
                    "id_scheme": r.id_scheme,
                    "target_identity": r.target_identity,
                    "field": r.field_name,
                }
                for r in self.references
            ],
            "children": [
                {
                    "child_id": c.child_id,
                    "revision_id": c.revision_id,
                    "kind": c.kind,
                    "role": c.role,
                    "path": c.path,
                    "ordinal": c.ordinal,
                    "source_id": c.source_id,
                    "payload_json": c.payload_json,
                    "typed_table": c.typed_table,
                    "typed_attributes": c.typed_attributes,
                }
                for c in self.children
            ],
            "issues": [
                {
                    "issue_id": i.issue_id,
                    "observation_id": i.observation_id,
                    "code": i.code,
                    "path": i.path,
                    "severity": i.severity,
                }
                for i in self.issues
            ],
            "quarantine": {
                "observation_id": quarantine.observation_id,
                "reason": quarantine.reason,
            }
            if quarantine is not None
            else None,
        }


@runtime_checkable
class SilverTransformerProtocol(Protocol):
    """Protocol that all resource-specific transformers must satisfy."""

    revision_contracts: dict[str, str]

    @property
    def table_name(self) -> str:
        """Name of the Bronze table handled by this transformer."""
        ...

    @property
    def entity_type(self) -> str:
        """Domain entity type (e.g. 'project', 'plan', 'package', 'notice', 'result', 'opening')."""
        ...

    def transform(self, envelope: RawRecordEnvelope) -> TransformedRecord:
        """Transform a raw envelope into a TransformedRecord."""
        ...


__all__ = [
    "EntityIdentity",
    "EntityRevision",
    "ExtractedChild",
    "ExtractedReference",
    "QualityIssue",
    "QuarantineRecord",
    "RawRecordEnvelope",
    "SilverTransformerProtocol",
    "TransformedRecord",
]
