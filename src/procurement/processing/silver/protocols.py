"""Protocols and standardized data structures for Silver layer transformations.

Any team member adding a new resource transformer must implement
`SilverTransformerProtocol` and return `TransformedRecord`.
"""

from dataclasses import dataclass, field
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
    payload_json: str


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
    """Sub-entity or component (e.g. lot, participant, item, document)."""

    child_id: str
    revision_id: str
    kind: str
    role: str
    path: str
    ordinal: int
    source_id: str | None
    payload_json: str


@dataclass(frozen=True)
class QualityIssue:
    """Audit quality issue or warning observed during transformation."""

    issue_id: str
    observation_id: str
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

    def to_legacy_dict(self) -> dict[str, Any]:
        """Convert to dictionary format expected by downstream reconciliation / assemble()."""
        return {
            "observation": self.observation,
            "entity": {
                "entity_id": self.entity.entity_id,
                "namespace": self.entity.namespace,
                "entity_type": self.entity.entity_type,
                "id_scheme": self.entity.id_scheme,
                "source_identity": self.entity.source_identity,
            }
            if self.entity
            else None,
            "revision": {
                "revision_id": self.revision.revision_id,
                "entity_id": self.revision.entity_id,
                "source_version": self.revision.source_version,
                "semantic_hash": self.revision.semantic_hash,
                "mapping_version": self.revision.mapping_version,
                "payload_json": self.revision.payload_json,
            }
            if self.revision
            else None,
            "typed": self.typed_attributes,
            "references": [
                {
                    "relationship_id": ref.relationship_id,
                    "from_revision_id": ref.from_revision_id,
                    "target_type": ref.target_type,
                    "id_scheme": ref.id_scheme,
                    "target_identity": ref.target_identity,
                    "field": ref.field_name,
                }
                for ref in self.references
            ],
            "children": [
                {
                    "child_id": ch.child_id,
                    "revision_id": ch.revision_id,
                    "kind": ch.kind,
                    "role": ch.role,
                    "path": ch.path,
                    "ordinal": ch.ordinal,
                    "source_id": ch.source_id,
                    "payload_json": ch.payload_json,
                }
                for ch in self.children
            ],
            "issues": [
                {
                    "issue_id": iss.issue_id,
                    "observation_id": iss.observation_id,
                    "code": iss.code,
                    "path": iss.path,
                    "severity": iss.severity,
                }
                for iss in self.issues
            ],
            "quarantine": {
                "observation_id": self.quarantine.observation_id,
                "reason": self.quarantine.reason,
            }
            if self.quarantine
            else None,
        }


@runtime_checkable
class SilverTransformerProtocol(Protocol):
    """Protocol that all resource-specific transformers must satisfy."""

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
