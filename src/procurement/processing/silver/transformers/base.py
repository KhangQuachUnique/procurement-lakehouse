"""Base class for Silver resource transformers providing shared scaffolding and helpers."""

import json
from abc import ABC, abstractmethod
from typing import Any

from procurement.processing.silver.contracts import VERSION
from procurement.processing.silver.parsers import clean_text
from procurement.processing.silver.protocols import (
    EntityIdentity,
    EntityRevision,
    ExtractedChild,
    ExtractedReference,
    QualityIssue,
    QuarantineRecord,
    RawRecordEnvelope,
    SilverTransformerProtocol,
    TransformedRecord,
)
from procurement.processing.silver.selection import digest


def canonical_json(value: Any) -> str:
    """Deterministic, sorted JSON serialization."""
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, default=str)


class BaseResourceTransformer(ABC, SilverTransformerProtocol):
    """Abstract base transformer implementing common scaffolding.

    Every resource transformer should inherit from this class and implement:
    - table_name property
    - entity_type property
    - id_scheme property
    - extract_root(payload)
    - extract_source_identity(root)
    - extract_attributes(root, revision_id, entity_id)
    - extract_references(root, revision_id)
    - extract_children(payload, revision_id)
    """

    mapping_version: str = VERSION.get("mapping", "1")

    @property
    @abstractmethod
    def table_name(self) -> str:
        """The Bronze table name this transformer is responsible for."""
        ...

    @property
    @abstractmethod
    def entity_type(self) -> str:
        """The logical domain entity type (e.g. 'project', 'plan', 'package', etc.)."""
        ...

    @property
    @abstractmethod
    def id_scheme(self) -> str:
        """The ID scheme ('uuid', 'notify_no', etc.)."""
        ...

    @abstractmethod
    def extract_root(self, payload: dict[str, Any]) -> dict[str, Any] | None:
        """Extract the authoritative root dictionary from payload."""
        ...

    @abstractmethod
    def extract_source_identity(self, root: dict[str, Any]) -> str | None:
        """Extract the natural primary key string from the root dictionary."""
        ...

    @abstractmethod
    def extract_attributes(
        self,
        root: dict[str, Any],
        revision_id: str,
        entity_id: str,
        issues: list[QualityIssue],
        obs_id: str,
    ) -> dict[str, Any]:
        """Extract typed, normalized domain attributes."""
        ...

    def extract_references(
        self,
        root: dict[str, Any],
        revision_id: str,
    ) -> list[ExtractedReference]:
        """Extract foreign key references to other domain entities. Default empty."""
        return []

    def extract_children(
        self,
        payload: dict[str, Any],
        revision_id: str,
        issues: list[QualityIssue],
        obs_id: str,
    ) -> list[ExtractedChild]:
        """Extract sub-components / child collections. Default empty."""
        return []

    def build_observation(self, envelope: RawRecordEnvelope) -> tuple[str, dict[str, Any]]:
        """Create physical observation metadata and unique observation_id."""
        obs_id = digest([
            envelope.namespace,
            envelope.file_key,
            envelope.file_sha256,
            envelope.row_ordinal,
        ])
        observation = {
            "observation_id": obs_id,
            "resource": envelope.resource,
            "source_date": envelope.source_date,
            "run_id": envelope.run_id,
            "table_name": envelope.table_name,
            "file_key": envelope.file_key,
            "file_sha256": envelope.file_sha256,
            "row_ordinal": envelope.row_ordinal,
            "observed_at": envelope.ingested_at.isoformat(),
            "source_id": envelope.source_id,
            "source_version": envelope.source_version,
            "content_hash": envelope.content_hash,
            "payload_json": canonical_json(envelope.payload),
        }
        return obs_id, observation

    def transform(self, envelope: RawRecordEnvelope) -> TransformedRecord:
        """Execute the end-to-end transformation flow for a single record."""
        obs_id, observation = self.build_observation(envelope)
        issues: list[QualityIssue] = []

        root = self.extract_root(envelope.payload)
        if not isinstance(root, dict):
            issue = QualityIssue(
                issue_id=digest([obs_id, "missing_root", "root"]),
                observation_id=obs_id,
                code="missing_root",
                path="root",
                severity="error",
            )
            return TransformedRecord(
                observation=observation,
                issues=[issue],
                quarantine=QuarantineRecord(observation_id=obs_id, reason="missing_root"),
            )

        raw_id = self.extract_source_identity(root)
        source_id = clean_text(raw_id)
        if not source_id:
            issue = QualityIssue(
                issue_id=digest([obs_id, "missing_identity", "id"]),
                observation_id=obs_id,
                code="missing_identity",
                path="id",
                severity="error",
            )
            return TransformedRecord(
                observation=observation,
                issues=[issue],
                quarantine=QuarantineRecord(observation_id=obs_id, reason="missing_identity"),
            )

        # Build Canonical Entity
        entity_id = digest([envelope.namespace, self.entity_type, self.id_scheme, source_id])
        entity = EntityIdentity(
            entity_id=entity_id,
            namespace=envelope.namespace,
            entity_type=self.entity_type,
            id_scheme=self.id_scheme,
            source_identity=source_id,
        )

        # Build Immutable Entity Revision
        semantic_hash = digest(envelope.payload)
        source_ver = None if self.entity_type == "package" else clean_text(envelope.source_version)
        revision_id = digest([entity_id, source_ver, semantic_hash, VERSION])
        revision = EntityRevision(
            revision_id=revision_id,
            entity_id=entity_id,
            source_version=source_ver,
            semantic_hash=semantic_hash,
            mapping_version=self.mapping_version,
            payload_json=canonical_json(envelope.payload),
        )

        # Extract Typed Attributes, References & Children
        typed = self.extract_attributes(root, revision_id, entity_id, issues, obs_id)
        references = self.extract_references(root, revision_id)
        children = self.extract_children(envelope.payload, revision_id, issues, obs_id)

        return TransformedRecord(
            observation=observation,
            entity=entity,
            revision=revision,
            typed_attributes=typed,
            references=references,
            children=children,
            issues=issues,
            quarantine=None,
        )
