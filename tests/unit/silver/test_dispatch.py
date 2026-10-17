"""There is one dispatch path; unsupported mappings never silently fall back."""

from dataclasses import replace

import pytest

from procurement.silver.application.observations import transform
from procurement.silver.domain.registry import require_transformer, transformer_contracts
from tests.unit.silver.test_project_transformer import make_envelope


def test_unimplemented_resource_is_explicit():
    with pytest.raises(ValueError, match="not implemented"):
        require_transformer("contractor_result_detail")


def test_project_build_uses_same_registered_transformer():
    envelope = make_envelope(
        {"projectDTO": {"id": "p", "name": "Project", "investTotal": 10, "investTotalUnit": "VND"}}
    )
    result = transform(
        envelope.namespace,
        {"resource": envelope.resource, "source_date": envelope.source_date},
        {"table": envelope.table_name, "key": envelope.file_key, "sha256": envelope.file_sha256},
        0,
        {
            "payload": envelope.payload,
            "ingested_at": envelope.ingested_at,
            "run_id": envelope.run_id,
            "source_id": envelope.source_id,
            "source_version": envelope.source_version,
            "content_hash": envelope.content_hash,
        },
    )
    expected = require_transformer(envelope.table_name).transform(envelope).to_dict()
    assert result == expected
    assert result["typed"]["currency"] == "VND"


def test_cache_contract_only_includes_selected_transformers():
    contracts = transformer_contracts({"project_detail"})
    assert set(contracts) == {"project_detail"}
    assert "notice" not in contracts["project_detail"]


def test_typed_child_identity_must_match_raw_child():
    from procurement.silver.application.revisions import assemble
    from procurement.silver.domain.protocols import ExtractedChild

    envelope = make_envelope({"projectDTO": {"id": "p"}})
    result = require_transformer("project_detail").transform(envelope)
    child = ExtractedChild(
        "c",
        result.revision.revision_id,
        "location",
        "location",
        "locations",
        0,
        None,
        "{}",
        "notice_location",
        {"child_id": "wrong", "revision_id": result.revision.revision_id},
    )
    result.children = [child]
    with pytest.raises(ValueError, match="identity"):
        assemble([result.to_dict()])
    result.children = [
        replace(
            child,
            typed_attributes={"child_id": "c", "revision_id": result.revision.revision_id},
        )
    ]
    tables = assemble([result.to_dict()])
    assert len(tables["notice_location"]) == 1
    assert "child" not in tables
