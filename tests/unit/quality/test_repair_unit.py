"""Unit tests for the quality repair package."""

from unittest.mock import Mock

import pytest

from procurement.quality.repair import (
    validate_plan,
)


def test_validate_plan_empty_or_tampered():
    config = Mock()
    config.fingerprint = "hash123"
    config.resource = "notify_contractor"

    plan = {
        "schema_version": 1,
        "resource": "notify_contractor",
        "config_hash": "wrong_hash",
        "plan_hash": "some_hash",
        "days": [],
    }
    with pytest.raises(ValueError, match="Repair plan or config changed"):
        validate_plan(plan, config)
