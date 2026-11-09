from unittest.mock import Mock

import pytest
from typer.testing import CliRunner

from procurement.gold.adapters.driving.cli import app
from procurement.tools.gold_main_history import run
from procurement.tools.silver_bootstrap_writer import load_empty


def test_managed_opt_in_does_not_unlock_arbitrary_source_namespaces(tmp_path):
    for ns, flag in [("silver_v2_main_2022_2025", False), ("silver", True), ("unrelated", True)]:
        db, catalog = Mock(), Mock()
        with pytest.raises(ValueError, match="isolated"):
            load_empty(db, catalog, ns, {}, tmp_path, "id", allow_managed_namespace=flag)
        db.execute.assert_not_called()
        catalog.table.assert_not_called()


def test_managed_rejects_unknown_nonempty_table_without_a_durable_intent(tmp_path):
    db, catalog = Mock(), Mock()
    catalog.table.return_value = {
        "metadata": {"format-version": 2, "current-snapshot-id": 123, "table-uuid": "old"}
    }
    with pytest.raises(ValueError, match="existing nonempty"):
        load_empty(
            db,
            catalog,
            "silver_v2_main_2022_2025",
            {"observation": "selected"},
            tmp_path,
            "id",
            allow_managed_namespace=True,
        )
    assert not any("INSERT INTO" in call.args[0] for call in db.execute.call_args_list)


def test_official_history_requires_consent_before_creating_any_output(tmp_path):
    output = tmp_path / "no-output"
    with pytest.raises(ValueError, match="opt-in"):
        run(tmp_path / "missing-source", output)
    assert not output.exists()
    result = CliRunner().invoke(
        app, ["materialize-history", str(tmp_path / "missing-source"), str(output)]
    )
    assert result.exit_code != 0
    assert not output.exists()
