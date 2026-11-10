"""Smoke tests for the Bronze ZIP CLI."""

from pathlib import Path
from unittest.mock import Mock

from typer.testing import CliRunner

from procurement.cli import bronze_transfer


def test_import_command_uses_transfer_importer(monkeypatch, tmp_path: Path):
    archive = tmp_path / "bronze-2022.zip"
    archive.write_bytes(b"placeholder")
    imported = Mock(return_value={"mode": "import", "imported_days": [{"date": "2022-01-01"}]})
    monkeypatch.setattr(bronze_transfer, "_storage", lambda: object())
    monkeypatch.setattr(bronze_transfer, "import_bundle", imported)
    result = CliRunner().invoke(bronze_transfer.app, ["import", "--archive", str(archive)])
    assert result.exit_code == 0, result.output
    assert '"imported_days": 1' in result.output
    assert imported.call_args.kwargs["workers"] == 4
    assert imported.call_args.kwargs["dry_run"] is False


def test_import_dry_run(monkeypatch, tmp_path: Path):
    archive = tmp_path / "bronze-2022.zip"
    archive.write_bytes(b"placeholder")
    imported = Mock(return_value={"mode": "import", "planned_days": []})
    monkeypatch.setattr(bronze_transfer, "_storage", lambda: object())
    monkeypatch.setattr(bronze_transfer, "import_bundle", imported)
    result = CliRunner().invoke(
        bronze_transfer.app, ["import", "--archive", str(archive), "--dry-run"]
    )
    assert result.exit_code == 0, result.output
    assert imported.call_args.kwargs["dry_run"] is True


def test_inspect_does_not_open_storage(monkeypatch, tmp_path: Path):
    archive = tmp_path / "bronze-2022.zip"
    archive.write_bytes(b"placeholder")
    monkeypatch.setattr(
        bronze_transfer, "_storage",
        lambda: (_ for _ in ()).throw(AssertionError("no S3")),
    )
    monkeypatch.setattr(
        bronze_transfer, "inspect_bundle",
        lambda path, workers: {"mode": "inspect", "objects": 7},
    )
    result = CliRunner().invoke(bronze_transfer.app, ["inspect", "--archive", str(archive)])
    assert result.exit_code == 0, result.output
    assert '"objects": 7' in result.output
