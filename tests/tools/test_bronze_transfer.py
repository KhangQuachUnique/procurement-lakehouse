import json
from datetime import date

import pytest

from procurement.common.settings import settings
from procurement.storage.transfer import TransferError
from procurement.tools import bronze_transfer as cli


@pytest.mark.parametrize("argv", [[], ["export"], ["export", "--year", "9999", "--output", "x"],
                                 ["export", "--year", "2024", "--output", "x",
                                  "--resource", "unknown"], ["import"]])
def test_invalid_cli_arguments_exit_two(argv):
    with pytest.raises(SystemExit) as result:
        cli.main(argv)
    assert result.value.code == 2


def test_current_year_is_rejected(monkeypatch):
    from procurement.storage import transfer_archive

    monkeypatch.setattr(transfer_archive, "today_vn", lambda: date(2026, 1, 1))
    with pytest.raises(SystemExit) as result:
        cli.main(["export", "--year", "2026", "--output", "x.zip"])
    assert result.value.code == 2


def test_inspect_does_not_create_storage_client(monkeypatch, capsys):
    def forbidden():
        pytest.fail("inspect must not connect to object storage")

    monkeypatch.setattr(cli, "create_s3_filesystem", forbidden)
    monkeypatch.setattr(cli, "inspect_bundle", lambda _: {"verified": {"days": 1}})
    with pytest.raises(SystemExit) as result:
        cli.main(["inspect", "--archive", "year.zip"])
    assert result.value.code == 0
    assert json.loads(capsys.readouterr().out) == {"verified": {"days": 1}}


@pytest.mark.parametrize("interrupted", [False, True])
def test_cli_error_exit_codes_and_redaction(monkeypatch, capsys, interrupted):
    monkeypatch.setattr(settings, "OBJECT_STORAGE_SECRET_KEY", "private-test-secret")

    def fail(_):
        if interrupted:
            raise KeyboardInterrupt
        raise TransferError("storage private-test-secret ?token=another-secret", {"uploaded": 2})

    monkeypatch.setattr(cli, "inspect_bundle", fail)
    with pytest.raises(SystemExit) as result:
        cli.main(["inspect", "--archive", "year.zip"])
    assert result.value.code == (130 if interrupted else 1)
    output = capsys.readouterr()
    assert "private-test-secret" not in output.out + output.err
    assert "another-secret" not in output.out + output.err
    report = json.loads(output.out)
    if not interrupted:
        assert report["uploaded"] == 2
        assert "[REDACTED]" in report["message"]


@pytest.mark.parametrize("mode", ["export", "import"])
def test_cli_dispatches_storage_options(monkeypatch, capsys, mode, tmp_path):
    store = object()
    monkeypatch.setattr(cli, "create_s3_filesystem", lambda: store)
    captured = {}

    def operation(fs, *args, **kwargs):
        assert fs is store
        captured.update(kwargs)
        return {"coverage_complete": False}

    monkeypatch.setattr(cli, f"{mode}_bundle", operation)
    arguments = (["--year", "2024", "--resource", "khlcnt", "--output", "x.zip"]
                 if mode == "export" else ["--archive", "x.zip"])
    with pytest.raises(SystemExit) as result:
        cli.main([mode, *arguments, "--dry-run", "--lock-dir", str(tmp_path)])
    assert result.value.code == 0
    assert captured["dry_run"] is True
    assert captured["lock_dir"] == tmp_path
    assert json.loads(capsys.readouterr().out)["coverage_complete"] is False


@pytest.mark.parametrize("mode,expected", [("export", "rerun export from the beginning"),
                                           ("import", "committed days will be skipped"),
                                           ("inspect", "rerun inspect")])
def test_interrupt_message_matches_active_command(monkeypatch, capsys, mode, expected):
    def interrupt(*args, **kwargs):
        raise KeyboardInterrupt

    monkeypatch.setattr(cli, "create_s3_filesystem", lambda: object())
    monkeypatch.setattr(cli, f"{mode}_bundle", interrupt)
    arguments = (["--year", "2024", "--output", "x.zip"] if mode == "export"
                 else ["--archive", "x.zip"])
    with pytest.raises(SystemExit) as result:
        cli.main([mode, *arguments])
    assert result.value.code == 130
    report = json.loads(capsys.readouterr().out)
    assert report["mode"] == mode
    assert expected in report["error"]
