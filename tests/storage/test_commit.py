from datetime import UTC, date, datetime
from unittest.mock import Mock

import pytest

from procurement.common.resources import ResourceIdentity
from procurement.models.control import DayManifest, DayStatus
from procurement.storage import control

IDENTITY = ResourceIdentity("muasamcong", "project")


def successful_day():
    return DayManifest(
        run_id="run-a",
        source=IDENTITY.source,
        resource=IDENTITY.resource,
        source_date=date(2026, 9, 1),
        status=DayStatus.SUCCESS,
        started_at=datetime(2026, 9, 2, tzinfo=UTC),
        completed_at=datetime(2026, 9, 2, tzinfo=UTC),
        expected_pages=1,
        completed_pages=1,
    )


def test_lost_commit_ack_is_resolved_by_readback(monkeypatch):
    day = successful_day()
    write = Mock(side_effect=TimeoutError("ACK lost"))
    monkeypatch.setattr(control, "write_day_manifest", write)
    monkeypatch.setattr(control, "read_day_manifest", lambda *_: day)
    assert control.commit_day_manifest(object(), IDENTITY, day).endswith("/day.json")
    write.assert_called_once()  # no compensating FAILED write


@pytest.mark.parametrize("persisted", [None, "running", "read-error"])
def test_unconfirmed_commit_is_never_overwritten(monkeypatch, persisted):
    day = successful_day()
    write = Mock(side_effect=TimeoutError("ACK lost"))
    monkeypatch.setattr(control, "write_day_manifest", write)
    read = Mock(
        side_effect=TimeoutError("read failed") if persisted == "read-error" else None,
        return_value=day.model_copy(update={"status": DayStatus.RUNNING})
        if persisted == "running"
        else None,
    )
    monkeypatch.setattr(control, "read_day_manifest", read)
    with pytest.raises(control.DayCommitUncertainError):
        control.commit_day_manifest(object(), IDENTITY, day)
    write.assert_called_once()
