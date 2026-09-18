import pytest

from procurement.common.resources import ResourceIdentity
from procurement.storage import execution


def test_heartbeat_storage_failure_does_not_fail_business_work(monkeypatch):
    def unavailable(*_):
        raise OSError('sidecar storage unavailable')

    monkeypatch.setattr(execution, 'write_json', unavailable)
    with execution.ExecutionHeartbeat(object(), ResourceIdentity('source', 'resource'), 'run'):
        completed = True
    assert completed


def test_interruption_publishes_sidecar_without_swallowing_error(monkeypatch):
    writes = []
    monkeypatch.setattr(execution, 'write_json', lambda _fs, _key, value: writes.append(value))
    with pytest.raises(KeyboardInterrupt), execution.ExecutionHeartbeat(
        object(), ResourceIdentity('source', 'resource'), 'run'
    ):
        raise KeyboardInterrupt
    assert writes[-1]['state'] == 'interrupted'
