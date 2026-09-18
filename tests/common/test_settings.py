import pytest
from pydantic import ValidationError

from procurement.common.settings import Settings


def test_settings_validate_environment_without_dotenv(monkeypatch):
    monkeypatch.setenv("MUASAMCONG_TIMEOUT_SECONDS", "12.5")
    monkeypatch.setenv("MUASAMCONG_MAX_ATTEMPTS", "4")
    settings = Settings.from_environment(dotenv=False)
    assert settings.MUASAMCONG_TIMEOUT_SECONDS == 12.5
    assert settings.MUASAMCONG_MAX_ATTEMPTS == 4


@pytest.mark.parametrize(
    "field,value",
    [
        ("MUASAMCONG_TIMEOUT_SECONDS", 0),
        ("MUASAMCONG_MAX_ATTEMPTS", 0),
        ("MUASAMCONG_MAX_INFLIGHT", 0),
        ("MUASAMCONG_MAX_INFLIGHT", 33),
        ("KHLCNT_PACKAGE_WORKERS", 0),
        ("INGESTION_RESOURCE_WORKERS", 5),
        ("OPS_SYNC_INTERVAL_SECONDS", 0),
        ("OPS_RECONCILE_INTERVAL_SECONDS", 0),
        ("OPS_SYNC_WORKERS", 0),
        ("OPS_STALE_AFTER_SECONDS", 0),
        ("OBJECT_STORAGE_ENDPOINT", "localhost:8333"),
        ("LOG_LEVEL", "bad"),
    ],
)
def test_settings_reject_invalid_config(field, value):
    with pytest.raises(ValidationError):
        Settings(**{field: value})


def test_settings_repr_excludes_secrets():
    settings = Settings(MUASAMCONG_TOKEN="token-value", OBJECT_STORAGE_SECRET_KEY="secret-value")
    assert "token-value" not in repr(settings)
    assert "secret-value" not in repr(settings)
