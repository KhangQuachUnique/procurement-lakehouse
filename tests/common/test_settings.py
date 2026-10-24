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
        ("MUASAMCONG_REQUEST_INTERVAL_SECONDS", -1),
        ("MUASAMCONG_REQUEST_INTERVAL_SECONDS", float("nan")),
        ("MUASAMCONG_REQUEST_INTERVAL_SECONDS", float("inf")),
        ("KHLCNT_PACKAGE_WORKERS", 0),
        ("OPS_SYNC_INTERVAL_SECONDS", 0),
        ("OPS_RECONCILE_INTERVAL_SECONDS", 0),
        ("OPS_SYNC_WORKERS", 0),
        ("OPS_STALE_AFTER_SECONDS", 0),
        ("OBJECT_STORAGE_ENDPOINT", "localhost:8333"),
        ("ICEBERG_CATALOG_URI", "localhost:8181"),
        ("ICEBERG_CATALOG_PORT", 0),
        ("ICEBERG_CATALOG_PORT", 70000),
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


def test_settings_iceberg_config():
    settings = Settings(
        ICEBERG_CATALOG_URI="http://localhost:8181",
        ICEBERG_WAREHOUSE="s3://warehouse",
        OBJECT_STORAGE_ENDPOINT="http://localhost:8333",
        OBJECT_STORAGE_ACCESS_KEY="my_ak",
        OBJECT_STORAGE_SECRET_KEY="my_sk",
    )
    cfg = settings.iceberg_config()
    assert cfg.catalog_uri == "http://localhost:8181"
    assert cfg.warehouse == "s3://warehouse"
    assert cfg.s3_endpoint == "http://localhost:8333"
    assert cfg.access_key == "my_ak"
    assert cfg.secret_key == "my_sk"

    # Test overrides
    overridden = settings.iceberg_config(
        catalog_uri="http://custom:18181",
        warehouse="s3://custom",
        s3_endpoint="http://custom:18333",
        access_key="new_ak",
        secret_key="new_sk",
    )
    assert overridden.catalog_uri == "http://custom:18181"
    assert overridden.warehouse == "s3://custom"
    assert overridden.s3_endpoint == "http://custom:18333"
    assert overridden.access_key == "new_ak"
    assert overridden.secret_key == "new_sk"

