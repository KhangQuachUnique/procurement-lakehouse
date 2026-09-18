import os

from dotenv import load_dotenv
from pydantic import BaseModel, ConfigDict, Field, field_validator


class Settings(BaseModel):
    """Validated configuration. Load env explicitly when constructing the application."""

    model_config = ConfigDict(extra="forbid", hide_input_in_errors=True)

    APP_ENV: str = "dev"
    LOG_LEVEL: str = "INFO"
    MUASAMCONG_BASE_URL: str = "https://muasamcong.mpi.gov.vn"
    MUASAMCONG_TIMEOUT_SECONDS: float = Field(default=30, gt=0)
    MUASAMCONG_MAX_ATTEMPTS: int = Field(default=3, ge=1, le=10)
    MUASAMCONG_MAX_RETRY_DELAY_SECONDS: float = Field(default=30, ge=0, le=300)
    MUASAMCONG_MAX_INFLIGHT: int = Field(default=3, ge=1, le=32)
    KHLCNT_PACKAGE_WORKERS: int = Field(default=3, ge=1, le=32)
    INGESTION_RESOURCE_WORKERS: int = Field(default=2, ge=1, le=4)
    MUASAMCONG_TOKEN: str | None = Field(default=None, repr=False)
    OBJECT_STORAGE_ENDPOINT: str = "http://localhost:8333"
    OBJECT_STORAGE_ACCESS_KEY: str | None = Field(default=None, repr=False)
    OBJECT_STORAGE_SECRET_KEY: str | None = Field(default=None, repr=False)
    OBJECT_STORAGE_BUCKET: str = "procurement-lakehouse"
    DLT_PIPELINES_DIR: str | None = None
    INGESTION_LOCK_DIR: str = "data/locks"
    OPS_INDEX_PATH: str = "data/ops/index.sqlite3"
    OPS_SYNC_INTERVAL_SECONDS: float = Field(default=5, ge=1, le=3600)
    OPS_RECONCILE_INTERVAL_SECONDS: float = Field(default=300, ge=5, le=86400)
    OPS_SYNC_WORKERS: int = Field(default=8, ge=1, le=32)
    OPS_STALE_AFTER_SECONDS: float = Field(default=600, ge=30)

    @field_validator("MUASAMCONG_BASE_URL", "OBJECT_STORAGE_ENDPOINT")
    @classmethod
    def http_endpoint(cls, value: str) -> str:
        from urllib.parse import urlsplit

        parsed = urlsplit(value)
        if parsed.scheme not in {"http", "https"} or not parsed.hostname:
            raise ValueError("Endpoint must be an absolute HTTP(S) URL")
        return value

    @field_validator("LOG_LEVEL")
    @classmethod
    def log_level(cls, value: str) -> str:
        value = value.upper()
        if value not in {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}:
            raise ValueError("Unsupported log level")
        return value

    @classmethod
    def from_environment(cls, *, dotenv: bool = True) -> "Settings":
        if dotenv:
            load_dotenv()
        return cls.model_validate(
            {key: os.environ[key] for key in cls.model_fields if key in os.environ}
        )


# Compatibility for existing modules; tests and new entry points may construct Settings directly.
settings = Settings.from_environment()
