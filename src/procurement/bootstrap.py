"""Application bootstrap: wires infrastructure, metadata, sources, and ingestion services."""

from collections.abc import Callable

from procurement.common.settings import settings
from procurement.infrastructure.database import create_application_engine
from procurement.ingestion.engine.models import ResourceSpec
from procurement.ingestion.service import IngestionService
from procurement.ingestion.sources.muasamcong.client import MuasamcongClient
from procurement.ingestion.sources.muasamcong.project.resource import create_project_spec
from procurement.metadata.service import PostgresMetadataService


def create_spec_factory(client: MuasamcongClient) -> Callable[[str], ResourceSpec]:
    def factory(resource_name: str) -> ResourceSpec:
        if resource_name == "project":
            return create_project_spec(client)
        raise ValueError(f"Unsupported resource for refactored core: {resource_name}")

    return factory


def bootstrap_services(
    *,
    db_url: str | None = None,
    bucket: str | None = None,
    access_key: str | None = None,
    secret_key: str | None = None,
    endpoint_url: str | None = None,
    client: MuasamcongClient | None = None,
) -> tuple[IngestionService, PostgresMetadataService]:
    """Bootstrap and connect the production/sandbox metadata and ingestion services."""
    engine = create_application_engine(db_url)
    metadata_service = PostgresMetadataService(engine)

    b = bucket or settings.OBJECT_STORAGE_BUCKET
    ak = access_key or settings.OBJECT_STORAGE_ACCESS_KEY or ""
    sk = secret_key or settings.OBJECT_STORAGE_SECRET_KEY or ""
    ep = endpoint_url if endpoint_url is not None else settings.OBJECT_STORAGE_ENDPOINT

    if client is None:
        token = settings.MUASAMCONG_TOKEN or "placeholder-token"
        client = MuasamcongClient(
            token=token,
            max_attempts=settings.MUASAMCONG_MAX_ATTEMPTS,
            max_retry_delay=settings.MUASAMCONG_MAX_RETRY_DELAY_SECONDS,
        )

    spec_factory = create_spec_factory(client)

    ingestion_service = IngestionService(
        metadata=metadata_service,
        spec_factory=spec_factory,
        bucket=b,
        access_key=ak,
        secret_key=sk,
        endpoint_url=ep,
        pipelines_dir=settings.DLT_PIPELINES_DIR,
    )

    return ingestion_service, metadata_service
