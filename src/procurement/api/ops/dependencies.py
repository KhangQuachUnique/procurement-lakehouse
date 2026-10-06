import os
from functools import lru_cache
from typing import Annotated

from fastapi import Depends, Request

from procurement.common.settings import settings
from procurement.infrastructure.database import create_application_engine
from procurement.ops.index import OpsIndex
from procurement.ops.repositories.indexed import (
    IndexedControlRepository,
    IndexedErrorRepository,
)
from procurement.ops.repositories.postgres import (
    PostgresOpsControlRepository,
    PostgresOpsErrorRepository,
)
from procurement.ops.service import OpsService
from procurement.ops.sync import IndexSynchronizer
from procurement.storage.object_store import create_s3_filesystem


@lru_cache(maxsize=1)
def get_ops_runtime() -> IndexSynchronizer | None:
    if os.environ.get("APP_DATABASE_URL"):
        return None
    fs = create_s3_filesystem()
    index = OpsIndex(
        settings.OPS_INDEX_PATH,
        namespace=f"{settings.OBJECT_STORAGE_ENDPOINT}/{settings.OBJECT_STORAGE_BUCKET}",
    )
    return IndexSynchronizer(
        index,
        fs,
        bucket=settings.OBJECT_STORAGE_BUCKET,
        interval=settings.OPS_SYNC_INTERVAL_SECONDS,
        reconcile_interval=settings.OPS_RECONCILE_INTERVAL_SECONDS,
        workers=settings.OPS_SYNC_WORKERS,
    )


def get_ops_service(
    request: Request,
    runtime: Annotated[IndexSynchronizer | None, Depends(get_ops_runtime)] = None,
):
    app_db_url = os.environ.get("APP_DATABASE_URL")
    if app_db_url:
        engine = create_application_engine(app_db_url)
        yield OpsService(
            PostgresOpsControlRepository(engine),
            PostgresOpsErrorRepository(engine),
            sync_status=None,
            stale_after_seconds=settings.OPS_STALE_AFTER_SECONDS,
        )
    elif runtime is not None:
        with runtime.index.snapshot() as (db, status):
            request.state.ops_sync = status
            yield OpsService(
                IndexedControlRepository(db, runtime.fs),
                IndexedErrorRepository(db),
                sync_status=status,
                stale_after_seconds=settings.OPS_STALE_AFTER_SECONDS,
            )
    else:
        raise RuntimeError("Neither APP_DATABASE_URL nor IndexSynchronizer is available")
