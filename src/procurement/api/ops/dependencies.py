from functools import lru_cache

from procurement.ops.repositories import ControlRepository, ErrorRepository
from procurement.ops.service import OpsService
from procurement.storage.object_store import create_s3_filesystem


@lru_cache(maxsize=1)
def get_ops_service() -> OpsService:
    fs = create_s3_filesystem()
    return OpsService(ControlRepository(fs), ErrorRepository(fs))
