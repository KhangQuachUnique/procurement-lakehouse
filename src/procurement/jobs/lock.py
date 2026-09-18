"""Host-level scheduler lock released by the OS on process exit."""

import hashlib
from contextlib import contextmanager
from pathlib import Path

from procurement.common.file_lock import LockBusy, exclusive_file_lock
from procurement.common.settings import settings


@contextmanager
def execution_lock(directory: Path):
    namespace = f"{settings.OBJECT_STORAGE_ENDPOINT}/{settings.OBJECT_STORAGE_BUCKET}"
    name = hashlib.sha256(namespace.encode()).hexdigest()[:20]
    try:
        with exclusive_file_lock(directory / f"ingestion-{name}.lock"):
            yield
    except LockBusy as exc:
        raise RuntimeError("Another ingestion flow holds this host's storage lock") from exc
