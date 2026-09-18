"""Host-level scheduler lock released by the OS on process exit; not a JSON lease."""

import hashlib
import os
from contextlib import contextmanager
from pathlib import Path

from procurement.common.settings import settings


@contextmanager
def execution_lock(directory: Path):
    directory.mkdir(parents=True, exist_ok=True)
    namespace = f"{settings.OBJECT_STORAGE_ENDPOINT}/{settings.OBJECT_STORAGE_BUCKET}"
    name = hashlib.sha256(namespace.encode()).hexdigest()[:20]
    with (directory / f"ingestion-{name}.lock").open("a+b") as file:
        file.seek(0, 2)
        if file.tell() == 0:
            file.write(b"0")
            file.flush()
        file.seek(0)
        try:
            if os.name == "nt":
                import msvcrt
                msvcrt.locking(file.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl
                fcntl.flock(file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as exc:
            raise RuntimeError("Another ingestion flow holds this host's storage lock") from exc
        try:
            yield
        finally:
            file.seek(0)
            if os.name == "nt":
                msvcrt.locking(file.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                fcntl.flock(file.fileno(), fcntl.LOCK_UN)
