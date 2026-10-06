import time

from procurement.common.settings import settings
from procurement.storage.object_store import create_s3_filesystem

fs = create_s3_filesystem()
for attempt in range(60):
    try:
        if fs.exists(settings.OBJECT_STORAGE_BUCKET):
            print("Sandbox bucket ready", flush=True)
            break
    except OSError:
        pass
    time.sleep(2)
else:
    raise RuntimeError("Sandbox bucket was not ready after 120 seconds")
