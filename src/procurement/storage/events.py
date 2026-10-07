"""Best-effort invalidations; manifests remain authoritative and fully rebuildable."""

import json
import logging
from datetime import UTC, datetime
from uuid import uuid4

logger = logging.getLogger(__name__)


def notify_changed(fs, key):
    area = next((area for area in ("_control", "_ops", "_errors")
                 if f"/{area}/" in key), None)
    if area is None or (area == "_control" and not key.endswith(("/day.json", "/run.json"))):
        return
    bucket = key.split(f"/{area}/", 1)[0]
    now = datetime.now(UTC)
    event_key = f"{bucket}/_events/{now:%Y-%m-%d/%H}/{uuid4().hex}.json"
    try:
        fs.pipe_file(event_key, json.dumps({"key": key, "at": now.isoformat()}).encode())
    except Exception:
        # A notification failure must never turn an acknowledged commit into a failed attempt.
        logger.warning("ops_notification_failed; reconciliation will recover", exc_info=True)
