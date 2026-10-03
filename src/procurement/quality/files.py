"""Atomic local reports and secret-safe CLI diagnostics."""

import json
import time
from datetime import UTC, datetime
from pathlib import Path

from procurement.common.errors import sanitize_error_message
from procurement.common.settings import settings


def now():
    return datetime.now(UTC).isoformat()


def read_json(path):
    try:
        return json.loads(Path(path).read_text(encoding="utf-8-sig"))
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise ValueError(f"Invalid JSON checkpoint {path}: {exc}") from exc


def write_json(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(value, ensure_ascii=False, indent=2), encoding="utf-8")
    for attempt in range(6):
        try:
            temporary.replace(path)
            break
        except PermissionError:
            if attempt == 5:
                raise
            time.sleep(.05 * 2 ** attempt)


def safe_error(exc):
    def scrub(value):
        if isinstance(value, dict):
            return {key: scrub(item) for key, item in value.items()}
        if isinstance(value, list):
            return [scrub(item) for item in value]
        if not isinstance(value, str):
            return value
        value = sanitize_error_message(value)
        for secret in (settings.MUASAMCONG_TOKEN, settings.OBJECT_STORAGE_ACCESS_KEY,
                       settings.OBJECT_STORAGE_SECRET_KEY):
            if secret:
                value = value.replace(secret, "[REDACTED]")
        return value
    message = scrub(str(exc))
    result = {"type": type(exc).__name__, "message": message}
    diagnostics = getattr(exc, "diagnostics", None)
    if diagnostics:
        result["diagnostics"] = scrub(diagnostics)
    return result


def cli(main):
    try:
        return main() or 0
    except KeyboardInterrupt:
        print("Interrupted; completed checkpoints have been retained.")
        return 130
    except Exception as exc:  # noqa: BLE001 -- CLI must not leak token-bearing URLs
        print(json.dumps(safe_error(exc), ensure_ascii=False))
        return 1
