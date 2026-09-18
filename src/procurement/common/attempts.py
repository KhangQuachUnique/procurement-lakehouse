from collections.abc import Iterable

from procurement.models.control import DayManifest, DayStatus


def effective_attempt(attempts: Iterable[DayManifest]) -> DayManifest | None:
    """Preserve the existing Ops policy: successful attempt with latest started_at."""
    successful = [item for item in attempts if item.status is DayStatus.SUCCESS]
    return max(successful, key=lambda item: item.started_at, default=None)
