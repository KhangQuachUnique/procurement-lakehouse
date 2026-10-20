"""Inbound Port for Silver Processing."""

from collections.abc import Callable
from datetime import date
from typing import Any, Protocol


class ProcessSilverUseCase(Protocol):
    """Driving use case interface for processing Bronze into Silver candidates and Iceberg tables."""

    def execute(
        self,
        start_date: date,
        end_date: date,
        *,
        family: str | None = None,
        continue_on_error: bool = False,
        skip_existing: bool = False,
        on_progress: Callable[[dict[str, Any]], None] | None = None,
    ) -> dict[str, Any]:
        """Execute Silver candidate generation across date range."""
        ...


# Backward compatibility alias
ProcessNoticeUseCase = ProcessSilverUseCase

__all__ = ["ProcessNoticeUseCase", "ProcessSilverUseCase"]
