"""Inbound Port for Silver Notice Processing."""

from collections.abc import Callable
from datetime import date
from typing import Any, Protocol


class ProcessNoticeUseCase(Protocol):
    """Driving use case interface for processing notice Bronze into Silver candidates."""

    def execute(
        self,
        start_date: date,
        end_date: date,
        *,
        family: str | None = None,
        continue_on_error: bool = False,
        on_progress: Callable[[dict[str, Any]], None] | None = None,
    ) -> dict[str, Any]:
        """Execute Silver notice candidate generation across date range."""
        ...
