"""Application Service implementing ProcessSilverUseCase (Backward compatibility alias)."""

from procurement.silver.application.process_silver_service import (
    ProcessNoticeService,
    ProcessSilverService,
)

__all__ = ["ProcessNoticeService", "ProcessSilverService"]
