"""Inbound Port for Silver Notice Processing (Backward compatibility alias)."""

from procurement.silver.ports.inbound.process_silver import (
    ProcessNoticeUseCase,
    ProcessSilverUseCase,
)

__all__ = ["ProcessNoticeUseCase", "ProcessSilverUseCase"]
