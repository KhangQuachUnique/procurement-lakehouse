from enum import StrEnum


class DetailKind(StrEnum):
    STANDARD = "standard"
    REOFFER = "reoffer"


class UnsupportedNotifyWorkflowError(ValueError):
    pass


def resolve_detail_kind(step_code: str | None) -> DetailKind:
    if not step_code:
        raise UnsupportedNotifyWorkflowError("Missing stepCode")

    if step_code.startswith("notify-contractor-"):
        return DetailKind.STANDARD

    if step_code.startswith("reoffer-price-"):
        return DetailKind.REOFFER

    raise UnsupportedNotifyWorkflowError(f"Unsupported stepCode: {step_code}")
