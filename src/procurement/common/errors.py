from dataclasses import dataclass
from enum import StrEnum

import httpx


class ErrorStage(StrEnum):
    SEARCH_PAGE = "search_page"
    SEARCH_LIMIT = "search_limit"
    PLAN_DETAIL = "plan_detail"
    BID_PACKAGE_DETAIL = "bid_package_detail"
    RAW_STORAGE = "raw_storage"
    BRONZE_LOAD = "bronze_load"


class ErrorCode(StrEnum):
    SOURCE_TIMEOUT = "SOURCE_TIMEOUT"
    SOURCE_CONNECTION_ERROR = "SOURCE_CONNECTION_ERROR"
    SOURCE_RATE_LIMITED = "SOURCE_RATE_LIMITED"
    SOURCE_SERVER_ERROR = "SOURCE_SERVER_ERROR"
    SOURCE_CLIENT_ERROR = "SOURCE_CLIENT_ERROR"
    SOURCE_INVALID_RESPONSE = "SOURCE_INVALID_RESPONSE"
    SEARCH_RESULT_LIMIT_REACHED = "SEARCH_RESULT_LIMIT_REACHED"
    OBJECT_STORAGE_WRITE_FAILED = "OBJECT_STORAGE_WRITE_FAILED"
    BRONZE_LOAD_FAILED = "BRONZE_LOAD_FAILED"
    INTERNAL_ERROR = "INTERNAL_ERROR"


@dataclass(frozen=True)
class ErrorClassification:
    code: ErrorCode
    retryable: bool
    http_status: int | None = None


def classify_exception(exc: Exception) -> ErrorClassification:
    if isinstance(exc, httpx.TimeoutException):
        return ErrorClassification(ErrorCode.SOURCE_TIMEOUT, True)
    if isinstance(exc, httpx.ConnectError):
        return ErrorClassification(ErrorCode.SOURCE_CONNECTION_ERROR, True)
    if isinstance(exc, httpx.HTTPStatusError):
        status = exc.response.status_code
        if status == 429:
            return ErrorClassification(ErrorCode.SOURCE_RATE_LIMITED, True, status)
        if status >= 500:
            return ErrorClassification(ErrorCode.SOURCE_SERVER_ERROR, True, status)
        return ErrorClassification(ErrorCode.SOURCE_CLIENT_ERROR, False, status)
    if isinstance(exc, (KeyError, TypeError, ValueError)):
        return ErrorClassification(ErrorCode.SOURCE_INVALID_RESPONSE, False)
    return ErrorClassification(ErrorCode.INTERNAL_ERROR, False)


def safe_error_message(exc: Exception, classification: ErrorClassification) -> str:
    """Return a useful message without request URLs, query strings, or tokens."""

    messages = {
        ErrorCode.SOURCE_TIMEOUT: "Source request timed out",
        ErrorCode.SOURCE_CONNECTION_ERROR: "Could not connect to source",
        ErrorCode.SOURCE_RATE_LIMITED: "Source rate limit reached",
        ErrorCode.SOURCE_SERVER_ERROR: "Source server returned an error",
        ErrorCode.SOURCE_CLIENT_ERROR: "Source rejected the request",
        ErrorCode.SOURCE_INVALID_RESPONSE: "Source response has an invalid structure",
        ErrorCode.SEARCH_RESULT_LIMIT_REACHED: "Daily search result limit reached",
        ErrorCode.OBJECT_STORAGE_WRITE_FAILED: "Object storage write failed",
        ErrorCode.BRONZE_LOAD_FAILED: "Bronze load failed",
        ErrorCode.INTERNAL_ERROR: f"Unexpected {type(exc).__name__}",
    }
    message = messages[classification.code]
    if classification.http_status is not None:
        message = f"{message} (HTTP {classification.http_status})"
    return message
