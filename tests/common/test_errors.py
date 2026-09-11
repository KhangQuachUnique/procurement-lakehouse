import httpx

from procurement.common.errors import ErrorCode, classify_exception, safe_error_message


def test_http_error_is_classified_without_exposing_request_url() -> None:
    request = httpx.Request("GET", "https://example.test/path?token=secret")
    response = httpx.Response(503, request=request)
    error = httpx.HTTPStatusError("failed", request=request, response=response)

    classification = classify_exception(error)
    message = safe_error_message(error, classification)

    assert classification.code is ErrorCode.SOURCE_SERVER_ERROR
    assert classification.retryable is True
    assert classification.http_status == 503
    assert "secret" not in message
    assert "HTTP 503" in message
