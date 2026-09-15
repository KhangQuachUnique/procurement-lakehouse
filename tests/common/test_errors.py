import httpx

from procurement.common.errors import extract_http_status, sanitize_error_message


def test_error_message_keeps_original_context_but_redacts_token() -> None:
    message = "failed for https://example.test/path?token=secret&foo=bar"

    sanitized = sanitize_error_message(message)

    assert "failed for https://example.test/path" in sanitized
    assert "foo=bar" in sanitized
    assert "secret" not in sanitized
    assert "token=[REDACTED]" in sanitized


def test_http_status_is_extracted_without_error_taxonomy() -> None:
    request = httpx.Request("GET", "https://example.test/path")
    response = httpx.Response(503, request=request)
    error = httpx.HTTPStatusError("upstream failed", request=request, response=response)

    assert extract_http_status(error) == 503
    assert extract_http_status(httpx.ReadTimeout("timeout")) is None
