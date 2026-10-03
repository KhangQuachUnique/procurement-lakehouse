from datetime import UTC, datetime, timedelta
from email.utils import format_datetime

import httpx
import pytest

from procurement.ingestion.sources.muasamcong.client import MuasamcongClient, SourceResponseError


def test_retry_after_is_respected_without_real_sleep():
    calls = []
    delays = []

    def handler(request):
        calls.append(request)
        if len(calls) == 1:
            return httpx.Response(429, headers={"Retry-After": "5"})
        return httpx.Response(200, json={"page": {"content": []}})

    with MuasamcongClient(
        token="test", transport=httpx.MockTransport(handler), sleep=delays.append
    ) as client:
        assert client.post("/search", {}) == {"page": {"content": []}}
    assert len(calls) == 2
    assert delays == [5]


def test_excessive_retry_after_fails_without_retrying_early():
    calls = []
    delays = []

    def handler(request):
        calls.append(request)
        return httpx.Response(429, headers={"Retry-After": "600"})

    with (
        MuasamcongClient(
            token="test", transport=httpx.MockTransport(handler), sleep=delays.append
        ) as client,
        pytest.raises(httpx.HTTPStatusError),
    ):
        client.post("/search", {})
    assert len(calls) == 1
    assert delays == []


@pytest.mark.parametrize("status", [401, 403])
def test_rejected_credential_opens_circuit_for_remaining_calls(status):
    calls = []

    def handler(request):
        calls.append(request)
        return httpx.Response(status)

    with MuasamcongClient(token="test", transport=httpx.MockTransport(handler)) as client:
        for _ in range(3):
            with pytest.raises(httpx.HTTPStatusError):
                client.post("/detail", {})
    assert len(calls) == 1


def test_transport_retry_is_bounded():
    calls = []
    delays = []

    def handler(request):
        calls.append(request)
        raise httpx.ReadTimeout("slow", request=request)

    with (
        MuasamcongClient(
            token="test", transport=httpx.MockTransport(handler), sleep=delays.append
        ) as client,
        pytest.raises(httpx.ReadTimeout),
    ):
        client.post("/detail", {})
    assert len(calls) == 3
    assert len(delays) == 2
    assert all(0 <= delay <= 2 for delay in delays)


def test_json_array_does_not_satisfy_response_contract():
    with (
        MuasamcongClient(
            token="test", transport=httpx.MockTransport(lambda _: httpx.Response(200, json=[]))
        ) as client,
        pytest.raises(TypeError, match="JSON object"),
    ):
        client.post("/search", {})


def test_http_date_retry_after_and_invalid_header():
    with MuasamcongClient(
        token="test", transport=httpx.MockTransport(lambda _: httpx.Response(200, json={}))
    ) as client:
        delay = client._retry_delay(1, format_datetime(datetime.now(UTC) + timedelta(seconds=10)))
        assert 8 <= delay <= 10
        assert 0 <= client._retry_delay(1, "invalid") <= 1


@pytest.mark.parametrize("body", [b"", b"<html>upstream unavailable</html>", b'{"truncated":'])
def test_invalid_json_success_is_retried_within_request_budget(body):
    calls, delays = [], []
    def handler(request):
        calls.append(request)
        return httpx.Response(200, content=body) if len(calls) == 1 else httpx.Response(200, json={"id": "ok"})
    with MuasamcongClient(token="test", transport=httpx.MockTransport(handler), sleep=delays.append) as client:
        assert client.post("/detail", {"id": "notice"}) == {"id": "ok"}
    assert len(calls) == 2
    assert len(delays) == 1


def test_invalid_json_exhaustion_has_safe_actionable_diagnostics():
    calls = []
    def handler(request):
        calls.append(request)
        return httpx.Response(200, content=b"private body", headers={"content-type": "text/html"})
    with (
        MuasamcongClient(token="secret-token", transport=httpx.MockTransport(handler), sleep=lambda _: None) as client,
        pytest.raises(SourceResponseError) as caught,
    ):
        client.post("/detail", {"id": "notice"})
    assert len(calls) == 3
    assert caught.value.diagnostics["request_id"] == "notice"
    assert caught.value.diagnostics["content_type"] == "text/html"
    assert caught.value.diagnostics["response_bytes"] == 12
    assert "private body" not in str(caught.value)
    assert "secret-token" not in str(caught.value)
