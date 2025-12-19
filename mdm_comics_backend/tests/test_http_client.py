import httpx
import pytest

from app.core.http_client import (
    ResilientHTTPClient,
    RateLimitConfig,
    RetryConfig,
    RateLimitExceeded,
)


@pytest.mark.asyncio
async def test_request_retries_after_retry_after_header():
    """
    Ensure 429 with Retry-After sets blocked_until, then recovers on retry.
    """
    call_count = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            # Immediate retry allowed; no real sleep expected
            return httpx.Response(429, headers={"Retry-After": "0"})
        return httpx.Response(200, json={"ok": True})

    client = ResilientHTTPClient(
        rate_limit_config=RateLimitConfig(
            requests_per_second=1000,
            burst_limit=1000,
            min_request_interval=0,
        ),
        retry_config=RetryConfig(
            max_retries=1,
            base_delay=0,
            max_delay=0,
            jitter_factor=0,
        ),
        timeout=5.0,
    )
    transport = httpx.MockTransport(handler)
    client._client = httpx.AsyncClient(
        transport=transport,
        timeout=client.timeout,
        headers=client.default_headers,
    )

    resp = await client.get("https://example.com/test")
    await client.close()

    assert resp.status_code == 200
    assert call_count == 2
    state = client._get_host_state("example.com")
    assert state.blocked_until is None
    assert state.failure_count == 0


@pytest.mark.asyncio
async def test_rate_limit_exceeded_raises_fast(monkeypatch):
    """
    Very long Retry-After should raise RateLimitExceeded without blocking for minutes.
    """

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(429, headers={"Retry-After": "120"})

    client = ResilientHTTPClient(
        rate_limit_config=RateLimitConfig(
            requests_per_second=1000,
            burst_limit=1000,
            min_request_interval=0,
        ),
        retry_config=RetryConfig(
            max_retries=0,
            base_delay=0,
            max_delay=0,
            jitter_factor=0,
        ),
        timeout=5.0,
    )
    transport = httpx.MockTransport(handler)
    client._client = httpx.AsyncClient(
        transport=transport,
        timeout=client.timeout,
        headers=client.default_headers,
    )

    with pytest.raises(RateLimitExceeded):
        await client.get("https://example.com/test")

    await client.close()
    state = client._get_host_state("example.com")
    assert state.blocked_until is not None
