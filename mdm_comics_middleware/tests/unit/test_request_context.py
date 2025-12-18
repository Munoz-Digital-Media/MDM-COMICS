import pytest
from starlette.requests import Request
from starlette.responses import Response

from app.middleware.request_context import RequestContextMiddleware


@pytest.mark.asyncio
async def test_request_context_middleware_adds_headers_and_state():
    middleware = RequestContextMiddleware(app=None)

    async def _call_next(request: Request) -> Response:
        assert hasattr(request.state, "request_id")
        assert hasattr(request.state, "started_at")
        return Response("ok")

    scope = {"type": "http", "method": "GET", "path": "/", "headers": []}
    request = Request(scope)

    response = await middleware.dispatch(request, _call_next)

    assert response.status_code == 200
    assert "x-mdm-request-id" in response.headers
    assert "x-mdm-request-duration" in response.headers
