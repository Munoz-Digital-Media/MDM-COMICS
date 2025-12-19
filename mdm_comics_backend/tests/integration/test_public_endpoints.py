import pytest
from httpx import ASGITransport, AsyncClient

from app.main import app


@pytest.mark.anyio
async def test_root_endpoint_basic_response():
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/")
    assert resp.status_code == 200
    body = resp.json()
    assert body.get("message") == "MDM Comics API"
    assert body.get("status") == "operational"


@pytest.mark.anyio
async def test_public_config_endpoint():
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/api/config")
    assert resp.status_code == 200
    body = resp.json()
    # under_construction flag should always be present for the frontend
    assert "under_construction" in body
