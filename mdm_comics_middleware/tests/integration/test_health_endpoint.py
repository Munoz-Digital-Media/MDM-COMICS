from fastapi.testclient import TestClient

from app.main import app


def test_health_endpoint_with_middleware_headers():
    client = TestClient(app)
    resp = client.get("/health")
    assert resp.status_code == 200
    data = resp.json()
    assert data["status"] == "ok"
    assert "mdm_comics_middleware" in data["service"]
    # Request context middleware should have injected headers
    assert "x-mdm-request-id" in resp.headers
    assert "x-mdm-request-duration" in resp.headers
