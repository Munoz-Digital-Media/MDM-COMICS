import pytest
from fastapi import HTTPException

from app.routers.transformations import (
    HeaderPropagationRequest,
    propagate_headers,
    _normalize_postal_code,
)


def test_normalize_postal_code_invalid_length():
    with pytest.raises(HTTPException):
        _normalize_postal_code("12")


def test_propagate_headers_defaults_roles_sorted_and_deduped():
    payload = HeaderPropagationRequest(
        user_id="user-1",
        email="user@example.com",
        roles=["admin", "admin", "editor"],
    )
    resp = propagate_headers(payload)
    assert resp.headers["x-mdm-user"] == "user-1"
    assert resp.headers["x-mdm-email"] == "user@example.com"
    assert resp.headers["x-mdm-roles"] == "admin,editor"


def test_propagate_headers_assigns_customer_when_empty():
    payload = HeaderPropagationRequest(
        user_id="user-2",
        email="user2@example.com",
        roles=[],
    )
    resp = propagate_headers(payload)
    assert resp.headers["x-mdm-roles"] == "customer"
