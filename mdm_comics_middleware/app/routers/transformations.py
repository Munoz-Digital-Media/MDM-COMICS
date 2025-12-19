from typing import List, Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, EmailStr, Field

router = APIRouter(prefix="/middleware", tags=["middleware-service"])


class NormalizeAddressRequest(BaseModel):
    line1: str
    line2: Optional[str] = None
    city: str
    state: str
    postal_code: str
    country: str = "US"


class NormalizeAddressResponse(BaseModel):
    address_lines: List[str]
    city: str
    state: str
    postal_code: str
    country: str


class HeaderPropagationRequest(BaseModel):
    user_id: str
    email: EmailStr
    roles: List[str] = Field(default_factory=list)


class HeaderPropagationResponse(BaseModel):
    headers: dict


def _normalize_postal_code(value: str) -> str:
    digits = "".join(ch for ch in value if ch.isdigit())
    if len(digits) not in (5, 9):
        raise HTTPException(status_code=400, detail="Invalid postal code")
    if len(digits) == 9:
        return f"{digits[:5]}-{digits[5:]}"
    return digits


@router.post("/normalize-address", response_model=NormalizeAddressResponse)
def normalize_address(payload: NormalizeAddressRequest) -> NormalizeAddressResponse:
    lines = [payload.line1.strip().upper()]
    if payload.line2:
        lines.append(payload.line2.strip().upper())

    return NormalizeAddressResponse(
        address_lines=lines,
        city=payload.city.strip().title(),
        state=payload.state.strip().upper(),
        postal_code=_normalize_postal_code(payload.postal_code),
        country=payload.country.strip().upper(),
    )


@router.post("/propagate-headers", response_model=HeaderPropagationResponse)
def propagate_headers(payload: HeaderPropagationRequest) -> HeaderPropagationResponse:
    headers = {
        "x-mdm-user": payload.user_id,
        "x-mdm-email": payload.email,
        "x-mdm-roles": ",".join(sorted(set(payload.roles))) or "customer",
    }
    return HeaderPropagationResponse(headers=headers)
