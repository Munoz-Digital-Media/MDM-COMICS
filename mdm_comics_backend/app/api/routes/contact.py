"""
Contact Form API Route
IMPL-001: Combined About & Contact Page

Public endpoint for contact form submissions with:
- Rate limiting: 5 requests/minute per IP
- Pydantic validation
- PII redaction in logs (per constitution_pii.json)
- Email notification hooks
"""

import logging
import hashlib
from enum import Enum
from datetime import datetime, timezone
from typing import Optional

from fastapi import APIRouter, Request, HTTPException
from pydantic import BaseModel, Field, EmailStr, field_validator
from slowapi import Limiter
from slowapi.util import get_remote_address

from app.core.rate_limit import limiter
from app.services.email_hooks import get_email_service

logger = logging.getLogger(__name__)

router = APIRouter()


class ContactSubject(str, Enum):
    """Valid contact form subjects."""
    general = "general"
    order = "order"
    returns = "returns"
    wholesale = "wholesale"
    other = "other"


class ContactFormRequest(BaseModel):
    """
    Contact form request schema.

    Validation rules per IMPL-001:
    - name: 1-100 characters
    - email: valid email format
    - subject: enum value
    - message: 10-2000 characters
    """
    name: str = Field(
        ...,
        min_length=1,
        max_length=100,
        description="Sender's name",
        examples=["John Doe"]
    )
    email: EmailStr = Field(
        ...,
        description="Sender's email address",
        examples=["john@example.com"]
    )
    subject: ContactSubject = Field(
        ...,
        description="Contact subject category",
        examples=["general"]
    )
    message: str = Field(
        ...,
        min_length=10,
        max_length=2000,
        description="Contact message content",
        examples=["I have a question about my recent order..."]
    )

    @field_validator('name')
    @classmethod
    def sanitize_name(cls, v: str) -> str:
        """Strip whitespace and validate name."""
        return v.strip()

    @field_validator('message')
    @classmethod
    def sanitize_message(cls, v: str) -> str:
        """Strip whitespace from message."""
        return v.strip()


class ContactFormResponse(BaseModel):
    """Response for successful contact form submission."""
    message: str = "Contact form submitted successfully"
    reference_id: str = Field(
        ...,
        description="Reference ID for tracking (not stored, for user reference only)"
    )


def _hash_email(email: str) -> str:
    """
    Hash email for logging (per constitution_pii.json).
    Uses SHA-256 truncated to 8 chars for log correlation.
    """
    return hashlib.sha256(email.lower().encode()).hexdigest()[:8]


def _generate_reference_id() -> str:
    """Generate a simple reference ID for user tracking."""
    import secrets
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d")
    return f"MDM-{timestamp}-{secrets.token_hex(4).upper()}"


@router.post(
    "/contact",
    response_model=ContactFormResponse,
    status_code=202,
    summary="Submit contact form",
    description="""
Submit a contact form inquiry.

**Rate Limit**: 5 requests per minute per IP address.

**Subject Options**:
- `general`: General Inquiry
- `order`: Order Question
- `returns`: Returns & Exchanges
- `wholesale`: Wholesale Inquiry
- `other`: Other

**Response Codes**:
- `202`: Form accepted for processing
- `422`: Validation error (check field requirements)
- `429`: Rate limit exceeded (try again later)
    """,
    responses={
        202: {
            "description": "Contact form submitted successfully",
            "model": ContactFormResponse,
        },
        422: {
            "description": "Validation error",
            "content": {
                "application/json": {
                    "example": {
                        "detail": [
                            {
                                "loc": ["body", "message"],
                                "msg": "String should have at least 10 characters",
                                "type": "string_too_short"
                            }
                        ]
                    }
                }
            }
        },
        429: {
            "description": "Rate limit exceeded",
            "content": {
                "application/json": {
                    "example": {"detail": "Rate limit exceeded: 5 per 1 minute"}
                }
            }
        }
    },
    tags=["Contact"]
)
@limiter.limit("5/minute")
async def submit_contact_form(
    request: Request,
    form: ContactFormRequest
) -> ContactFormResponse:
    """
    Handle contact form submission.

    - Validates input via Pydantic schema
    - Rate limited to 5 requests/minute per IP
    - Logs submission with redacted PII
    - Sends email notification (mock in dev)
    - Returns 202 Accepted (async processing)
    """
    reference_id = _generate_reference_id()
    email_hash = _hash_email(form.email)

    # Log with redacted PII (per constitution_pii.json)
    logger.info(
        f"Contact form submission: ref={reference_id}, "
        f"email_hash={email_hash}, subject={form.subject.value}, "
        f"msg_len={len(form.message)}"
    )

    # Send notification via email service
    email_service = get_email_service()
    try:
        await email_service.send_contact_notification(
            name=form.name,
            email=form.email,
            subject=form.subject.value,
            message=form.message,
            reference_id=reference_id,
        )
    except Exception as e:
        # Log error but don't fail the request - form was received
        logger.error(f"Failed to send contact notification: ref={reference_id}, error={e}")

    return ContactFormResponse(
        message="Contact form submitted successfully",
        reference_id=reference_id
    )
