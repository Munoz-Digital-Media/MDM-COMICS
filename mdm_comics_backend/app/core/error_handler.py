"""
Error handling and sanitization middleware

P2-4: Sanitize error messages to prevent internal information leakage
- Database errors → generic message
- Stack traces → logged only, not returned to client
- Validation errors → kept as-is (safe to expose)
"""
import logging
import traceback
from typing import Union

from fastapi import Request, HTTPException
from fastapi.responses import JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware

from app.core.config import settings

logger = logging.getLogger(__name__)

# Patterns that indicate internal/sensitive error information
SENSITIVE_PATTERNS = [
    "password",
    "secret",
    "token",
    "key",
    "credential",
    "sqlalchemy",
    "asyncpg",
    "psycopg",
    "postgresql",
    "mysql",
    "sqlite",
    "traceback",
    "file \"",
    "line ",
    "/app/",
    "\\app\\",
]


def is_sensitive_error(message: str) -> bool:
    """Check if error message contains sensitive information."""
    message_lower = message.lower()
    return any(pattern in message_lower for pattern in SENSITIVE_PATTERNS)


def sanitize_error_message(error: Union[str, Exception], include_type: bool = False) -> str:
    """
    Sanitize an error message for safe client exposure.

    Args:
        error: The error string or exception
        include_type: Whether to include the exception type in production

    Returns:
        Sanitized error message safe for client
    """
    if isinstance(error, str):
        message = error
    else:
        message = str(error)

    # In debug mode, return full message
    if settings.DEBUG:
        return message

    # Check for sensitive patterns
    if is_sensitive_error(message):
        return "An internal error occurred. Please try again later."

    # Truncate very long messages
    if len(message) > 200:
        return message[:200] + "..."

    return message


class ErrorSanitizationMiddleware(BaseHTTPMiddleware):
    """
    P2-4: Middleware to catch unhandled exceptions and sanitize error responses.

    - In production: Returns generic error, logs full details
    - In development: Returns full error for debugging
    """

    async def dispatch(self, request: Request, call_next):
        try:
            response = await call_next(request)
            return response
        except HTTPException:
            # Let FastAPI handle HTTPExceptions normally
            raise
        except Exception as e:
            # Log full error with traceback
            error_id = f"{request.client.host if request.client else 'unknown'}-{id(e)}"
            logger.error(
                f"Unhandled exception [{error_id}]: {type(e).__name__}: {str(e)}\n"
                f"Path: {request.url.path}\n"
                f"Method: {request.method}\n"
                f"Traceback:\n{traceback.format_exc()}"
            )

            # Return sanitized response
            if settings.DEBUG:
                return JSONResponse(
                    status_code=500,
                    content={
                        "error": "internal_error",
                        "message": str(e),
                        "type": type(e).__name__,
                        "error_id": error_id,
                    }
                )
            else:
                return JSONResponse(
                    status_code=500,
                    content={
                        "error": "internal_error",
                        "message": "An unexpected error occurred. Please try again later.",
                        "error_id": error_id,
                    }
                )
