"""
Audit logging for admin actions

P2-6: Track all administrative actions for security and compliance
- Records who did what and when
- Logs to both structured logger and database (optional)
- Provides decorator for easy use on admin endpoints
"""
import logging
import functools
from datetime import datetime, timezone
from typing import Optional, Any, Callable
from fastapi import Request

from app.core.config import settings

# Structured audit logger
audit_logger = logging.getLogger("audit")
audit_logger.setLevel(logging.INFO)

# Action categories
ACTION_PRODUCT_CREATE = "product.create"
ACTION_PRODUCT_UPDATE = "product.update"
ACTION_PRODUCT_DELETE = "product.delete"
ACTION_USER_PROMOTE = "user.promote_admin"
ACTION_USER_DEACTIVATE = "user.deactivate"
ACTION_ORDER_UPDATE = "order.update"
ACTION_ORDER_REFUND = "order.refund"
ACTION_CONFIG_UPDATE = "config.update"


def log_admin_action(
    action: str,
    user_id: int,
    user_email: str,
    resource_type: str,
    resource_id: Optional[Any] = None,
    details: Optional[dict] = None,
    ip_address: Optional[str] = None,
    success: bool = True,
):
    """
    Log an administrative action.

    Args:
        action: Action identifier (e.g., "product.create")
        user_id: ID of the admin performing the action
        user_email: Email of the admin
        resource_type: Type of resource affected (e.g., "product", "user")
        resource_id: ID of the affected resource (if applicable)
        details: Additional context about the action
        ip_address: IP address of the request
        success: Whether the action succeeded
    """
    log_entry = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "action": action,
        "admin_id": user_id,
        "admin_email": user_email,
        "resource_type": resource_type,
        "resource_id": str(resource_id) if resource_id else None,
        "success": success,
        "ip_address": ip_address,
        "environment": settings.ENVIRONMENT,
    }

    if details:
        # Filter out sensitive fields from details
        safe_details = {
            k: v for k, v in details.items()
            if k.lower() not in ("password", "secret", "token", "key", "credential")
        }
        log_entry["details"] = safe_details

    # Log as structured JSON for easy parsing
    if success:
        audit_logger.info(
            f"AUDIT: {action} by {user_email} on {resource_type}/{resource_id}",
            extra={"audit": log_entry}
        )
    else:
        audit_logger.warning(
            f"AUDIT FAILED: {action} by {user_email} on {resource_type}/{resource_id}",
            extra={"audit": log_entry}
        )


def audit_action(action: str, resource_type: str):
    """
    Decorator to automatically log admin actions.

    Usage:
        @audit_action("product.create", "product")
        async def create_product(request: Request, current_user: User, ...):
            ...

    The decorated function must have:
    - A 'request' parameter (FastAPI Request)
    - A 'current_user' parameter (User model with id and email)

    The function's return value is used as the resource_id.
    """
    def decorator(func: Callable):
        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            # Extract request and user from kwargs
            request: Request = kwargs.get("request")
            current_user = kwargs.get("current_user")

            ip_address = None
            user_id = None
            user_email = "unknown"

            if request and hasattr(request, "client") and request.client:
                ip_address = request.client.host

            if current_user:
                user_id = getattr(current_user, "id", None)
                user_email = getattr(current_user, "email", "unknown")

            try:
                result = await func(*args, **kwargs)

                # Try to extract resource_id from result
                resource_id = None
                if result is not None:
                    if hasattr(result, "id"):
                        resource_id = result.id
                    elif isinstance(result, dict) and "id" in result:
                        resource_id = result["id"]

                log_admin_action(
                    action=action,
                    user_id=user_id,
                    user_email=user_email,
                    resource_type=resource_type,
                    resource_id=resource_id,
                    ip_address=ip_address,
                    success=True,
                )

                return result
            except Exception as e:
                log_admin_action(
                    action=action,
                    user_id=user_id,
                    user_email=user_email,
                    resource_type=resource_type,
                    details={"error": str(e)[:200]},
                    ip_address=ip_address,
                    success=False,
                )
                raise

        return wrapper
    return decorator
