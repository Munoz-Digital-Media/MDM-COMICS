# Services layer for business logic
"""
User Management System v1.0.0 - optional imports for graceful degradation

NOTE: We catch Exception (not just ImportError) because downstream modules
can fail during import due to:
- Missing environment variables (pydantic.ValidationError)
- Database connection issues
- Missing dependencies

This allows the app to start even if some services can't be loaded,
enabling health checks and debugging.
"""
import logging

logger = logging.getLogger(__name__)

try:
    from app.services.audit_service import AuditService, log_audit
except Exception as e:
    logger.warning(f"Could not import AuditService: {type(e).__name__}: {e}")
    AuditService = None
    log_audit = None

try:
    from app.services.session_service import SessionService
except Exception as e:
    logger.warning(f"Could not import SessionService: {type(e).__name__}: {e}")
    SessionService = None

try:
    from app.services.role_service import RoleService
except Exception as e:
    logger.warning(f"Could not import RoleService: {type(e).__name__}: {e}")
    RoleService = None

try:
    from app.services.user_service import UserService
except Exception as e:
    logger.warning(f"Could not import UserService: {type(e).__name__}: {e}")
    UserService = None

try:
    from app.services.dsar_service import DSARService
except Exception as e:
    logger.warning(f"Could not import DSARService: {type(e).__name__}: {e}")
    DSARService = None

try:
    from app.services.retention_service import RetentionService
except Exception as e:
    logger.warning(f"Could not import RetentionService: {type(e).__name__}: {e}")
    RetentionService = None

__all__ = [
    "AuditService",
    "log_audit",
    "SessionService",
    "RoleService",
    "UserService",
    "DSARService",
    "RetentionService",
]
