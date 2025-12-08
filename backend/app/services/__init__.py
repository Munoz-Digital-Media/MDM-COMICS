# Services layer for business logic

# User Management System v1.0.0 - optional imports for graceful degradation
try:
    from app.services.audit_service import AuditService, log_audit
except ImportError as e:
    print(f"Warning: Could not import AuditService: {e}")
    AuditService = None
    log_audit = None

try:
    from app.services.session_service import SessionService
except ImportError as e:
    print(f"Warning: Could not import SessionService: {e}")
    SessionService = None

try:
    from app.services.role_service import RoleService
except ImportError as e:
    print(f"Warning: Could not import RoleService: {e}")
    RoleService = None

try:
    from app.services.user_service import UserService
except ImportError as e:
    print(f"Warning: Could not import UserService: {e}")
    UserService = None

try:
    from app.services.dsar_service import DSARService
except ImportError as e:
    print(f"Warning: Could not import DSARService: {e}")
    DSARService = None

try:
    from app.services.retention_service import RetentionService
except ImportError as e:
    print(f"Warning: Could not import RetentionService: {e}")
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
