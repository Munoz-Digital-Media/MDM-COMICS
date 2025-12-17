from app.models.user import User
from app.models.product import Product
from app.models.cart import CartItem
from app.models.order import Order, OrderItem
from app.models.grading import GradeRequest
from app.models.stock_reservation import StockReservation
from app.models.comic_data import (
    ComicPublisher,
    ComicSeries,
    ComicIssue,
    ComicCharacter,
    ComicCreator,
    ComicArc,
    MetronAPILog,
)
from app.models.funko import Funko, FunkoSeriesName
from app.models.analytics import (
    AnalyticsSession,
    AnalyticsEvent,
    SearchQuery,
    ProductView,
    CartSnapshot,
    CartEvent,
    SessionReplay,
    SessionReplayChunk,
    WebVital,
    ErrorEvent,
    CartAbandonmentQueue,
)
from app.models.coupon import Coupon, CouponCampaign, CouponUsage
# Admin Console Inventory System v1.3.0
from app.models.barcode_queue import BarcodeQueue
from app.models.stock_movement import StockMovement
from app.models.inventory_alert import InventoryAlert
# UPS Shipping Integration v1.28.0
from app.models.address import Address, AddressType, AddressValidationStatus
from app.models.carrier import Carrier, CarrierCode, UPS_SERVICE_CODES, UPS_PACKAGE_TYPES
from app.models.shipment import (
    Shipment,
    ShipmentRate,
    ShipmentStatus,
    TrackingEvent,
)
# User Management System v1.0.0
from app.models.role import Role, SYSTEM_ROLES
from app.models.user_role import UserRole
from app.models.user_session import UserSession
from app.models.user_audit_log import UserAuditLog, AuditAction
from app.models.password_reset import PasswordResetToken
from app.models.email_verification import EmailVerificationToken
from app.models.dsar_request import DSARRequest, DSARType, DSARStatus
# Outreach System v1.5.0 - optional imports for graceful degradation
try:
    from app.models.newsletter import NewsletterSubscriber, EmailEvent, SubscriberStatus
except ImportError as e:
    print(f"Warning: Could not import newsletter models: {e}")
    NewsletterSubscriber = None
    EmailEvent = None
    SubscriberStatus = None

try:
    from app.models.content_queue import ContentQueueItem, ContentStatus
except ImportError as e:
    print(f"Warning: Could not import content_queue models: {e}")
    ContentQueueItem = None
    ContentStatus = None

try:
    from app.models.price_changelog import PriceChangelog
except ImportError as e:
    print(f"Warning: Could not import price_changelog model: {e}")
    PriceChangelog = None

# Price Snapshots for AI Intelligence v1.7.0
try:
    from app.models.price_snapshot import PriceSnapshot
except ImportError as e:
    print(f"Warning: Could not import price_snapshot model: {e}")
    PriceSnapshot = None

# Brand Asset Management v1.0.0
from app.models.site_settings import SiteSettings, BrandAsset, BrandAssetVersion, DEFAULT_SITE_SETTINGS

# Data Acquisition Pipeline v1.0.0
try:
    from app.models.pipeline import (
        FieldChangelog,
        ChangeReason,
        DeadLetterQueue,
        DLQStatus,
        PipelineCheckpoint,
        DataQuarantine,
        QuarantineReason,
        FieldProvenance,
    )
except ImportError as e:
    print(f"Warning: Could not import pipeline models: {e}")
    FieldChangelog = None
    ChangeReason = None
    DeadLetterQueue = None
    DLQStatus = None
    PipelineCheckpoint = None
    DataQuarantine = None
    QuarantineReason = None
    FieldProvenance = None

# Multi-Source Enrichment v1.10.0
try:
    from app.models.source_quota import (
        SourceQuota,
        EnrichmentAttempt,
        GradingTrainingExample,
    )
except ImportError as e:
    print(f"Warning: Could not import source_quota models: {e}")
    SourceQuota = None
    EnrichmentAttempt = None
    GradingTrainingExample = None

# Match Review Queue v1.0.0
try:
    from app.models.match_review import (
        MatchReviewQueue,
        MatchAuditLog,
        IsbnSource,
    )
except ImportError as e:
    print(f"Warning: Could not import match_review models: {e}")
    MatchReviewQueue = None
    MatchAuditLog = None
    IsbnSource = None

# Feature Flags v1.0.0 - Runtime carrier toggles
from app.models.feature_flag import FeatureFlag

__all__ = [
    "User",
    "Product",
    "CartItem",
    "Order",
    "OrderItem",
    "GradeRequest",
    "StockReservation",
    "ComicPublisher",
    "ComicSeries",
    "ComicIssue",
    "ComicCharacter",
    "ComicCreator",
    "ComicArc",
    "MetronAPILog",
    "Funko",
    "FunkoSeriesName",
    # Analytics models
    "AnalyticsSession",
    "AnalyticsEvent",
    "SearchQuery",
    "ProductView",
    "CartSnapshot",
    "CartEvent",
    "SessionReplay",
    "SessionReplayChunk",
    "WebVital",
    "ErrorEvent",
    "CartAbandonmentQueue",
    # Coupon models
    "Coupon",
    "CouponCampaign",
    "CouponUsage",
    # Admin Console Inventory System v1.3.0
    "BarcodeQueue",
    "StockMovement",
    "InventoryAlert",
    # UPS Shipping Integration v1.28.0
    "Address",
    "AddressType",
    "AddressValidationStatus",
    "Carrier",
    "CarrierCode",
    "UPS_SERVICE_CODES",
    "UPS_PACKAGE_TYPES",
    "Shipment",
    "ShipmentRate",
    "ShipmentStatus",
    "TrackingEvent",
    # User Management System v1.0.0
    "Role",
    "SYSTEM_ROLES",
    "UserRole",
    "UserSession",
    "UserAuditLog",
    "AuditAction",
    "PasswordResetToken",
    "EmailVerificationToken",
    "DSARRequest",
    "DSARType",
    "DSARStatus",
    # Outreach System v1.5.0
    "NewsletterSubscriber",
    "EmailEvent",
    "SubscriberStatus",
    "ContentQueueItem",
    "ContentStatus",
    "PriceChangelog",
    # Price Snapshots for AI Intelligence v1.7.0
    "PriceSnapshot",
    # Data Acquisition Pipeline v1.0.0
    "FieldChangelog",
    "ChangeReason",
    "DeadLetterQueue",
    "DLQStatus",
    "PipelineCheckpoint",
    "DataQuarantine",
    "QuarantineReason",
    "FieldProvenance",
    # Brand Asset Management v1.0.0
    "SiteSettings",
    "BrandAsset",
    "BrandAssetVersion",
    "DEFAULT_SITE_SETTINGS",
    # Multi-Source Enrichment v1.10.0
    "SourceQuota",
    "EnrichmentAttempt",
    "GradingTrainingExample",
    # Match Review Queue v1.0.0
    "MatchReviewQueue",
    "MatchAuditLog",
    "IsbnSource",
    # Feature Flags v1.0.0
    "FeatureFlag",
]
