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
]
