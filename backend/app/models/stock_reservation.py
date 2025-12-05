"""
Stock Reservation model

Used to temporarily reserve stock between payment intent creation and order confirmation.
Prevents overselling race conditions per constitution.json §15 (Checkout & Payment Safety).
P2-6: Uses timezone-aware datetime
"""
from datetime import datetime, timedelta, timezone
from sqlalchemy import Column, Integer, String, DateTime, ForeignKey, CheckConstraint
from sqlalchemy.orm import relationship

from app.core.database import Base

# Default reservation TTL in minutes
RESERVATION_TTL_MINUTES = 15


class StockReservation(Base):
    """
    Temporary stock reservation tied to a Stripe PaymentIntent.

    Lifecycle:
    1. Created when PaymentIntent is created (stock is decremented)
    2. Deleted when order is confirmed (reservation converted to sale)
    3. Expired reservations are cleaned up by background job (stock restored)
    """
    __tablename__ = "stock_reservations"
    __table_args__ = (
        CheckConstraint("quantity > 0", name="ck_reservation_quantity_positive"),
    )

    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False, index=True)
    product_id = Column(Integer, ForeignKey("products.id"), nullable=False, index=True)
    quantity = Column(Integer, nullable=False)
    payment_intent_id = Column(String(255), nullable=False, index=True)
    expires_at = Column(DateTime, nullable=False, index=True)
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))

    # Relationships
    user = relationship("User")
    product = relationship("Product")

    @property
    def is_expired(self) -> bool:
        """Check if this reservation has expired."""
        return datetime.now(timezone.utc) > self.expires_at

    @classmethod
    def create_expiry(cls, ttl_minutes: int = RESERVATION_TTL_MINUTES) -> datetime:
        """Calculate expiry timestamp from now."""
        return datetime.now(timezone.utc) + timedelta(minutes=ttl_minutes)
