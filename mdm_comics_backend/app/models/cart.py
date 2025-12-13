"""
Cart model

DB-004: Added FK indexes per constitution_db.json Section 5
"""
from sqlalchemy import Column, Integer, ForeignKey, DateTime, Index
from sqlalchemy.orm import relationship

from app.core.database import Base
from app.core.utils import utcnow


class CartItem(Base):
    __tablename__ = "cart_items"

    id = Column(Integer, primary_key=True, index=True)
    # DB-003: FK cascades per constitution_db.json Section 5
    # DB-004: FK indexes for JOIN performance
    user_id = Column(Integer, ForeignKey("users.id", ondelete="CASCADE"), nullable=False, index=True)
    product_id = Column(Integer, ForeignKey("products.id", ondelete="CASCADE"), nullable=False, index=True)
    quantity = Column(Integer, default=1)
    created_at = Column(DateTime, default=utcnow)
    updated_at = Column(DateTime, default=utcnow, onupdate=utcnow)

    # Relationships
    user = relationship("User", back_populates="cart_items")
    product = relationship("Product", back_populates="cart_items")

    # DB-004: Composite index for cart lookups
    __table_args__ = (
        Index('ix_cart_items_user_product', 'user_id', 'product_id'),
    )
