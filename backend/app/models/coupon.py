"""
Coupon System Models

Full-featured coupon and promotion management.
Supports percentage/fixed discounts, usage limits, campaigns, and recovery coupons.
"""

from datetime import datetime, timezone
from typing import Optional, List
from sqlalchemy import (
    Column, String, Boolean, Integer, Float,
    DateTime, ForeignKey, Text, Numeric
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import relationship

from app.core.database import Base


def utcnow():
    """Return timezone-aware UTC datetime."""
    return datetime.now(timezone.utc)


class CouponCampaign(Base):
    """Campaign grouping for coupons."""
    __tablename__ = "coupon_campaigns"

    id = Column(Integer, primary_key=True, autoincrement=True)

    name = Column(String(100), nullable=False)
    description = Column(Text)
    campaign_type = Column(String(30), nullable=False, index=True)  # 'abandonment_recovery', 'welcome', 'seasonal', 'flash_sale'

    # Auto-generation settings (for abandonment campaigns)
    auto_generate = Column(Boolean, default=False)
    code_prefix = Column(String(20))
    discount_type = Column(String(20))
    discount_value = Column(Numeric(10, 2))
    validity_hours = Column(Integer, default=72)

    # Targeting
    target_segment = Column(JSONB, default=dict)  # {cart_value_min: 50, abandoned_hours_min: 2}

    # Limits
    max_coupons = Column(Integer)
    generated_count = Column(Integer, default=0)
    redeemed_count = Column(Integer, default=0)

    # Performance tracking
    emails_sent = Column(Integer, default=0)
    emails_opened = Column(Integer, default=0)
    total_revenue = Column(Numeric(12, 2), default=0)

    # Status
    status = Column(String(20), default='draft', index=True)  # 'draft', 'active', 'paused', 'completed'
    starts_at = Column(DateTime(timezone=True))
    ends_at = Column(DateTime(timezone=True))

    created_at = Column(DateTime(timezone=True), nullable=False, default=utcnow)
    updated_at = Column(DateTime(timezone=True), nullable=False, default=utcnow, onupdate=utcnow)

    coupons = relationship("Coupon", back_populates="campaign")


class Coupon(Base):
    """Individual coupon codes."""
    __tablename__ = "coupons"

    id = Column(Integer, primary_key=True, autoincrement=True)

    code = Column(String(50), unique=True, nullable=False, index=True)
    name = Column(String(100), nullable=False)
    description = Column(Text)

    # Discount type: 'percentage', 'fixed_amount', 'free_shipping'
    discount_type = Column(String(20), nullable=False)
    discount_value = Column(Numeric(10, 2), nullable=False)

    # Constraints
    minimum_order_value = Column(Numeric(10, 2))
    maximum_discount = Column(Numeric(10, 2))  # Cap for percentage discounts

    # Product restrictions (NULL = all products)
    applies_to = Column(String(20), default='all')  # 'all', 'category', 'product'
    applicable_categories = Column(JSONB, default=list)  # ['comics', 'funko']
    applicable_product_ids = Column(JSONB, default=list)  # [1, 2, 3]
    excluded_product_ids = Column(JSONB, default=list)

    # Usage limits
    usage_limit_total = Column(Integer)  # NULL = unlimited
    usage_limit_per_user = Column(Integer, default=1)
    usage_count = Column(Integer, default=0)

    # User restrictions
    first_order_only = Column(Boolean, default=False)
    registered_users_only = Column(Boolean, default=False)
    specific_user_ids = Column(JSONB, default=list)  # Empty = all users

    # Validity
    starts_at = Column(DateTime(timezone=True), nullable=False, default=utcnow)
    expires_at = Column(DateTime(timezone=True))
    is_active = Column(Boolean, default=True, index=True)

    # Campaign tracking
    campaign_id = Column(Integer, ForeignKey("coupon_campaigns.id"), index=True)
    source = Column(String(50), index=True)  # 'manual', 'abandonment', 'welcome', 'referral'

    # Metadata
    created_by = Column(Integer, ForeignKey("users.id"))
    created_at = Column(DateTime(timezone=True), nullable=False, default=utcnow)
    updated_at = Column(DateTime(timezone=True), nullable=False, default=utcnow, onupdate=utcnow)

    campaign = relationship("CouponCampaign", back_populates="coupons")
    usages = relationship("CouponUsage", back_populates="coupon")


class CouponUsage(Base):
    """Track coupon redemptions."""
    __tablename__ = "coupon_usage"

    id = Column(Integer, primary_key=True, autoincrement=True)

    # Coupon reference
    coupon_id = Column(Integer, ForeignKey("coupons.id"), nullable=False, index=True)
    coupon_code = Column(String(50), nullable=False)

    # User
    user_id = Column(Integer, ForeignKey("users.id"), index=True)
    session_id = Column(String(64))

    # Order
    order_id = Column(Integer, ForeignKey("orders.id"), index=True)

    # Application details
    cart_value_before = Column(Numeric(10, 2), nullable=False)
    discount_applied = Column(Numeric(10, 2), nullable=False)
    cart_value_after = Column(Numeric(10, 2), nullable=False)

    # Timing
    applied_at = Column(DateTime(timezone=True), nullable=False, default=utcnow)
    converted_at = Column(DateTime(timezone=True))  # When order completed

    # Status: 'applied', 'converted', 'expired', 'removed'
    status = Column(String(20), default='applied', index=True)

    created_at = Column(DateTime(timezone=True), nullable=False, default=utcnow)

    coupon = relationship("Coupon", back_populates="usages")
