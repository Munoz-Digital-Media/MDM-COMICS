"""
Analytics Models - Maximum Collection

All the models for session tracking, events, replays, and performance data.
Implements full telemetry capture for commerce intelligence.
"""

from datetime import datetime, timezone
from typing import Optional, List, Dict, Any
from uuid import uuid4
from sqlalchemy import (
    Column, String, Boolean, Integer, Float, BigInteger,
    DateTime, ForeignKey, Text, LargeBinary, Numeric, UniqueConstraint
)
from sqlalchemy.dialects.postgresql import UUID, JSONB
from sqlalchemy.orm import relationship

from app.core.database import Base


def utcnow():
    """Return timezone-aware UTC datetime."""
    return datetime.now(timezone.utc)


class AnalyticsSession(Base):
    """User session with full context capture."""
    __tablename__ = "analytics_sessions"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid4)
    session_id = Column(String(64), unique=True, nullable=False, index=True)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=True, index=True)

    # Timing
    started_at = Column(DateTime(timezone=True), nullable=False, default=utcnow)
    ended_at = Column(DateTime(timezone=True), nullable=True)
    last_activity_at = Column(DateTime(timezone=True), nullable=False, default=utcnow)

    # Entry context
    landing_page = Column(String(500))
    referrer = Column(String(500))
    referrer_domain = Column(String(255))
    utm_source = Column(String(100))
    utm_medium = Column(String(100))
    utm_campaign = Column(String(100))
    utm_term = Column(String(255))
    utm_content = Column(String(100))

    # Device & connection
    user_agent = Column(Text)
    user_agent_parsed = Column(JSONB, default=dict)
    viewport_width = Column(Integer)
    viewport_height = Column(Integer)
    screen_width = Column(Integer)
    screen_height = Column(Integer)
    device_pixel_ratio = Column(Float)
    connection_type = Column(String(20))
    connection_downlink = Column(Float)

    # Geo (from IP - hashed for privacy)
    ip_hash = Column(String(64))
    country_code = Column(String(2))
    region = Column(String(100))
    city = Column(String(100))

    # Session summary (updated on end)
    page_count = Column(Integer, default=0)
    event_count = Column(Integer, default=0)
    duration_seconds = Column(Integer)
    bounced = Column(Boolean, default=False)
    converted = Column(Boolean, default=False)
    conversion_order_id = Column(Integer)

    # Replay availability
    has_replay = Column(Boolean, default=False)
    replay_duration_seconds = Column(Integer)

    created_at = Column(DateTime(timezone=True), nullable=False, default=utcnow)
    updated_at = Column(DateTime(timezone=True), nullable=False, default=utcnow, onupdate=utcnow)

    # Relationships
    events = relationship("AnalyticsEvent", back_populates="session",
                         foreign_keys="AnalyticsEvent.session_id",
                         primaryjoin="AnalyticsSession.session_id == AnalyticsEvent.session_id")
    replay = relationship("SessionReplay", back_populates="session", uselist=False,
                         foreign_keys="SessionReplay.session_id",
                         primaryjoin="AnalyticsSession.session_id == SessionReplay.session_id")


class AnalyticsEvent(Base):
    """Generic event storage with flexible payload."""
    __tablename__ = "analytics_events"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid4)
    session_id = Column(String(64), nullable=False, index=True)
    user_id = Column(Integer, nullable=True, index=True)

    event_type = Column(String(50), nullable=False, index=True)
    event_category = Column(String(30), nullable=False, index=True)
    payload = Column(JSONB, nullable=False, default=dict)

    page_url = Column(String(500))
    page_route = Column(String(255))

    client_timestamp = Column(DateTime(timezone=True), nullable=False)
    server_timestamp = Column(DateTime(timezone=True), nullable=False, default=utcnow)
    sequence_number = Column(Integer, nullable=False)

    created_at = Column(DateTime(timezone=True), nullable=False, default=utcnow)

    session = relationship("AnalyticsSession", back_populates="events",
                          foreign_keys=[session_id],
                          primaryjoin="AnalyticsEvent.session_id == AnalyticsSession.session_id")


class SearchQuery(Base):
    """Captured search queries with outcomes."""
    __tablename__ = "search_queries"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid4)
    session_id = Column(String(64), nullable=False, index=True)
    user_id = Column(Integer, nullable=True, index=True)

    query_text = Column(String(500), nullable=False)
    query_normalized = Column(String(500), index=True)
    search_type = Column(String(20), nullable=False)  # 'comic', 'funko', 'all'

    result_count = Column(Integer, nullable=False)
    had_results = Column(Boolean, nullable=False)
    filters = Column(JSONB, default=dict)

    clicked_result = Column(Boolean, default=False)
    clicked_position = Column(Integer)
    clicked_product_id = Column(Integer)

    searched_at = Column(DateTime(timezone=True), nullable=False, default=utcnow)
    time_to_click_ms = Column(Integer)


class ProductView(Base):
    """Product page views with engagement metrics."""
    __tablename__ = "product_views"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid4)
    session_id = Column(String(64), nullable=False, index=True)
    user_id = Column(Integer, nullable=True, index=True)
    product_id = Column(Integer, ForeignKey("products.id"), nullable=False, index=True)

    source_type = Column(String(30), nullable=False)  # 'search', 'browse', 'direct', 'recommendation', 'cart'
    source_query = Column(String(500))
    source_page = Column(String(255))

    view_duration_seconds = Column(Integer)
    scroll_depth_percent = Column(Integer)
    image_views = Column(Integer, default=0)
    detail_expands = Column(Integer, default=0)

    added_to_cart = Column(Boolean, default=False)
    time_to_cart_ms = Column(Integer)

    viewed_at = Column(DateTime(timezone=True), nullable=False, default=utcnow)
    left_at = Column(DateTime(timezone=True))


class CartSnapshot(Base):
    """Point-in-time cart state for abandonment tracking."""
    __tablename__ = "cart_snapshots"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid4)
    cart_id = Column(String(64), nullable=False, index=True)
    session_id = Column(String(64), nullable=False, index=True)
    user_id = Column(Integer, nullable=True, index=True)

    snapshot_type = Column(String(20), nullable=False, index=True)  # 'created', 'updated', 'checkout_started', 'abandoned', 'converted'
    items = Column(JSONB, nullable=False)  # [{product_id, name, price, quantity, image_url}]
    item_count = Column(Integer, nullable=False)
    subtotal = Column(Numeric(10, 2), nullable=False)

    coupon_code = Column(String(50))
    discount_amount = Column(Numeric(10, 2), default=0)
    order_id = Column(Integer)

    snapshot_at = Column(DateTime(timezone=True), nullable=False, default=utcnow)


class CartEvent(Base):
    """Individual cart modifications."""
    __tablename__ = "cart_events"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid4)
    cart_id = Column(String(64), nullable=False, index=True)
    session_id = Column(String(64), nullable=False, index=True)
    user_id = Column(Integer, nullable=True, index=True)

    event_type = Column(String(20), nullable=False, index=True)  # 'add', 'update', 'remove', 'clear'
    product_id = Column(Integer, ForeignKey("products.id"), index=True)
    product_name = Column(String(255))
    product_price = Column(Numeric(10, 2))

    quantity_before = Column(Integer)
    quantity_after = Column(Integer)
    quantity_delta = Column(Integer)

    cart_item_count_after = Column(Integer, nullable=False)
    cart_value_after = Column(Numeric(10, 2), nullable=False)

    source_page = Column(String(255))
    occurred_at = Column(DateTime(timezone=True), nullable=False, default=utcnow)


class SessionReplay(Base):
    """Session replay metadata."""
    __tablename__ = "session_replays"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid4)
    session_id = Column(String(64), unique=True, nullable=False, index=True)
    user_id = Column(Integer, nullable=True, index=True)

    started_at = Column(DateTime(timezone=True), nullable=False)
    ended_at = Column(DateTime(timezone=True))
    duration_seconds = Column(Integer)

    chunk_count = Column(Integer, default=0)
    total_size_bytes = Column(BigInteger, default=0)
    compressed = Column(Boolean, default=True)

    has_errors = Column(Boolean, default=False)
    has_rage_clicks = Column(Boolean, default=False)
    has_cart_abandonment = Column(Boolean, default=False)

    expires_at = Column(DateTime(timezone=True))
    created_at = Column(DateTime(timezone=True), nullable=False, default=utcnow)

    session = relationship("AnalyticsSession", back_populates="replay",
                          foreign_keys=[session_id],
                          primaryjoin="SessionReplay.session_id == AnalyticsSession.session_id")
    chunks = relationship("SessionReplayChunk", back_populates="replay", cascade="all, delete-orphan")


class SessionReplayChunk(Base):
    """Compressed rrweb event chunks."""
    __tablename__ = "session_replay_chunks"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid4)
    replay_id = Column(UUID(as_uuid=True), ForeignKey("session_replays.id", ondelete="CASCADE"), nullable=False, index=True)

    chunk_index = Column(Integer, nullable=False)
    data = Column(LargeBinary, nullable=False)

    event_count = Column(Integer, nullable=False)
    start_timestamp = Column(BigInteger, nullable=False)
    end_timestamp = Column(BigInteger, nullable=False)
    size_bytes = Column(Integer, nullable=False)

    created_at = Column(DateTime(timezone=True), nullable=False, default=utcnow)

    __table_args__ = (
        UniqueConstraint('replay_id', 'chunk_index', name='uq_replay_chunk'),
    )

    replay = relationship("SessionReplay", back_populates="chunks")


class WebVital(Base):
    """Core Web Vitals measurements."""
    __tablename__ = "web_vitals"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid4)
    session_id = Column(String(64), nullable=False, index=True)

    route = Column(String(255), nullable=False, index=True)
    page_url = Column(String(500))

    # Core Web Vitals
    lcp_ms = Column(Integer)  # Largest Contentful Paint
    lcp_element = Column(String(255))
    fid_ms = Column(Integer)  # First Input Delay
    fid_event = Column(String(50))
    cls = Column(Float)  # Cumulative Layout Shift
    inp_ms = Column(Integer)  # Interaction to Next Paint
    ttfb_ms = Column(Integer)  # Time to First Byte

    # Navigation timing
    dns_ms = Column(Integer)
    tcp_ms = Column(Integer)
    tls_ms = Column(Integer)
    request_ms = Column(Integer)
    response_ms = Column(Integer)
    dom_interactive_ms = Column(Integer)
    dom_complete_ms = Column(Integer)
    load_ms = Column(Integer)

    # Resource summary
    resource_count = Column(Integer)
    resource_total_bytes = Column(BigInteger)
    resource_cached_count = Column(Integer)

    # Device context
    device_type = Column(String(20))
    connection_type = Column(String(20))

    measured_at = Column(DateTime(timezone=True), nullable=False, default=utcnow)


class ErrorEvent(Base):
    """JavaScript and network errors."""
    __tablename__ = "error_events"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid4)
    session_id = Column(String(64), nullable=False, index=True)
    user_id = Column(Integer, nullable=True)

    error_type = Column(String(30), nullable=False, index=True)  # 'js', 'network', 'unhandled_rejection'
    message = Column(Text, nullable=False)
    stack_trace = Column(Text)

    filename = Column(String(500))
    line_number = Column(Integer)
    column_number = Column(Integer)

    page_url = Column(String(500))
    page_route = Column(String(255), index=True)
    user_action_context = Column(String(255))

    # Network error specific
    request_url = Column(String(500))
    request_method = Column(String(10))
    response_status = Column(Integer)

    user_agent = Column(Text)
    occurred_at = Column(DateTime(timezone=True), nullable=False, default=utcnow)


class CartAbandonmentQueue(Base):
    """Queue for abandoned cart recovery."""
    __tablename__ = "cart_abandonment_queue"

    id = Column(Integer, primary_key=True, autoincrement=True)
    cart_id = Column(String(64), nullable=False, index=True)
    session_id = Column(String(64), nullable=False)
    user_id = Column(Integer, nullable=True, index=True)

    user_email = Column(String(255), index=True)
    user_name = Column(String(100))

    cart_snapshot = Column(JSONB, nullable=False)
    cart_value = Column(Numeric(10, 2), nullable=False)
    item_count = Column(Integer, nullable=False)

    last_activity_at = Column(DateTime(timezone=True), nullable=False)
    checkout_step_reached = Column(String(30))
    time_in_cart_seconds = Column(Integer)

    abandonment_score = Column(Float)
    recovery_priority = Column(String(10), index=True)  # 'high', 'medium', 'low'

    recovery_status = Column(String(30), default='pending', index=True)
    recovery_coupon_id = Column(Integer, ForeignKey("coupons.id"))
    recovery_email_sent_at = Column(DateTime(timezone=True))
    recovery_email_opened_at = Column(DateTime(timezone=True))
    recovered_at = Column(DateTime(timezone=True))
    recovered_order_id = Column(Integer)

    expires_at = Column(DateTime(timezone=True), nullable=False)
    created_at = Column(DateTime(timezone=True), nullable=False, default=utcnow)
    updated_at = Column(DateTime(timezone=True), nullable=False, default=utcnow, onupdate=utcnow)
