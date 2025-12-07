"""
Alerting Service for UPS Shipping Integration v1.28.0

Provides PagerDuty integration for shipping alerts:
- Label generation failures
- Tracking sync issues
- Address validation errors
- Rate quote failures
- Redis fallback state alerts

Per constitution_binder.json: All critical failures must trigger alerts.
"""
import json
import logging
import os
from datetime import datetime, timezone
from typing import Any, Dict, Optional

import httpx

logger = logging.getLogger(__name__)

# PagerDuty API
PAGERDUTY_EVENTS_API = "https://events.pagerduty.com/v2/enqueue"

# Environment variables
ENV_PAGERDUTY_ROUTING_KEY = "PAGERDUTY_ROUTING_KEY"
ENV_PAGERDUTY_ENABLED = "PAGERDUTY_ENABLED"
ENV_ALERT_DRY_RUN = "ALERT_DRY_RUN"

# Alert severity mapping
SEVERITY_MAPPING = {
    "critical": "critical",
    "high": "error",
    "warning": "warning",
    "info": "info",
}


def is_alerting_enabled() -> bool:
    """Check if PagerDuty alerting is enabled."""
    enabled = os.environ.get(ENV_PAGERDUTY_ENABLED, "true").lower()
    return enabled in ("true", "1", "yes")


def is_dry_run() -> bool:
    """Check if alerts should be logged only (not sent)."""
    dry_run = os.environ.get(ENV_ALERT_DRY_RUN, "false").lower()
    return dry_run in ("true", "1", "yes")


def get_routing_key() -> Optional[str]:
    """Get PagerDuty routing key from environment."""
    return os.environ.get(ENV_PAGERDUTY_ROUTING_KEY)


def send_pagerduty_alert(
    severity: str,
    summary: str,
    details: Optional[Dict[str, Any]] = None,
    source: str = "mdm-comics-shipping",
    component: str = "shipping",
    group: str = "ups-integration",
    dedup_key: Optional[str] = None,
) -> bool:
    """
    Send an alert to PagerDuty.

    Args:
        severity: Alert severity (critical, high, warning, info)
        summary: Short summary of the alert
        details: Additional details for the alert
        source: Source of the alert
        component: Component that generated the alert
        group: Logical grouping for the alert
        dedup_key: Deduplication key (optional)

    Returns:
        True if alert was sent successfully
    """
    if not is_alerting_enabled():
        logger.debug(f"Alerting disabled, skipping: {summary}")
        return False

    # Map severity
    pd_severity = SEVERITY_MAPPING.get(severity.lower(), "warning")

    # Build payload
    payload = {
        "routing_key": get_routing_key(),
        "event_action": "trigger",
        "payload": {
            "summary": summary[:1024],  # PagerDuty limit
            "severity": pd_severity,
            "source": source,
            "component": component,
            "group": group,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "custom_details": details or {},
        },
    }

    if dedup_key:
        payload["dedup_key"] = dedup_key

    # Dry run mode - just log
    if is_dry_run():
        logger.info(f"[DRY RUN] PagerDuty alert: {summary} (severity: {pd_severity})")
        logger.debug(f"[DRY RUN] Alert details: {json.dumps(details or {}, indent=2)}")
        return True

    # Check for routing key
    if not payload["routing_key"]:
        logger.warning(f"No PAGERDUTY_ROUTING_KEY set, logging alert only: {summary}")
        logger.info(f"Alert [{pd_severity}]: {summary}")
        if details:
            logger.info(f"Alert details: {json.dumps(details, indent=2)}")
        return False

    # Send to PagerDuty
    try:
        response = httpx.post(
            PAGERDUTY_EVENTS_API,
            json=payload,
            headers={"Content-Type": "application/json"},
            timeout=10.0,
        )

        if response.status_code == 202:
            logger.info(f"PagerDuty alert sent: {summary}")
            return True
        else:
            logger.error(f"PagerDuty API error: {response.status_code} - {response.text}")
            return False

    except Exception as e:
        logger.error(f"Failed to send PagerDuty alert: {e}")
        # Fall back to logging
        logger.critical(f"[ALERT FAILED] {summary}")
        if details:
            logger.critical(f"[ALERT FAILED] Details: {json.dumps(details, indent=2)}")
        return False


def resolve_pagerduty_alert(
    dedup_key: str,
    summary: Optional[str] = None,
) -> bool:
    """
    Resolve a PagerDuty alert.

    Args:
        dedup_key: The deduplication key of the alert to resolve
        summary: Optional resolution summary

    Returns:
        True if resolution was sent successfully
    """
    if not is_alerting_enabled():
        return False

    if is_dry_run():
        logger.info(f"[DRY RUN] PagerDuty resolve: {dedup_key}")
        return True

    routing_key = get_routing_key()
    if not routing_key:
        logger.debug(f"No routing key, skipping resolve for: {dedup_key}")
        return False

    payload = {
        "routing_key": routing_key,
        "event_action": "resolve",
        "dedup_key": dedup_key,
    }

    try:
        response = httpx.post(
            PAGERDUTY_EVENTS_API,
            json=payload,
            headers={"Content-Type": "application/json"},
            timeout=10.0,
        )

        if response.status_code == 202:
            logger.info(f"PagerDuty alert resolved: {dedup_key}")
            return True
        else:
            logger.error(f"PagerDuty resolve error: {response.status_code}")
            return False

    except Exception as e:
        logger.error(f"Failed to resolve PagerDuty alert: {e}")
        return False


# Convenience functions for specific alert types


def alert_label_generation_failure(
    order_id: int,
    error: str,
    tracking_number: Optional[str] = None,
    retry_count: int = 0,
) -> bool:
    """Alert for label generation failure."""
    return send_pagerduty_alert(
        severity="high" if retry_count < 3 else "critical",
        summary=f"[LABEL FAILURE] Order {order_id} label generation failed",
        details={
            "order_id": order_id,
            "tracking_number": tracking_number,
            "error": error[:500],
            "retry_count": retry_count,
            "action": "Check UPS API credentials and order details",
        },
        component="label-generation",
        dedup_key=f"label-failure-{order_id}",
    )


def alert_tracking_sync_failure(
    shipment_id: int,
    tracking_number: str,
    error: str,
    consecutive_failures: int = 1,
) -> bool:
    """Alert for tracking sync failure."""
    severity = "warning" if consecutive_failures < 3 else "high"
    if consecutive_failures >= 5:
        severity = "critical"

    return send_pagerduty_alert(
        severity=severity,
        summary=f"[TRACKING SYNC] Shipment {shipment_id} tracking update failed",
        details={
            "shipment_id": shipment_id,
            "tracking_number": tracking_number,
            "error": error[:500],
            "consecutive_failures": consecutive_failures,
            "action": "Check UPS tracking API and network connectivity",
        },
        component="tracking-sync",
        dedup_key=f"tracking-sync-{shipment_id}",
    )


def alert_address_validation_failure(
    address_id: int,
    error: str,
    user_id: Optional[int] = None,
) -> bool:
    """Alert for address validation failure."""
    return send_pagerduty_alert(
        severity="warning",
        summary=f"[ADDRESS VALIDATION] Address {address_id} validation failed",
        details={
            "address_id": address_id,
            "user_id": user_id,
            "error": error[:500],
            "action": "Review address data and UPS validation response",
        },
        component="address-validation",
        dedup_key=f"address-validation-{address_id}",
    )


def alert_rate_quote_failure(
    order_id: Optional[int],
    destination: str,
    error: str,
) -> bool:
    """Alert for rate quote failure."""
    return send_pagerduty_alert(
        severity="warning",
        summary=f"[RATE QUOTE] Failed to get shipping rates",
        details={
            "order_id": order_id,
            "destination": destination[:100],  # Sanitized
            "error": error[:500],
            "action": "Check UPS rating API and carrier configuration",
        },
        component="rate-quote",
        dedup_key=f"rate-quote-{order_id or 'unknown'}",
    )


def alert_redis_fallback(
    filename: str,
    records_processed: int,
    ttl_remaining: Optional[int] = None,
) -> bool:
    """Alert for Redis fallback state."""
    severity = "warning"
    if records_processed > 50:
        severity = "high"
    if records_processed > 100:
        severity = "critical"

    return send_pagerduty_alert(
        severity=severity,
        summary=f"[REDIS FALLBACK] Config file {filename} in fallback mode",
        details={
            "filename": filename,
            "records_processed": records_processed,
            "ttl_remaining_seconds": ttl_remaining,
            "action": "Check config file and Redis connectivity",
        },
        component="migration-fallback",
        dedup_key=f"redis-fallback-{filename}",
    )


def alert_carrier_api_error(
    carrier: str,
    endpoint: str,
    error_code: str,
    error_message: str,
) -> bool:
    """Alert for carrier API error."""
    return send_pagerduty_alert(
        severity="high",
        summary=f"[CARRIER API] {carrier} {endpoint} returned error {error_code}",
        details={
            "carrier": carrier,
            "endpoint": endpoint,
            "error_code": error_code,
            "error_message": error_message[:500],
            "action": "Check carrier API status and credentials",
        },
        component="carrier-api",
        dedup_key=f"carrier-api-{carrier}-{error_code}",
    )


def alert_shipment_exception(
    shipment_id: int,
    tracking_number: str,
    exception_code: str,
    exception_description: str,
) -> bool:
    """Alert for shipment exception (delivery issue)."""
    return send_pagerduty_alert(
        severity="high",
        summary=f"[SHIPMENT EXCEPTION] Tracking {tracking_number} has delivery exception",
        details={
            "shipment_id": shipment_id,
            "tracking_number": tracking_number,
            "exception_code": exception_code,
            "exception_description": exception_description,
            "action": "Review shipment status and contact carrier if needed",
        },
        component="shipment-tracking",
        dedup_key=f"shipment-exception-{tracking_number}",
    )
