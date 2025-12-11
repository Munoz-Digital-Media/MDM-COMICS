"""
Monitoring and alerting utilities

P2-5: Application monitoring with metrics collection
- Request metrics (latency, error rates)
- Business metrics (orders, cart abandonment)
- Database metrics (pool utilization)
- Alerting hooks for external systems

Uses Prometheus-style metrics that can be scraped by monitoring systems.
"""
import asyncio
import logging
import time
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from typing import Dict, Optional, Callable, List
from threading import Lock

logger = logging.getLogger(__name__)


@dataclass
class MetricBucket:
    """Time-bucketed metric storage for rolling windows."""
    timestamp: datetime
    count: int = 0
    total: float = 0.0
    min_val: Optional[float] = None
    max_val: Optional[float] = None


class MetricsCollector:
    """
    In-memory metrics collector with rolling windows.

    Collects:
    - Counters (monotonically increasing values)
    - Gauges (point-in-time values)
    - Histograms (distribution of values)

    For production, export to Prometheus, DataDog, or CloudWatch.
    """

    def __init__(self, window_minutes: int = 60):
        self._counters: Dict[str, int] = {}
        self._gauges: Dict[str, float] = {}
        self._histograms: Dict[str, deque] = {}  # Rolling window of values
        self._window_minutes = window_minutes
        self._lock = Lock()
        self._start_time = datetime.now(timezone.utc)

    def increment(self, name: str, value: int = 1, labels: Dict[str, str] = None) -> None:
        """Increment a counter metric."""
        key = self._make_key(name, labels)
        with self._lock:
            self._counters[key] = self._counters.get(key, 0) + value

    def gauge(self, name: str, value: float, labels: Dict[str, str] = None) -> None:
        """Set a gauge metric (point-in-time value)."""
        key = self._make_key(name, labels)
        with self._lock:
            self._gauges[key] = value

    def observe(self, name: str, value: float, labels: Dict[str, str] = None) -> None:
        """Record a histogram observation (e.g., latency)."""
        key = self._make_key(name, labels)
        now = datetime.now(timezone.utc)
        with self._lock:
            if key not in self._histograms:
                self._histograms[key] = deque(maxlen=10000)  # Keep last 10k observations
            self._histograms[key].append((now, value))

    def _make_key(self, name: str, labels: Optional[Dict[str, str]]) -> str:
        """Create metric key from name and labels."""
        if not labels:
            return name
        label_str = ",".join(f"{k}={v}" for k, v in sorted(labels.items()))
        return f"{name}{{{label_str}}}"

    def get_counter(self, name: str, labels: Dict[str, str] = None) -> int:
        """Get counter value."""
        key = self._make_key(name, labels)
        return self._counters.get(key, 0)

    def get_gauge(self, name: str, labels: Dict[str, str] = None) -> Optional[float]:
        """Get gauge value."""
        key = self._make_key(name, labels)
        return self._gauges.get(key)

    def get_histogram_stats(self, name: str, labels: Dict[str, str] = None,
                           window_seconds: int = 300) -> Dict:
        """Get histogram statistics for time window."""
        key = self._make_key(name, labels)
        cutoff = datetime.now(timezone.utc) - timedelta(seconds=window_seconds)

        with self._lock:
            if key not in self._histograms:
                return {"count": 0, "avg": 0, "min": 0, "max": 0, "p95": 0}

            values = [v for ts, v in self._histograms[key] if ts > cutoff]

        if not values:
            return {"count": 0, "avg": 0, "min": 0, "max": 0, "p95": 0}

        values.sort()
        p95_idx = int(len(values) * 0.95)

        return {
            "count": len(values),
            "avg": sum(values) / len(values),
            "min": values[0],
            "max": values[-1],
            "p95": values[p95_idx] if p95_idx < len(values) else values[-1],
        }

    def get_all_metrics(self) -> Dict:
        """Get all metrics for export/display."""
        now = datetime.now(timezone.utc)
        uptime = (now - self._start_time).total_seconds()

        # Get histogram stats for common metrics
        request_stats = self.get_histogram_stats("http_request_duration_seconds")
        db_stats = self.get_histogram_stats("db_query_duration_seconds")

        return {
            "uptime_seconds": uptime,
            "counters": dict(self._counters),
            "gauges": dict(self._gauges),
            "request_latency": request_stats,
            "db_latency": db_stats,
            "collected_at": now.isoformat(),
        }

    def cleanup_old_data(self, max_age_minutes: int = 60) -> None:
        """Remove histogram data older than max_age_minutes."""
        cutoff = datetime.now(timezone.utc) - timedelta(minutes=max_age_minutes)
        with self._lock:
            for key in self._histograms:
                # Filter to keep only recent data
                self._histograms[key] = deque(
                    [(ts, v) for ts, v in self._histograms[key] if ts > cutoff],
                    maxlen=10000
                )


# Global metrics collector
metrics = MetricsCollector()


class AlertRule:
    """
    Alert rule that triggers when a condition is met.
    """

    def __init__(
        self,
        name: str,
        metric_name: str,
        threshold: float,
        comparison: str,  # "gt", "lt", "eq"
        window_seconds: int = 300,
        cooldown_seconds: int = 300,
    ):
        self.name = name
        self.metric_name = metric_name
        self.threshold = threshold
        self.comparison = comparison
        self.window_seconds = window_seconds
        self.cooldown_seconds = cooldown_seconds
        self.last_triggered: Optional[datetime] = None

    def check(self, current_value: float) -> bool:
        """Check if alert should fire."""
        # Check cooldown
        if self.last_triggered:
            cooldown_until = self.last_triggered + timedelta(seconds=self.cooldown_seconds)
            if datetime.now(timezone.utc) < cooldown_until:
                return False

        # Check condition
        triggered = False
        if self.comparison == "gt" and current_value > self.threshold:
            triggered = True
        elif self.comparison == "lt" and current_value < self.threshold:
            triggered = True
        elif self.comparison == "eq" and current_value == self.threshold:
            triggered = True

        if triggered:
            self.last_triggered = datetime.now(timezone.utc)

        return triggered


class AlertManager:
    """
    Manages alert rules and notifications.

    For production, integrate with:
    - PagerDuty
    - Slack/Discord webhooks
    - Email (via SMTP or SendGrid)
    - CloudWatch Alarms
    """

    def __init__(self):
        self.rules: List[AlertRule] = []
        self.handlers: List[Callable[[str, str, float], None]] = []
        self._setup_default_rules()

    def _setup_default_rules(self) -> None:
        """Setup default alert rules."""
        # High error rate
        self.add_rule(AlertRule(
            name="high_error_rate",
            metric_name="http_errors_total",
            threshold=100,
            comparison="gt",
            window_seconds=300,
            cooldown_seconds=600,
        ))

        # High latency
        self.add_rule(AlertRule(
            name="high_latency_p95",
            metric_name="http_request_duration_p95",
            threshold=2.0,  # 2 seconds
            comparison="gt",
            window_seconds=300,
            cooldown_seconds=600,
        ))

        # Database connection pool exhaustion
        self.add_rule(AlertRule(
            name="db_pool_exhausted",
            metric_name="db_pool_available",
            threshold=1,
            comparison="lt",
            window_seconds=60,
            cooldown_seconds=300,
        ))

    def add_rule(self, rule: AlertRule) -> None:
        """Add an alert rule."""
        self.rules.append(rule)

    def add_handler(self, handler: Callable[[str, str, float], None]) -> None:
        """
        Add alert handler.

        Handler signature: (alert_name: str, message: str, value: float) -> None
        """
        self.handlers.append(handler)

    def check_alerts(self) -> List[Dict]:
        """Check all rules and fire alerts if needed."""
        fired = []

        for rule in self.rules:
            # Get current value for the metric
            if rule.metric_name.endswith("_p95"):
                base_metric = rule.metric_name.replace("_p95", "")
                stats = metrics.get_histogram_stats(base_metric, window_seconds=rule.window_seconds)
                current_value = stats.get("p95", 0)
            else:
                current_value = metrics.get_counter(rule.metric_name)

            if rule.check(current_value):
                alert_info = {
                    "name": rule.name,
                    "metric": rule.metric_name,
                    "threshold": rule.threshold,
                    "current_value": current_value,
                    "comparison": rule.comparison,
                    "triggered_at": datetime.now(timezone.utc).isoformat(),
                }
                fired.append(alert_info)

                # Notify handlers
                message = (
                    f"Alert: {rule.name} triggered. "
                    f"{rule.metric_name} {rule.comparison} {rule.threshold} "
                    f"(current: {current_value})"
                )

                for handler in self.handlers:
                    try:
                        handler(rule.name, message, current_value)
                    except Exception as e:
                        logger.error(f"Alert handler failed: {e}")

                logger.warning(message)

        return fired


# Global alert manager
alerts = AlertManager()


# Default logging handler for alerts
def log_alert_handler(name: str, message: str, value: float) -> None:
    """Default alert handler that logs to the application logger."""
    logger.warning(f"[ALERT] {message}")


alerts.add_handler(log_alert_handler)


# ============== Request Metrics Middleware Helper ==============

class RequestMetricsMiddleware:
    """
    Middleware to collect request metrics.

    Usage in main.py:
        from app.core.monitoring import RequestMetricsMiddleware
        app.add_middleware(RequestMetricsMiddleware)
    """

    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        start_time = time.perf_counter()

        # Track response status
        status_code = 500

        async def send_wrapper(message):
            nonlocal status_code
            if message["type"] == "http.response.start":
                status_code = message["status"]
            await send(message)

        try:
            await self.app(scope, receive, send_wrapper)
        finally:
            # Record metrics
            duration = time.perf_counter() - start_time
            method = scope.get("method", "UNKNOWN")
            path = scope.get("path", "/")

            # Normalize path (remove IDs for aggregation)
            normalized_path = self._normalize_path(path)

            labels = {"method": method, "path": normalized_path, "status": str(status_code)}

            metrics.observe("http_request_duration_seconds", duration, labels)
            metrics.increment("http_requests_total", labels=labels)

            if status_code >= 400:
                metrics.increment("http_errors_total", labels={"status": str(status_code)})

    def _normalize_path(self, path: str) -> str:
        """Normalize path by replacing IDs with placeholders."""
        import re
        # Replace numeric IDs
        path = re.sub(r'/\d+', '/:id', path)
        # Replace UUIDs
        path = re.sub(r'/[0-9a-f-]{36}', '/:uuid', path)
        return path


# ============== Database Metrics Helper ==============

async def record_db_metrics(pool) -> None:
    """Record database pool metrics."""
    try:
        metrics.gauge("db_pool_size", pool.size())
        metrics.gauge("db_pool_checked_in", pool.checkedin())
        metrics.gauge("db_pool_checked_out", pool.checkedout())
        metrics.gauge("db_pool_overflow", pool.overflow())
        metrics.gauge("db_pool_available", pool.checkedin())
    except Exception as e:
        logger.debug(f"Failed to record DB metrics: {e}")


# ============== Metrics Endpoint Data ==============

def get_prometheus_metrics() -> str:
    """
    Export metrics in Prometheus text format.

    Can be used with /metrics endpoint:
        @app.get("/metrics")
        async def prometheus_metrics():
            return Response(
                content=get_prometheus_metrics(),
                media_type="text/plain"
            )
    """
    lines = []
    all_metrics = metrics.get_all_metrics()

    # Uptime
    lines.append(f"# HELP app_uptime_seconds Application uptime in seconds")
    lines.append(f"# TYPE app_uptime_seconds gauge")
    lines.append(f"app_uptime_seconds {all_metrics['uptime_seconds']:.2f}")

    # Counters
    for key, value in all_metrics["counters"].items():
        safe_key = key.replace("{", "_").replace("}", "_").replace(",", "_").replace("=", "_")
        lines.append(f"{safe_key} {value}")

    # Gauges
    for key, value in all_metrics["gauges"].items():
        safe_key = key.replace("{", "_").replace("}", "_").replace(",", "_").replace("=", "_")
        lines.append(f"{safe_key} {value:.4f}")

    # Request latency histogram summary
    req = all_metrics["request_latency"]
    if req["count"] > 0:
        lines.append(f"# HELP http_request_duration_seconds HTTP request latency")
        lines.append(f"http_request_duration_seconds_count {req['count']}")
        lines.append(f"http_request_duration_seconds_avg {req['avg']:.4f}")
        lines.append(f"http_request_duration_seconds_p95 {req['p95']:.4f}")

    return "\n".join(lines)
