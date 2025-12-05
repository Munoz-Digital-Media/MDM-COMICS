/**
 * Full Web Vitals Collection
 *
 * Captures all Core Web Vitals + Navigation Timing.
 */

import { onCLS, onFID, onLCP, onTTFB, onINP } from 'web-vitals';
import { analytics } from './analyticsCollector';

const API_BASE = import.meta.env.VITE_API_URL || 'http://localhost:8080/api';
const VITALS_URL = `${API_BASE}/analytics/beacon/vitals`;

// Collected metrics
let metrics = {};

/**
 * Get device type from user agent
 */
function getDeviceType() {
  const ua = navigator.userAgent.toLowerCase();
  if (/mobile|android|iphone|ipod/.test(ua)) return 'mobile';
  if (/ipad|tablet/.test(ua)) return 'tablet';
  return 'desktop';
}

/**
 * Get navigation timing data
 */
function getNavigationTiming() {
  const timing = performance.getEntriesByType('navigation')[0];
  if (!timing) return {};

  return {
    dns: Math.round(timing.domainLookupEnd - timing.domainLookupStart),
    tcp: Math.round(timing.connectEnd - timing.connectStart),
    tls: timing.secureConnectionStart > 0
      ? Math.round(timing.connectEnd - timing.secureConnectionStart)
      : 0,
    request: Math.round(timing.responseStart - timing.requestStart),
    response: Math.round(timing.responseEnd - timing.responseStart),
    dom_interactive: Math.round(timing.domInteractive),
    dom_complete: Math.round(timing.domComplete),
    load: Math.round(timing.loadEventEnd),
  };
}

/**
 * Get resource timing summary
 */
function getResourceSummary() {
  const resources = performance.getEntriesByType('resource');

  let totalBytes = 0;
  let cachedCount = 0;

  resources.forEach(r => {
    if (r.transferSize === 0 && r.decodedBodySize > 0) {
      cachedCount++;
    }
    totalBytes += r.transferSize || 0;
  });

  return {
    resource_count: resources.length,
    resource_bytes: totalBytes,
    resource_cached: cachedCount,
  };
}

/**
 * Send vitals to server
 */
function sendVitals() {
  const sessionId = analytics.getSessionId();
  const connection = navigator.connection || navigator.mozConnection || navigator.webkitConnection;

  const payload = {
    session_id: sessionId,
    route: window.location.pathname,
    page_url: window.location.href,
    ...metrics,
    ...getNavigationTiming(),
    ...getResourceSummary(),
    device_type: getDeviceType(),
    connection_type: connection?.effectiveType,
  };

  // Use sendBeacon for reliability
  const blob = new Blob([JSON.stringify(payload)], { type: 'application/json' });

  if (navigator.sendBeacon) {
    navigator.sendBeacon(VITALS_URL, blob);
  } else {
    fetch(VITALS_URL, {
      method: 'POST',
      body: blob,
      keepalive: true,
      credentials: 'include',
    }).catch(() => {});
  }
}

/**
 * Initialize Web Vitals collection
 */
function initWebVitals() {
  if (analytics.isOptedOut()) return;

  // Core Web Vitals
  onLCP((metric) => {
    metrics.lcp = Math.round(metric.value);
    metrics.lcp_element = metric.entries?.[0]?.element?.tagName;
  });

  onFID((metric) => {
    metrics.fid = Math.round(metric.value);
    metrics.fid_event = metric.entries?.[0]?.name;
  });

  onCLS((metric) => {
    metrics.cls = Math.round(metric.value * 1000) / 1000;
  });

  onINP((metric) => {
    metrics.inp = Math.round(metric.value);
  });

  onTTFB((metric) => {
    metrics.ttfb = Math.round(metric.value);
  });

  // Send on page hide
  document.addEventListener('visibilitychange', () => {
    if (document.visibilityState === 'hidden') {
      sendVitals();
    }
  });

  // Also send after load (with delay to ensure all CWV are captured)
  window.addEventListener('load', () => {
    setTimeout(sendVitals, 5000);
  });
}

export const vitals = {
  init: initWebVitals,
  send: sendVitals,
};

export default vitals;
