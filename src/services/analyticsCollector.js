/**
 * Maximum Telemetry Collector
 *
 * Captures everything possible from the browser.
 */

import { v4 as uuidv4 } from 'uuid';

const API_BASE = import.meta.env.VITE_API_URL || 'http://localhost:8080/api';
const BEACON_URL = `${API_BASE}/analytics`;

// Session storage key
const SESSION_KEY = 'mdm_session_id';

// Event buffer
let eventBuffer = [];
let bufferTimeout = null;
const BUFFER_FLUSH_INTERVAL = 5000; // 5 seconds
const BUFFER_MAX_SIZE = 50;

// Sequence counter
let sequenceNumber = 0;

/**
 * Get or create session ID
 */
function getSessionId() {
  let sessionId = sessionStorage.getItem(SESSION_KEY);
  if (!sessionId) {
    sessionId = uuidv4();
    sessionStorage.setItem(SESSION_KEY, sessionId);
  }
  return sessionId;
}

/**
 * Check if user has opted out
 */
function isOptedOut() {
  return document.cookie.includes('mdm_analytics_optout=1');
}

/**
 * Get UTM parameters from URL
 */
function getUtmParams() {
  const params = new URLSearchParams(window.location.search);
  return {
    utm_source: params.get('utm_source'),
    utm_medium: params.get('utm_medium'),
    utm_campaign: params.get('utm_campaign'),
    utm_term: params.get('utm_term'),
    utm_content: params.get('utm_content'),
  };
}

/**
 * Get device/connection info
 */
function getDeviceInfo() {
  const connection = navigator.connection || navigator.mozConnection || navigator.webkitConnection;

  return {
    viewport_width: window.innerWidth,
    viewport_height: window.innerHeight,
    screen_width: screen.width,
    screen_height: screen.height,
    device_pixel_ratio: window.devicePixelRatio,
    connection_type: connection?.effectiveType,
    connection_downlink: connection?.downlink,
  };
}

/**
 * Initialize session and send session.start event
 */
function initSession() {
  if (isOptedOut()) return;

  const sessionId = getSessionId();
  const utmParams = getUtmParams();
  const deviceInfo = getDeviceInfo();

  // Build session context
  const context = {
    session_id: sessionId,
    landing_page: window.location.pathname,
    referrer: document.referrer,
    ...utmParams,
    ...deviceInfo,
  };

  // Queue session start event
  trackEvent('session.start', { ...context });

  // Send initial context to server
  fetch(`${BEACON_URL}/beacon/events`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    credentials: 'include',
    body: JSON.stringify({
      session_id: sessionId,
      events: [],
      context,
    }),
  }).catch(() => {}); // Silent fail

  return sessionId;
}

/**
 * Queue an event for transmission
 */
function trackEvent(type, payload = {}) {
  if (isOptedOut()) return;

  const event = {
    type,
    payload,
    page_url: window.location.href,
    page_route: window.location.pathname,
    timestamp: new Date().toISOString(),
    sequence: sequenceNumber++,
  };

  eventBuffer.push(event);

  // Flush if buffer is full
  if (eventBuffer.length >= BUFFER_MAX_SIZE) {
    flushEvents();
  } else if (!bufferTimeout) {
    // Schedule flush
    bufferTimeout = setTimeout(flushEvents, BUFFER_FLUSH_INTERVAL);
  }
}

/**
 * Flush event buffer to server
 */
async function flushEvents() {
  if (bufferTimeout) {
    clearTimeout(bufferTimeout);
    bufferTimeout = null;
  }

  if (eventBuffer.length === 0) return;

  const events = [...eventBuffer];
  eventBuffer = [];

  const sessionId = getSessionId();

  try {
    // Use sendBeacon for reliability
    const blob = new Blob([JSON.stringify({
      session_id: sessionId,
      events,
      context: {},
    })], { type: 'application/json' });

    if (navigator.sendBeacon) {
      navigator.sendBeacon(`${BEACON_URL}/beacon/events`, blob);
    } else {
      await fetch(`${BEACON_URL}/beacon/events`, {
        method: 'POST',
        body: blob,
        credentials: 'include',
        keepalive: true,
      });
    }
  } catch (e) {
    // Put events back in buffer on failure
    eventBuffer = [...events, ...eventBuffer];
  }
}

/**
 * Identify user (call on login/register)
 */
async function identifyUser(userId) {
  if (isOptedOut()) return;

  const sessionId = getSessionId();

  trackEvent('session.identify', { user_id: userId });

  try {
    await fetch(`${BEACON_URL}/identify`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      credentials: 'include',
      body: JSON.stringify({ session_id: sessionId }),
    });
  } catch (e) {
    // Silent fail
  }
}

// ============================================================================
// SPECIALIZED TRACKERS
// ============================================================================

/**
 * Track search query
 */
function trackSearch(query, searchType, resultCount, filters = {}) {
  trackEvent('search.query', {
    query,
    search_type: searchType,
    result_count: resultCount,
    filters,
  });
}

/**
 * Track search result click
 */
function trackSearchClick(query, position, productId) {
  trackEvent('search.result_click', {
    query,
    result_position: position,
    product_id: productId,
  });
}

/**
 * Track product view
 */
function trackProductView(productId, source, sourceQuery = null) {
  trackEvent('product.view', {
    product_id: productId,
    source,
    source_query: sourceQuery,
    source_page: document.referrer,
  });
}

/**
 * Track cart add
 */
function trackCartAdd(productId, productName, price, quantity, cartValue, cartItemCount) {
  trackEvent('cart.add', {
    cart_id: getSessionId(),
    product_id: productId,
    product_name: productName,
    product_price: price,
    quantity_delta: quantity,
    cart_value: cartValue,
    cart_item_count: cartItemCount,
  });
}

/**
 * Track cart remove
 */
function trackCartRemove(productId, quantity, cartValue, cartItemCount) {
  trackEvent('cart.remove', {
    cart_id: getSessionId(),
    product_id: productId,
    quantity_delta: -quantity,
    cart_value: cartValue,
    cart_item_count: cartItemCount,
  });
}

/**
 * Track cart update
 */
function trackCartUpdate(productId, oldQty, newQty, cartValue, cartItemCount) {
  trackEvent('cart.update', {
    cart_id: getSessionId(),
    product_id: productId,
    quantity_before: oldQty,
    quantity_after: newQty,
    quantity_delta: newQty - oldQty,
    cart_value: cartValue,
    cart_item_count: cartItemCount,
  });
}

/**
 * Track checkout start
 */
function trackCheckoutStart(items, totalValue) {
  trackEvent('checkout.start', {
    cart_id: getSessionId(),
    items,
    cart_value: totalValue,
    cart_item_count: items.length,
  });
}

/**
 * Track checkout step
 */
function trackCheckoutStep(stepName, stepDuration = null) {
  trackEvent('checkout.step', {
    step_name: stepName,
    step_duration: stepDuration,
  });
}

/**
 * Track checkout complete
 */
function trackCheckoutComplete(orderId, total, couponCode = null) {
  trackEvent('checkout.complete', {
    order_id: orderId,
    total,
    coupon_used: couponCode,
  });

  // Immediate flush on conversion
  flushEvents();
}

/**
 * Track error
 */
function trackError(type, message, stack = null, context = null) {
  trackEvent(`error.${type}`, {
    message,
    stack,
    context,
    filename: stack?.match(/at.*\((.*?):\d+:\d+\)/)?.[1],
  });
}

// ============================================================================
// AUTO-TRACKING SETUP
// ============================================================================

/**
 * Set up automatic error tracking
 */
function setupErrorTracking() {
  // JavaScript errors
  window.onerror = (message, source, line, column, error) => {
    trackError('js', message, error?.stack, { source, line, column });
    return false;
  };

  // Unhandled promise rejections
  window.addEventListener('unhandledrejection', (event) => {
    trackError('unhandled_rejection', event.reason?.message || String(event.reason), event.reason?.stack);
  });
}

/**
 * Set up page visibility tracking
 */
function setupVisibilityTracking() {
  document.addEventListener('visibilitychange', () => {
    if (document.visibilityState === 'hidden') {
      trackEvent('page.hide', {});
      flushEvents();
    }
  });

  window.addEventListener('beforeunload', () => {
    trackEvent('session.end', {
      duration: Math.round((Date.now() - performance.timing.navigationStart) / 1000),
    });
    flushEvents();
  });
}

// ============================================================================
// EXPORTS
// ============================================================================

export const analytics = {
  init: () => {
    const sessionId = initSession();
    setupErrorTracking();
    setupVisibilityTracking();
    return sessionId;
  },
  track: trackEvent,
  flush: flushEvents,
  identify: identifyUser,

  // Specialized trackers
  search: trackSearch,
  searchClick: trackSearchClick,
  productView: trackProductView,
  cartAdd: trackCartAdd,
  cartRemove: trackCartRemove,
  cartUpdate: trackCartUpdate,
  checkoutStart: trackCheckoutStart,
  checkoutStep: trackCheckoutStep,
  checkoutComplete: trackCheckoutComplete,
  error: trackError,

  // Utilities
  getSessionId,
  isOptedOut,
};

export default analytics;
