/**
 * Error Tracking Service
 * FE-ERR-001: Production error tracking
 *
 * Provides a unified interface for error tracking that can be extended
 * to integrate with Sentry, LogRocket, or other error tracking services.
 *
 * Currently implements:
 * - Console logging (always)
 * - Backend error reporting endpoint (production)
 * - Browser context collection (URL, user agent, etc.)
 */

import { API_BASE } from './api';

// Error tracking configuration
const ERROR_TRACKING_CONFIG = {
  enabled: import.meta.env.PROD, // Only send to backend in production
  maxErrorsPerSession: 50,       // Prevent error spam
  sampleRate: 1.0,               // 100% of errors (reduce for high traffic)
  ignorePatterns: [
    /ResizeObserver loop/i,      // Benign browser warning
    /Loading chunk \d+ failed/i, // Network issues, handled by retry
    /Network request failed/i,   // Network issues
  ],
};

// Track error count to prevent spam
let errorCount = 0;
let sessionId = null;

/**
 * Initialize error tracking session
 */
function initSession() {
  if (!sessionId) {
    sessionId = `${Date.now()}-${Math.random().toString(36).substr(2, 9)}`;
  }
  return sessionId;
}

/**
 * Check if error should be ignored
 */
function shouldIgnoreError(error) {
  const errorString = error?.message || error?.toString() || '';
  return ERROR_TRACKING_CONFIG.ignorePatterns.some(pattern => pattern.test(errorString));
}

/**
 * Collect browser context for error reports
 */
function collectContext() {
  return {
    url: window.location.href,
    userAgent: navigator.userAgent,
    screenSize: `${window.innerWidth}x${window.innerHeight}`,
    timestamp: new Date().toISOString(),
    sessionId: initSession(),
    referrer: document.referrer || null,
    online: navigator.onLine,
  };
}

/**
 * Format error for reporting
 */
function formatError(error, errorInfo = null, extra = {}) {
  return {
    name: error?.name || 'Error',
    message: error?.message || String(error),
    stack: error?.stack || null,
    componentStack: errorInfo?.componentStack || null,
    context: collectContext(),
    extra,
  };
}

/**
 * Send error to backend endpoint
 */
async function sendToBackend(errorReport) {
  try {
    await fetch(`${API_BASE}/errors/client`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(errorReport),
      credentials: 'include',
      // Don't throw on network errors - we don't want error tracking to cause more errors
      keepalive: true,
    });
  } catch {
    // Silently fail - error tracking should never cause user-facing issues
  }
}

/**
 * Main error capture function
 *
 * @param {Error} error - The error object
 * @param {Object} errorInfo - React error info (componentStack)
 * @param {Object} extra - Additional context
 */
export function captureException(error, errorInfo = null, extra = {}) {
  // Always log to console
  console.error('[ErrorTracking]', error, errorInfo, extra);

  // Check if we should report this error
  if (!ERROR_TRACKING_CONFIG.enabled) {
    return;
  }

  if (shouldIgnoreError(error)) {
    console.debug('[ErrorTracking] Ignored (matched ignore pattern)');
    return;
  }

  if (errorCount >= ERROR_TRACKING_CONFIG.maxErrorsPerSession) {
    console.debug('[ErrorTracking] Rate limited');
    return;
  }

  // Sample rate check
  if (Math.random() > ERROR_TRACKING_CONFIG.sampleRate) {
    return;
  }

  errorCount++;

  const errorReport = formatError(error, errorInfo, extra);
  sendToBackend(errorReport);
}

/**
 * Capture a message (non-error) for tracking
 *
 * @param {string} message - The message
 * @param {string} level - 'info', 'warning', or 'error'
 * @param {Object} extra - Additional context
 */
export function captureMessage(message, level = 'info', extra = {}) {
  if (!ERROR_TRACKING_CONFIG.enabled) {
    console.log(`[ErrorTracking:${level}]`, message, extra);
    return;
  }

  const errorReport = {
    name: 'Message',
    message,
    level,
    context: collectContext(),
    extra,
  };

  sendToBackend(errorReport);
}

/**
 * Set user context for error reports
 * Call this after user logs in
 *
 * @param {Object} user - User object with id, email, name
 */
export function setUser(user) {
  if (user) {
    sessionStorage.setItem('error_tracking_user', JSON.stringify({
      id: user.id,
      email: user.email,
      name: user.name,
    }));
  } else {
    sessionStorage.removeItem('error_tracking_user');
  }
}

/**
 * Get current user from session
 */
export function getUser() {
  try {
    const stored = sessionStorage.getItem('error_tracking_user');
    return stored ? JSON.parse(stored) : null;
  } catch {
    return null;
  }
}

/**
 * Install global error handlers
 * Call this once at app initialization
 */
export function installGlobalHandlers() {
  // Unhandled promise rejections
  window.addEventListener('unhandledrejection', (event) => {
    captureException(
      event.reason instanceof Error ? event.reason : new Error(String(event.reason)),
      null,
      { type: 'unhandledrejection' }
    );
  });

  // Global errors
  window.addEventListener('error', (event) => {
    captureException(
      event.error || new Error(event.message),
      null,
      {
        type: 'global_error',
        filename: event.filename,
        lineno: event.lineno,
        colno: event.colno,
      }
    );
  });

  console.log('[ErrorTracking] Global handlers installed');
}

export default {
  captureException,
  captureMessage,
  setUser,
  getUser,
  installGlobalHandlers,
};
