/**
 * Middleware Service API
 *
 * Connects to the MDM Comics Middleware Service for shared utilities
 * like address normalization and header propagation.
 */

// Middleware service URL - default to localhost for dev
function resolveMiddlewareBase() {
  if (import.meta.env.VITE_MIDDLEWARE_URL) {
    return import.meta.env.VITE_MIDDLEWARE_URL;
  }

  if (typeof window === 'undefined') {
    return 'http://localhost:8001';
  }

  const hostname = window.location.hostname;
  const isLocalhost = hostname === 'localhost' || hostname === '127.0.0.1';

  // In production, middleware should be configured via env var
  // Silently disable if not configured - no console noise
  if (!isLocalhost && import.meta.env.PROD) {
    return null;
  }

  return 'http://localhost:8001';
}

const MIDDLEWARE_BASE = resolveMiddlewareBase();

/**
 * Check if middleware service is configured
 */
export function isMiddlewareConfigured() {
  return MIDDLEWARE_BASE !== null;
}

/**
 * Fetch wrapper for middleware service
 */
async function fetchMiddleware(endpoint, options = {}) {
  if (!MIDDLEWARE_BASE) {
    throw new Error('Middleware service not configured');
  }

  const url = `${MIDDLEWARE_BASE}${endpoint}`;

  const headers = {
    'Content-Type': 'application/json',
    ...options.headers,
  };

  const response = await fetch(url, {
    ...options,
    headers,
  });

  if (!response.ok) {
    const error = await response.json().catch(() => ({}));
    throw new Error(error.detail || `Middleware Error: ${response.status}`);
  }

  return response.json();
}

/**
 * Middleware API
 */
export const middlewareAPI = {
  /**
   * Health check
   */
  health: async () => {
    return fetchMiddleware('/health');
  },

  /**
   * Normalize a shipping address
   * Standardizes address formatting (uppercase, postal code formatting, etc.)
   *
   * @param {Object} address - { line1, line2?, city, state, postal_code, country? }
   * @returns {Object} - { address_lines, city, state, postal_code, country }
   */
  normalizeAddress: async (address) => {
    return fetchMiddleware('/middleware/normalize-address', {
      method: 'POST',
      body: JSON.stringify({
        line1: address.line1,
        line2: address.line2 || null,
        city: address.city,
        state: address.state,
        postal_code: address.postal_code || address.postalCode,
        country: address.country || 'US',
      }),
    });
  },

  /**
   * Generate propagation headers for service-to-service calls
   * Used internally by backend services
   *
   * @param {Object} userContext - { user_id, email, roles? }
   * @returns {Object} - { headers: { x-mdm-user, x-mdm-email, x-mdm-roles } }
   */
  propagateHeaders: async (userContext) => {
    return fetchMiddleware('/middleware/propagate-headers', {
      method: 'POST',
      body: JSON.stringify({
        user_id: userContext.user_id || userContext.userId,
        email: userContext.email,
        roles: userContext.roles || [],
      }),
    });
  },
};

export default middlewareAPI;
