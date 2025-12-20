/**
 * MDM Comics API Service
 * Handles all communication with the backend
 *
 * P1-5: Updated for cookie-based auth with CSRF protection
 * - Uses credentials: 'include' to send cookies
 * - Reads CSRF token from cookie and sends in header
 * - Falls back to header-based auth for compatibility
 *
 * GOVERNANCE: constitution_cyberSec.json Section 5 - Zero cleartext
 * API_BASE imported from centralized config with HTTPS enforcement
 */

import { API_BASE } from '../config/api.config.js';

// Re-export API_BASE for backwards compatibility
export { API_BASE };

// CSRF token cookie name
const CSRF_COOKIE_NAME = 'mdm_csrf_token';

// Token storage key for cross-origin auth fallback
const TOKEN_STORAGE_KEY = 'mdm_access_token';

/**
 * Get CSRF token from cookie
 */
function getCsrfToken() {
  const cookies = document.cookie.split(';');
  for (const cookie of cookies) {
    const [name, value] = cookie.trim().split('=');
    if (name === CSRF_COOKIE_NAME) {
      return decodeURIComponent(value);
    }
  }
  return null;
}

/**
 * Token storage helpers for cross-origin deployments
 * When frontend and backend are on different domains, cookies don't work.
 * Fall back to localStorage + Authorization header.
 */
export function getStoredToken() {
  try {
    return localStorage.getItem(TOKEN_STORAGE_KEY);
  } catch {
    return null;
  }
}

export function setStoredToken(token) {
  try {
    if (token) {
      localStorage.setItem(TOKEN_STORAGE_KEY, token);
    } else {
      localStorage.removeItem(TOKEN_STORAGE_KEY);
    }
  } catch {
    // localStorage unavailable (private browsing, etc.)
  }
}

export function clearStoredToken() {
  try {
    localStorage.removeItem(TOKEN_STORAGE_KEY);
  } catch {
    // Ignore errors
  }
}

/**
 * Generic fetch wrapper with error handling
 *
 * P1-5: Updated to:
 * - Include credentials (cookies) in all requests
 * - Add CSRF token header for mutations
 * - Support both cookie and header auth
 */
async function fetchAPI(endpoint, options = {}) {
  const url = `${API_BASE}${endpoint}`;
  const method = options.method || 'GET';

  // Build headers
  const headers = {
    'Content-Type': 'application/json',
    ...options.headers,
  };

  // Cross-origin auth: Add Authorization header if token is stored and not already provided
  // This enables auth when cookies can't be shared across domains
  if (!headers.Authorization) {
    const storedToken = getStoredToken();
    if (storedToken) {
      headers.Authorization = `Bearer ${storedToken}`;
    }
  }

  // P1-5: Add CSRF token for mutations (non-GET requests)
  // NASTY-006: Log warning if CSRF token is missing for mutations
  if (method !== 'GET' && method !== 'HEAD' && method !== 'OPTIONS') {
    const csrfToken = getCsrfToken();
    if (csrfToken) {
      headers['X-CSRF-Token'] = csrfToken;
    } else if (import.meta.env.PROD && !headers.Authorization) {
      // In production without Authorization header, missing CSRF token is a concern
      console.warn(
        '[API] CSRF token missing for mutation request to:', endpoint,
        '- Request may be rejected. Try refreshing the page.'
      );
    }
  }

  try {
    const response = await fetch(url, {
      ...options,
      headers,
      // P1-5: Include cookies in all requests
      credentials: 'include',
    });

    if (!response.ok) {
      // IMPL-AUTH-RESILIENCE: Silent refresh on 401
      if (response.status === 401 && !options._retry) {
        // Try silent refresh before giving up
        try {
          const refreshResponse = await fetch(`${API_BASE}/auth/refresh`, {
            method: 'POST',
            credentials: 'include',
            headers: { 'Content-Type': 'application/json' },
          });

          if (refreshResponse.ok) {
            const refreshData = await refreshResponse.json();
            // Update stored token if provided (cross-origin fallback)
            if (refreshData?.access_token) {
              setStoredToken(refreshData.access_token);
            }
            // Retry original request with _retry flag to prevent infinite loop
            return fetchAPI(endpoint, { ...options, _retry: true });
          }
        } catch (refreshError) {
          // Refresh failed, fall through to clear and dispatch
          if (import.meta.env.DEV) console.warn('[API] Silent refresh failed:', refreshError);
        }

        // Refresh failed - clear tokens and notify
        clearStoredToken();
        window.dispatchEvent(new CustomEvent('auth:expired'));
      }
      const error = await response.json().catch(() => ({}));
      throw new Error(error.detail || `API Error: ${response.status}`);
    }

    // Handle 204 No Content
    if (response.status === 204) {
      return null;
    }

    return await response.json();
  } catch (error) {
    // LOW-001: Gate console.error behind DEV mode
    if (import.meta.env.DEV) console.error(`API call failed: ${endpoint}`, error);
    throw error;
  }
}

/**
 * Comic Search API
 */
export const comicsAPI = {
  /**
   * Search for comics by series name, issue number, publisher, year, or UPC barcode
   */
  search: async ({ series, number, publisher, year, upc, page = 1 }) => {
    const params = new URLSearchParams();
    if (series) params.append('series', series);
    if (number) params.append('number', number);
    if (publisher) params.append('publisher', publisher);
    if (year) params.append('year', year);
    if (upc) params.append('upc', upc);
    params.append('page', page);

    return fetchAPI(`/comics/search?${params.toString()}`);
  },

  /**
   * Get detailed info for a specific issue by ID
   */
  getIssue: async (issueId) => {
    return fetchAPI(`/comics/issue/${issueId}`);
  },

  /**
   * Search for series
   */
  searchSeries: async ({ name, publisher, year, page = 1 }) => {
    const params = new URLSearchParams();
    if (name) params.append('name', name);
    if (publisher) params.append('publisher', publisher);
    if (year) params.append('year', year);
    params.append('page', page);

    return fetchAPI(`/comics/series?${params.toString()}`);
  },

  /**
   * Get publishers list
   */
  getPublishers: async (page = 1) => {
    return fetchAPI(`/comics/publishers?page=${page}`);
  },

  /**
   * Search characters
   */
  searchCharacters: async (name, page = 1) => {
    const params = new URLSearchParams();
    if (name) params.append('name', name);
    params.append('page', page);

    return fetchAPI(`/comics/characters?${params.toString()}`);
  },

  /**
   * Search creators
   */
  searchCreators: async (name, page = 1) => {
    const params = new URLSearchParams();
    if (name) params.append('name', name);
    params.append('page', page);

    return fetchAPI(`/comics/creators?${params.toString()}`);
  },
};

/**
 * Products API (local inventory)
 * FE-PERF-004: Added pagination support
 */
export const productsAPI = {
  /**
   * Get all products with optional pagination and filters
   * @param {Object} options - Query options
   * @param {number} options.page - Page number (default: 1)
   * @param {number} options.per_page - Items per page (default: 100, max: 100)
   * @param {string} options.category - Filter by category
   * @param {string} options.sort - Sort order
   * @param {AbortSignal} options.signal - AbortController signal
   */
  getAll: async ({ page = 1, per_page = 100, category, sort, signal } = {}) => {
    const params = new URLSearchParams();
    params.append('page', page);
    params.append('per_page', per_page);
    if (category && category !== 'all') params.append('category', category);
    if (sort) params.append('sort', sort);

    const options = {};
    if (signal) options.signal = signal;

    return fetchAPI(`/products?${params.toString()}`, options);
  },

  getById: async (id) => {
    return fetchAPI(`/products/${id}`);
  },

  search: async (query, { page = 1, per_page = 100 } = {}) => {
    const params = new URLSearchParams();
    params.append('search', query);
    params.append('page', page);
    params.append('per_page', per_page);
    return fetchAPI(`/products?${params.toString()}`);
  },
};

/**
 * Auth API
 *
 * P1-5: Updated for cookie-based auth
 * - Tokens are now set as HttpOnly cookies by the server
 * - We still receive tokens in response for backwards compatibility
 * - Frontend stores csrf_token for sending with mutations
 */
export const authAPI = {
  login: async (email, password) => {
    const result = await fetchAPI('/auth/login', {
      method: 'POST',
      body: JSON.stringify({ email, password }),
    });
    // P1-5: Server sets HttpOnly cookies automatically
    // Cross-origin fallback: Also store token in localStorage for header-based auth
    if (result?.access_token) {
      setStoredToken(result.access_token);
    }
    return result;
  },

  register: async (name, email, password) => {
    const result = await fetchAPI('/auth/register', {
      method: 'POST',
      body: JSON.stringify({ name, email, password }),
    });
    // P1-5: Server sets HttpOnly cookies automatically
    // Cross-origin fallback: Also store token in localStorage for header-based auth
    if (result?.access_token) {
      setStoredToken(result.access_token);
    }
    return result;
  },

  logout: async () => {
    // P1-5: New endpoint that clears HttpOnly cookies
    // Also clear stored token for cross-origin auth
    clearStoredToken();
    return fetchAPI('/auth/logout', {
      method: 'POST',
    });
  },

  /**
   * Logout from all devices
   * Revokes all tokens issued before now for this user
   */
  logoutAll: async () => {
    return fetchAPI('/auth/logout-all', {
      method: 'POST',
    });
  },

  /**
   * Get current user
   * P1-5: No longer needs token parameter - uses cookie
   * Still accepts token for backwards compatibility
   */
  me: async (token = null) => {
    const options = {};
    // Support legacy header-based auth if token provided
    if (token) {
      options.headers = {
        Authorization: `Bearer ${token}`,
      };
    }
    return fetchAPI('/auth/me', options);
  },

  /**
   * Refresh tokens
   * P1-5: Uses refresh token from cookie, falls back to body
   */
  refresh: async (refreshToken = null) => {
    const options = {
      method: 'POST',
    };
    if (refreshToken) {
      options.body = JSON.stringify({ refresh_token: refreshToken });
    }
    return fetchAPI('/auth/refresh', options);
  },

  // ============================================================
  // Password Management
  // ============================================================

  /**
   * Request password reset email
   * Always returns success to prevent email enumeration
   */
  requestPasswordReset: async (email) => {
    return fetchAPI('/auth/password-reset/request', {
      method: 'POST',
      body: JSON.stringify({ email }),
    });
  },

  /**
   * Confirm password reset with token
   */
  confirmPasswordReset: async (token, newPassword) => {
    return fetchAPI('/auth/password-reset/confirm', {
      method: 'POST',
      body: JSON.stringify({ token, new_password: newPassword }),
    });
  },

  /**
   * Change password (requires current password)
   */
  changePassword: async (currentPassword, newPassword) => {
    return fetchAPI('/auth/password/change', {
      method: 'POST',
      body: JSON.stringify({
        current_password: currentPassword,
        new_password: newPassword,
      }),
    });
  },

  /**
   * Get password policy requirements
   */
  getPasswordRequirements: async () => {
    return fetchAPI('/auth/password/requirements');
  },

  /**
   * Check password strength without storing
   */
  checkPasswordStrength: async (password) => {
    return fetchAPI('/auth/password/check-strength', {
      method: 'POST',
      body: JSON.stringify(password),
    });
  },

  // ============================================================
  // Email Verification
  // ============================================================

  /**
   * Request email verification (resend verification email)
   */
  requestEmailVerification: async () => {
    return fetchAPI('/auth/email/request-verification', {
      method: 'POST',
    });
  },

  /**
   * Verify email with token
   */
  verifyEmail: async (token) => {
    return fetchAPI('/auth/email/verify', {
      method: 'POST',
      body: JSON.stringify({ token }),
    });
  },

  // ============================================================
  // Session Management
  // ============================================================

  /**
   * List active sessions for current user
   */
  getSessions: async () => {
    return fetchAPI('/auth/sessions');
  },

  /**
   * Revoke a specific session
   */
  revokeSession: async (sessionId) => {
    return fetchAPI(`/auth/sessions/${sessionId}`, {
      method: 'DELETE',
    });
  },
};

/**
 * Cart API
 *
 * P1-5: Updated for cookie-based auth - no longer needs token parameter
 * Still accepts token for backwards compatibility with mobile/API clients
 */
export const cartAPI = {
  get: async (token = null) => {
    const options = {};
    if (token) {
      options.headers = { Authorization: `Bearer ${token}` };
    }
    return fetchAPI('/cart', options);
  },

  addItem: async (token, productId, quantity = 1) => {
    const options = {
      method: 'POST',
      body: JSON.stringify({ product_id: productId, quantity }),
    };
    if (token) {
      options.headers = { Authorization: `Bearer ${token}` };
    }
    return fetchAPI('/cart/items', options);
  },

  updateItem: async (token, itemId, quantity) => {
    const options = {
      method: 'PATCH',
      body: JSON.stringify({ quantity }),
    };
    if (token) {
      options.headers = { Authorization: `Bearer ${token}` };
    }
    return fetchAPI(`/cart/items/${itemId}`, options);
  },

  removeItem: async (token, itemId) => {
    const options = {
      method: 'DELETE',
    };
    if (token) {
      options.headers = { Authorization: `Bearer ${token}` };
    }
    return fetchAPI(`/cart/items/${itemId}`, options);
  },
};

/**
 * Health check
 */
export const healthAPI = {
  check: async () => {
    return fetchAPI('/health');
  },
};

/**
 * Checkout API
 *
 * P1-5: Updated for cookie-based auth
 */
export const checkoutAPI = {
  getConfig: async () => {
    return fetchAPI('/checkout/config');
  },

  createPaymentIntent: async (token, items) => {
    const options = {
      method: 'POST',
      body: JSON.stringify({ items }),
    };
    if (token) {
      options.headers = { Authorization: `Bearer ${token}` };
    }
    return fetchAPI('/checkout/create-payment-intent', options);
  },

  confirmOrder: async (token, paymentIntentId, items) => {
    const options = {
      method: 'POST',
      body: JSON.stringify({ payment_intent_id: paymentIntentId, items }),
    };
    if (token) {
      options.headers = { Authorization: `Bearer ${token}` };
    }
    return fetchAPI('/checkout/confirm-order', options);
  },

  /**
   * Cancel a stock reservation and restore stock
   * Called when user cancels checkout
   */
  cancelReservation: async (paymentIntentId) => {
    return fetchAPI('/checkout/cancel-reservation', {
      method: 'POST',
      body: JSON.stringify(paymentIntentId),
    });
  },
};

/**
 * Funko API
 */
export const funkosAPI = {
  /**
   * Search Funkos by title or series
   */
  search: async ({ q, series, page = 1, per_page = 20 }) => {
    const params = new URLSearchParams();
    if (q) params.append('q', q);
    if (series) params.append('series', series);
    params.append('page', page);
    params.append('per_page', per_page);

    return fetchAPI(`/funkos/search?${params.toString()}`);
  },

  /**
   * Get Funko by ID
   */
  getById: async (id) => {
    return fetchAPI(`/funkos/${id}`);
  },

  /**
   * Get series list
   */
  getSeries: async (q, limit = 50) => {
    const params = new URLSearchParams();
    if (q) params.append('q', q);
    params.append('limit', limit);

    return fetchAPI(`/funkos/series?${params.toString()}`);
  },

  /**
   * Get database stats
   */
  getStats: async () => {
    return fetchAPI('/funkos/stats/count');
  },
};

/**
 * Contact API
 * IMPL-001: About & Contact Page
 */
export const contactAPI = {
  /**
   * Submit contact form
   * @param {Object} formData - { name, email, subject, message }
   * @returns {Promise<{ message: string, reference_id: string }>}
   */
  submit: async (formData) => {
    return fetchAPI('/contact', {
      method: 'POST',
      body: JSON.stringify(formData),
    });
  },
};

/**
 * Shipping API
 * UPS Shipping Integration v1.28.0
 */
export const shippingAPI = {
  // Address management
  createAddress: async (addressData) => {
    return fetchAPI('/shipping/addresses', {
      method: 'POST',
      body: JSON.stringify(addressData),
    });
  },

  getAddresses: async (type = null) => {
    const params = type ? `?address_type=${type}` : '';
    return fetchAPI(`/shipping/addresses${params}`);
  },

  getAddress: async (addressId) => {
    return fetchAPI(`/shipping/addresses/${addressId}`);
  },

  validateAddress: async (addressId) => {
    return fetchAPI(`/shipping/addresses/${addressId}/validate`, {
      method: 'POST',
    });
  },

  deleteAddress: async (addressId) => {
    return fetchAPI(`/shipping/addresses/${addressId}`, {
      method: 'DELETE',
    });
  },

  // Carrier management
  getEnabledCarriers: async () => {
    return fetchAPI('/shipping/carriers');
  },

  // Rate quoting
  getRates: async (destinationAddressId, orderId = null, packages = null, carrierCode = null) => {
    const body = {
      destination_address_id: destinationAddressId,
    };
    if (orderId) body.order_id = orderId;
    if (packages) body.packages = packages;
    if (carrierCode) body.carrier_code = carrierCode;

    return fetchAPI('/shipping/rates', {
      method: 'POST',
      body: JSON.stringify(body),
    });
  },

  getMultiCarrierRates: async (destinationAddressId, orderId = null, packages = null, carrierFilter = null) => {
    const body = {
      destination_address_id: destinationAddressId,
    };
    if (orderId) body.order_id = orderId;
    if (packages) body.packages = packages;
    if (carrierFilter) body.carrier_filter = carrierFilter;

    return fetchAPI('/shipping/rates/multi', {
      method: 'POST',
      body: JSON.stringify(body),
    });
  },

  selectRate: async (quoteId) => {
    return fetchAPI('/shipping/rates/select', {
      method: 'POST',
      body: JSON.stringify({ quote_id: quoteId }),
    });
  },

  // Shipment management
  createShipment: async (shipmentData) => {
    return fetchAPI('/shipping/shipments', {
      method: 'POST',
      body: JSON.stringify(shipmentData),
    });
  },

  getShipments: async (orderId = null, status = null, page = 1, pageSize = 20) => {
    const params = new URLSearchParams();
    if (orderId) params.append('order_id', orderId);
    if (status) params.append('status', status);
    params.append('page', page);
    params.append('page_size', pageSize);

    return fetchAPI(`/shipping/shipments?${params.toString()}`);
  },

  getShipment: async (shipmentId) => {
    return fetchAPI(`/shipping/shipments/${shipmentId}`);
  },

  getLabel: async (shipmentId) => {
    return fetchAPI(`/shipping/shipments/${shipmentId}/label`);
  },

  getTracking: async (shipmentId, refresh = false) => {
    return fetchAPI(`/shipping/shipments/${shipmentId}/tracking?refresh=${refresh}`);
  },

  voidShipment: async (shipmentId) => {
    return fetchAPI(`/shipping/shipments/${shipmentId}/void`, {
      method: 'POST',
    });
  },

  // Public tracking
  trackByNumber: async (trackingNumber) => {
    return fetchAPI(`/shipping/track/${trackingNumber}`);
  },
};


/**
 * Customer Refunds API
 * BCW Refund Request Module v1.0.0
 */
export const refundsAPI = {
  /**
   * Check if a product/order item is eligible for refund
   */
  checkEligibility: async (orderId, orderItemId) => {
    return fetchAPI(`/refunds/eligibility/${orderId}/${orderItemId}`);
  },

  /**
   * Submit a refund request
   */
  createRequest: async (orderId, orderItemId, reason, details = null) => {
    return fetchAPI('/refunds', {
      method: 'POST',
      body: JSON.stringify({
        order_id: orderId,
        order_item_id: orderItemId,
        reason,
        details,
      }),
    });
  },

  /**
   * Get customer's refund requests
   */
  getMyRequests: async () => {
    return fetchAPI('/refunds');
  },

  /**
   * Get a specific refund request
   */
  getRequest: async (requestId) => {
    return fetchAPI(`/refunds/${requestId}`);
  },

  /**
   * Cancel a pending refund request
   */
  cancelRequest: async (requestId) => {
    return fetchAPI(`/refunds/${requestId}/cancel`, {
      method: 'POST',
    });
  },
};

/**
 * Customer Orders API
 * BCW Refund Request Module v1.0.0
 */
export const ordersAPI = {
  /**
   * Get customer's orders
   */
  getMyOrders: async (page = 1, perPage = 20) => {
    const params = new URLSearchParams();
    params.append('page', page);
    params.append('per_page', perPage);
    return fetchAPI(`/orders?${params.toString()}`);
  },

  /**
   * Get a specific order
   */
  getOrder: async (orderId) => {
    return fetchAPI(`/orders/${orderId}`);
  },
};

/**
 * Match Review Queue API (Admin)
 */
export const matchReviewAPI = {
  /**
   * Get queue items with filters
   */
  getQueue: async (filter) => {
    return fetchAPI('/admin/match-queue', {
      method: 'POST',
      body: JSON.stringify(filter),
    });
  },

  /**
   * Get queue statistics
   */
  getStats: async () => {
    return fetchAPI('/admin/match-queue/stats');
  },

  /**
   * Approve a match
   */
  approve: async (matchId, notes = null) => {
    return fetchAPI(`/admin/match-queue/${matchId}/approve`, {
      method: 'POST',
      body: JSON.stringify({ notes }),
    });
  },

  /**
   * Reject a match
   */
  reject: async (matchId, reason, notes = null) => {
    return fetchAPI(`/admin/match-queue/${matchId}/reject`, {
      method: 'POST',
      body: JSON.stringify({ reason, notes }),
    });
  },

  /**
   * Skip a match for later
   */
  skip: async (matchId, notes = null) => {
    return fetchAPI(`/admin/match-queue/${matchId}/skip`, {
      method: 'POST',
      body: JSON.stringify({ notes }),
    });
  },

  /**
   * Bulk approve matches with score >= 8
   */
  bulkApprove: async (matchIds, notes = null) => {
    return fetchAPI('/admin/match-queue/bulk-approve', {
      method: 'POST',
      body: JSON.stringify({ match_ids: matchIds, notes }),
    });
  },

  /**
   * Bulk reject matches with image cleanup
   */
  bulkReject: async (matchIds, reason, notes = null) => {
    return fetchAPI('/admin/match-queue/bulk-reject', {
      method: 'POST',
      body: JSON.stringify({ match_ids: matchIds, reason, notes }),
    });
  },

  /**
   * Manual link entity to PriceCharting
   */
  manualLink: async (entityType, entityId, pricechartingId, notes = null) => {
    return fetchAPI('/admin/match-queue/manual-link', {
      method: 'POST',
      body: JSON.stringify({
        entity_type: entityType,
        entity_id: entityId,
        pricecharting_id: pricechartingId,
        notes,
      }),
    });
  },

  /**
   * Search PriceCharting for manual linking
   */
  search: async (query, entityType) => {
    return fetchAPI('/admin/match-queue/search', {
      method: 'POST',
      body: JSON.stringify({ query, entity_type: entityType }),
    });
  },
};

/**
 * Homepage API
 * CHARLIE-06: Homepage section configuration
 */
export const homepageAPI = {
  /**
   * Get homepage sections configuration
   * Returns ordered, visible sections for the landing page
   */
  getSections: async (options = {}) => {
    const fetchOptions = {};
    if (options.signal) fetchOptions.signal = options.signal;
    return fetchAPI('/homepage/sections', fetchOptions);
  },
};

/**
 * Bundles API
 * CHARLIE-06: Bundle display and cart integration
 */
export const bundlesAPI = {
  /**
   * Get featured bundles for homepage
   * @param {number} limit - Max bundles to return (default 5, max 10)
   * @param {Object} options - Fetch options (signal for abort)
   */
  getFeatured: async (limit = 5, options = {}) => {
    const params = new URLSearchParams();
    params.append('limit', Math.min(limit, 10));

    const fetchOptions = {};
    if (options.signal) fetchOptions.signal = options.signal;

    return fetchAPI(`/bundles/featured?${params.toString()}`, fetchOptions);
  },

  /**
   * Get all active bundles with pagination
   * @param {Object} options - Query options
   */
  getAll: async ({ page = 1, per_page = 20, category, sort, signal } = {}) => {
    const params = new URLSearchParams();
    params.append('page', page);
    params.append('per_page', per_page);
    if (category) params.append('category', category);
    if (sort) params.append('sort', sort);

    const fetchOptions = {};
    if (signal) fetchOptions.signal = signal;

    return fetchAPI(`/bundles?${params.toString()}`, fetchOptions);
  },

  /**
   * Get bundle by slug
   * @param {string} slug - Bundle slug
   */
  getBySlug: async (slug) => {
    return fetchAPI(`/bundles/${slug}`);
  },

  /**
   * Get bundle by ID
   * @param {number} id - Bundle ID
   */
  getById: async (id) => {
    return fetchAPI(`/bundles/id/${id}`);
  },

  /**
   * Add bundle to cart
   * @param {number} bundleId - Bundle ID
   * @param {number} quantity - Quantity (default 1)
   */
  addToCart: async (bundleId, quantity = 1) => {
    return fetchAPI('/cart/bundle', {
      method: 'POST',
      body: JSON.stringify({ bundle_id: bundleId, quantity }),
    });
  },
};

// Re-export middleware API for convenience
export { middlewareAPI, isMiddlewareConfigured } from './middlewareApi.js';

export default {
  comics: comicsAPI,
  products: productsAPI,
  auth: authAPI,
  cart: cartAPI,
  checkout: checkoutAPI,
  health: healthAPI,
  funkos: funkosAPI,
  contact: contactAPI,
  shipping: shippingAPI,
  matchReview: matchReviewAPI,
  refunds: refundsAPI,
  orders: ordersAPI,
  homepage: homepageAPI,
  bundles: bundlesAPI,
};
