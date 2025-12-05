/**
 * MDM Comics API Service
 * Handles all communication with the backend
 *
 * P1-5: Updated for cookie-based auth with CSRF protection
 * - Uses credentials: 'include' to send cookies
 * - Reads CSRF token from cookie and sends in header
 * - Falls back to header-based auth for compatibility
 */

// Use production API in production, localhost for dev
const API_BASE = import.meta.env.VITE_API_URL ||
  (window.location.hostname === 'localhost' ? 'http://localhost:8080/api' : 'https://api.mdmcomics.com/api');

// CSRF token cookie name
const CSRF_COOKIE_NAME = 'mdm_csrf_token';

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

  // P1-5: Add CSRF token for mutations (non-GET requests)
  if (method !== 'GET' && method !== 'HEAD' && method !== 'OPTIONS') {
    const csrfToken = getCsrfToken();
    if (csrfToken) {
      headers['X-CSRF-Token'] = csrfToken;
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
      const error = await response.json().catch(() => ({}));
      throw new Error(error.detail || `API Error: ${response.status}`);
    }

    // Handle 204 No Content
    if (response.status === 204) {
      return null;
    }

    return await response.json();
  } catch (error) {
    console.error(`API call failed: ${endpoint}`, error);
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
 */
export const productsAPI = {
  getAll: async () => {
    return fetchAPI('/products');
  },

  getById: async (id) => {
    return fetchAPI(`/products/${id}`);
  },

  search: async (query) => {
    return fetchAPI(`/products?search=${encodeURIComponent(query)}`);
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
    // access_token is still returned for backwards compatibility
    return result;
  },

  register: async (name, email, password) => {
    const result = await fetchAPI('/auth/register', {
      method: 'POST',
      body: JSON.stringify({ name, email, password }),
    });
    // P1-5: Server sets HttpOnly cookies automatically
    return result;
  },

  logout: async () => {
    // P1-5: New endpoint that clears HttpOnly cookies
    return fetchAPI('/auth/logout', {
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

export default {
  comics: comicsAPI,
  products: productsAPI,
  auth: authAPI,
  cart: cartAPI,
  checkout: checkoutAPI,
  health: healthAPI,
  funkos: funkosAPI,
};
