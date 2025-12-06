/**
 * Admin API - Product Management
 *
 * P1-5: Updated for cookie-based auth with CSRF protection
 */

const API_BASE = import.meta.env.VITE_API_URL || 'http://localhost:8080/api';
const CSRF_COOKIE_NAME = 'mdm_csrf_token';

/**
 * Get CSRF token from cookie
 * HIGH-005: Fixed split to handle '=' in cookie value
 */
function getCsrfToken() {
  const cookies = document.cookie.split(';');
  for (const cookie of cookies) {
    const trimmed = cookie.trim();
    const eqIndex = trimmed.indexOf('=');
    if (eqIndex === -1) continue;
    const name = trimmed.substring(0, eqIndex);
    const value = trimmed.substring(eqIndex + 1);
    if (name === CSRF_COOKIE_NAME) {
      return decodeURIComponent(value);
    }
  }
  return null;
}

/**
 * P1-5: Updated fetch wrapper with cookie auth and CSRF
 */
async function fetchAPI(endpoint, options = {}) {
  const url = API_BASE + endpoint;
  const method = options.method || 'GET';

  const headers = {
    'Content-Type': 'application/json',
    ...options.headers,
  };

  // P1-5: Add CSRF token for mutations
  if (method !== 'GET' && method !== 'HEAD' && method !== 'OPTIONS') {
    const csrfToken = getCsrfToken();
    if (csrfToken) {
      headers['X-CSRF-Token'] = csrfToken;
    }
  }

  const response = await fetch(url, {
    ...options,
    headers,
    // P1-5: Include cookies
    credentials: 'include',
  });

  if (!response.ok) {
    const error = await response.json().catch(() => ({}));
    throw new Error(error.detail || 'API Error: ' + response.status);
  }

  if (response.status === 204) {
    return null;
  }

  return response.json();
}

export const adminAPI = {
  // P1-5: Token parameter kept for backwards compatibility but not needed
  createProduct: async (token, productData) => {
    const options = {
      method: 'POST',
      body: JSON.stringify(productData),
    };
    // Legacy support: if token provided, use it
    if (token) {
      options.headers = { Authorization: 'Bearer ' + token };
    }
    return fetchAPI('/products', options);
  },

  updateProduct: async (token, productId, updateData) => {
    const options = {
      method: 'PATCH',
      body: JSON.stringify(updateData),
    };
    if (token) {
      options.headers = { Authorization: 'Bearer ' + token };
    }
    return fetchAPI('/products/' + productId, options);
  },

  deleteProduct: async (token, productId) => {
    const options = {
      method: 'DELETE',
    };
    if (token) {
      options.headers = { Authorization: 'Bearer ' + token };
    }
    return fetchAPI('/products/' + productId, options);
  },

  getProducts: async (options = {}) => {
    const page = options.page || 1;
    const per_page = options.per_page || 20;
    const search = options.search || '';

    let url = '/products?page=' + page + '&per_page=' + per_page;
    if (search) url += '&search=' + encodeURIComponent(search);

    return fetchAPI(url);
  },

  searchByImage: async (token, formData) => {
    const url = API_BASE + '/comics/search-by-image';

    const headers = {};
    // Legacy support
    if (token) {
      headers['Authorization'] = 'Bearer ' + token;
    }

    // P1-5: Add CSRF token for mutations
    const csrfToken = getCsrfToken();
    if (csrfToken) {
      headers['X-CSRF-Token'] = csrfToken;
    }

    const response = await fetch(url, {
      method: 'POST',
      headers,
      body: formData,
      credentials: 'include',
    });

    if (!response.ok) {
      const error = await response.json().catch(() => ({}));
      throw new Error(error.detail || 'Image search failed');
    }

    return response.json();
  },
};

export default adminAPI;
