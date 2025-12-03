/**
 * MDM Comics API Service
 * Handles all communication with the backend
 */

const API_BASE = import.meta.env.VITE_API_URL || 'http://localhost:8080/api';

/**
 * Generic fetch wrapper with error handling
 */
async function fetchAPI(endpoint, options = {}) {
  const url = `${API_BASE}${endpoint}`;

  try {
    const response = await fetch(url, {
      headers: {
        'Content-Type': 'application/json',
        ...options.headers,
      },
      ...options,
    });

    if (!response.ok) {
      throw new Error(`API Error: ${response.status} ${response.statusText}`);
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
   * Search for comics by series name, issue number, publisher, or year
   */
  search: async ({ series, number, publisher, year, page = 1 }) => {
    const params = new URLSearchParams();
    if (series) params.append('series', series);
    if (number) params.append('number', number);
    if (publisher) params.append('publisher', publisher);
    if (year) params.append('year', year);
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
    return fetchAPI(`/products/search?q=${encodeURIComponent(query)}`);
  },
};

/**
 * Auth API
 */
export const authAPI = {
  login: async (email, password) => {
    return fetchAPI('/auth/login', {
      method: 'POST',
      body: JSON.stringify({ email, password }),
    });
  },

  register: async (name, email, password) => {
    return fetchAPI('/auth/register', {
      method: 'POST',
      body: JSON.stringify({ name, email, password }),
    });
  },

  me: async (token) => {
    return fetchAPI('/auth/me', {
      headers: {
        Authorization: `Bearer ${token}`,
      },
    });
  },
};

/**
 * Cart API
 */
export const cartAPI = {
  get: async (token) => {
    return fetchAPI('/cart', {
      headers: { Authorization: `Bearer ${token}` },
    });
  },

  addItem: async (token, productId, quantity = 1) => {
    return fetchAPI('/cart/items', {
      method: 'POST',
      headers: { Authorization: `Bearer ${token}` },
      body: JSON.stringify({ product_id: productId, quantity }),
    });
  },

  updateItem: async (token, itemId, quantity) => {
    return fetchAPI(`/cart/items/${itemId}`, {
      method: 'PATCH',
      headers: { Authorization: `Bearer ${token}` },
      body: JSON.stringify({ quantity }),
    });
  },

  removeItem: async (token, itemId) => {
    return fetchAPI(`/cart/items/${itemId}`, {
      method: 'DELETE',
      headers: { Authorization: `Bearer ${token}` },
    });
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

export default {
  comics: comicsAPI,
  products: productsAPI,
  auth: authAPI,
  cart: cartAPI,
  health: healthAPI,
};
