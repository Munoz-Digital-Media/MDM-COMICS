/**
 * Admin API - Product Management
 */

const API_BASE = import.meta.env.VITE_API_URL || 'http://localhost:8080/api';

async function fetchAPI(endpoint, options = {}) {
  const url = API_BASE + endpoint;

  const response = await fetch(url, {
    headers: {
      'Content-Type': 'application/json',
      ...options.headers,
    },
    ...options,
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
  createProduct: async (token, productData) => {
    return fetchAPI('/products', {
      method: 'POST',
      headers: { Authorization: 'Bearer ' + token },
      body: JSON.stringify(productData),
    });
  },

  updateProduct: async (token, productId, updateData) => {
    return fetchAPI('/products/' + productId, {
      method: 'PATCH',
      headers: { Authorization: 'Bearer ' + token },
      body: JSON.stringify(updateData),
    });
  },

  deleteProduct: async (token, productId) => {
    return fetchAPI('/products/' + productId, {
      method: 'DELETE',
      headers: { Authorization: 'Bearer ' + token },
    });
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
    const response = await fetch(url, {
      method: 'POST',
      headers: {
        'Authorization': 'Bearer ' + token,
        // Note: Don't set Content-Type for FormData - browser sets it with boundary
      },
      body: formData,
    });

    if (!response.ok) {
      const error = await response.json().catch(() => ({}));
      throw new Error(error.detail || 'Image search failed');
    }

    return response.json();
  },
};

export default adminAPI;
