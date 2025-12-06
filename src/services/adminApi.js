/**
 * Admin API - Product Management
 *
 * P1-5: Updated for cookie-based auth with CSRF protection
 */

// HIGH-004 FIX: Changed default port from 8080 to 8000 (backend runs on 8000)
const API_BASE = import.meta.env.VITE_API_URL || 'http://localhost:8000/api';
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
  // ==================== PRODUCT MANAGEMENT ====================
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

  // ==================== ADMIN INVENTORY SYSTEM v1.3.0 ====================

  // --- Admin Products ---
  getAdminProducts: async (options = {}) => {
    const params = new URLSearchParams();
    if (options.search) params.set('search', options.search);
    if (options.category) params.set('category', options.category);
    if (options.lowStock) params.set('low_stock', 'true');
    if (options.includeDeleted) params.set('include_deleted', 'true');
    if (options.sort) params.set('sort', options.sort);
    params.set('limit', options.limit || 25);
    params.set('offset', options.offset || 0);

    return fetchAPI('/admin/products?' + params.toString());
  },

  adjustStock: async (productId, data) => {
    return fetchAPI('/admin/products/' + productId + '/adjust-stock', {
      method: 'POST',
      body: JSON.stringify(data),
    });
  },

  getStockHistory: async (productId) => {
    return fetchAPI('/admin/products/' + productId + '/stock-history');
  },

  restoreProduct: async (productId) => {
    return fetchAPI('/admin/products/' + productId + '/restore', {
      method: 'POST',
    });
  },

  // --- Barcode Queue ---
  submitBarcodes: async (barcodes) => {
    return fetchAPI('/admin/barcode-queue', {
      method: 'POST',
      body: JSON.stringify({ barcodes }),
    });
  },

  getBarcodeQueue: async (options = {}) => {
    const params = new URLSearchParams();
    if (options.status) params.set('status', options.status);
    params.set('limit', options.limit || 50);
    params.set('offset', options.offset || 0);

    return fetchAPI('/admin/barcode-queue?' + params.toString());
  },

  processQueueItem: async (queueId, action, productData = null) => {
    return fetchAPI('/admin/barcode-queue/' + queueId + '/process', {
      method: 'POST',
      body: JSON.stringify({ action, product_data: productData }),
    });
  },

  batchProcessQueue: async (ids, action, defaultStock = 1) => {
    return fetchAPI('/admin/barcode-queue/batch-process', {
      method: 'POST',
      body: JSON.stringify({ ids, action, default_stock: defaultStock }),
    });
  },

  // --- Orders ---
  getOrders: async (options = {}) => {
    const params = new URLSearchParams();
    if (options.status) params.set('status', options.status);
    if (options.search) params.set('search', options.search);
    if (options.dateFrom) params.set('date_from', options.dateFrom);
    if (options.dateTo) params.set('date_to', options.dateTo);
    params.set('limit', options.limit || 25);
    params.set('offset', options.offset || 0);

    return fetchAPI('/admin/orders?' + params.toString());
  },

  getOrder: async (orderId) => {
    return fetchAPI('/admin/orders/' + orderId);
  },

  updateOrderStatus: async (orderId, status, trackingNumber = null) => {
    const body = { status };
    if (trackingNumber) body.tracking_number = trackingNumber;
    return fetchAPI('/admin/orders/' + orderId + '/status', {
      method: 'PATCH',
      body: JSON.stringify(body),
    });
  },

  // --- Reports ---
  getInventorySummary: async () => {
    return fetchAPI('/admin/reports/inventory-summary');
  },

  getLowStockItems: async (threshold = 5) => {
    return fetchAPI('/admin/reports/low-stock?threshold=' + threshold);
  },

  getPriceChanges: async (days = 7, thresholdPct = 10) => {
    return fetchAPI('/admin/reports/price-changes?days=' + days + '&threshold_pct=' + thresholdPct);
  },

  // --- Dashboard ---
  getDashboard: async () => {
    return fetchAPI('/admin/dashboard');
  },
};

export default adminAPI;
