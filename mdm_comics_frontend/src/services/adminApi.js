/**
 * Admin API - Product Management
 *
 * P1-5: Updated for cookie-based auth with CSRF protection
 * Cross-origin: Uses stored token for Authorization header when cookies don't work
 *
 * GOVERNANCE: constitution_cyberSec.json Section 5 - Zero cleartext
 * API_BASE imported from centralized config with HTTPS enforcement
 */

import { getStoredToken, clearStoredToken } from './api';
import { API_BASE } from '../config/api.config.js';

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
 * Cross-origin: Also uses stored token for Authorization header
 */
async function fetchAPI(endpoint, options = {}) {
  const url = API_BASE + endpoint;
  const method = options.method || 'GET';

  const headers = {
    'Content-Type': 'application/json',
    ...options.headers,
  };

  // Cross-origin auth: Add Authorization header if token is stored and not already provided
  if (!headers.Authorization) {
    const storedToken = getStoredToken();
    if (storedToken) {
      headers.Authorization = 'Bearer ' + storedToken;
    }
  }

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
    // Clear stored token on auth failure
    if (response.status === 401) {
      clearStoredToken();
    }
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
    // Legacy support or use stored token
    if (token) {
      headers['Authorization'] = 'Bearer ' + token;
    } else {
      const storedToken = getStoredToken();
      if (storedToken) {
        headers['Authorization'] = 'Bearer ' + storedToken;
      }
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

    return fetchAPI('/admin/products/?' + params.toString());
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

  // --- Product Image Management ---
  uploadProductImage: async (productId, file) => {
    const formData = new FormData();
    formData.append('file', file);

    const url = API_BASE + '/admin/products/' + productId + '/image';
    const headers = {};

    const storedToken = getStoredToken();
    if (storedToken) {
      headers['Authorization'] = 'Bearer ' + storedToken;
    }

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
      throw new Error(error.detail || 'Image upload failed');
    }

    return response.json();
  },

  removeProductImage: async (productId) => {
    return fetchAPI('/admin/products/' + productId + '/image', {
      method: 'DELETE',
    });
  },

  uploadGalleryImage: async (productId, file) => {
    const formData = new FormData();
    formData.append('file', file);

    const url = API_BASE + '/admin/products/' + productId + '/gallery';
    const headers = {};

    const storedToken = getStoredToken();
    if (storedToken) {
      headers['Authorization'] = 'Bearer ' + storedToken;
    }

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
      throw new Error(error.detail || 'Gallery upload failed');
    }

    return response.json();
  },

  removeGalleryImage: async (productId, index) => {
    return fetchAPI('/admin/products/' + productId + '/gallery/' + index, {
      method: 'DELETE',
    });
  },

  reorderGalleryImages: async (productId, order) => {
    return fetchAPI('/admin/products/' + productId + '/gallery/reorder', {
      method: 'PUT',
      body: JSON.stringify({ order }),
    });
  },

  bulkClearImages: async (productIds) => {
    return fetchAPI('/admin/products/bulk/clear-images', {
      method: 'POST',
      body: JSON.stringify({ product_ids: productIds }),
    });
  },

  demotePrimaryImage: async (productId) => {
    return fetchAPI('/admin/products/' + productId + '/image/demote', {
      method: 'POST',
    });
  },

  promoteGalleryImage: async (productId, index) => {
    return fetchAPI('/admin/products/' + productId + '/gallery/' + index + '/promote', {
      method: 'POST',
    });
  },

  // --- Barcode Queue ---
  submitBarcodes: async (barcodes) => {
    return fetchAPI('/admin/barcode-queue/', {
      method: 'POST',
      body: JSON.stringify({ barcodes }),
    });
  },

  getBarcodeQueue: async (options = {}) => {
    const params = new URLSearchParams();
    if (options.status) params.set('status', options.status);
    params.set('limit', options.limit || 50);
    params.set('offset', options.offset || 0);

    return fetchAPI('/admin/barcode-queue/?' + params.toString());
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

    return fetchAPI('/admin/orders/?' + params.toString());
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

  getPriceChanges: async (days = 1, thresholdPct = 2, limit = 500) => {
    return fetchAPI('/admin/reports/price-changes?days=' + days + '&threshold_pct=' + thresholdPct + '&limit=' + limit);
  },

  getEntityDetails: async (entityType, entityId) => {
    return fetchAPI('/admin/reports/entity/' + entityType + '/' + entityId);
  },

  // --- Dashboard ---
  getDashboard: async () => {
    return fetchAPI('/admin/dashboard');
  },

  // ==================== USER MANAGEMENT ====================

  getUsers: async (options = {}) => {
    const params = new URLSearchParams();
    if (options.search) params.set('search', options.search);
    if (options.isAdmin !== undefined) params.set('is_admin', options.isAdmin);
    if (options.isActive !== undefined) params.set('is_active', options.isActive);
    if (options.sort) params.set('sort', options.sort);
    params.set('limit', options.limit || 25);
    params.set('offset', options.offset || 0);

    return fetchAPI('/admin/users/?' + params.toString());
  },

  getUser: async (userId) => {
    return fetchAPI('/admin/users/' + userId);
  },

  createUser: async (userData) => {
    return fetchAPI('/admin/users/', {
      method: 'POST',
      body: JSON.stringify(userData),
    });
  },

  updateUser: async (userId, updateData) => {
    return fetchAPI('/admin/users/' + userId, {
      method: 'PATCH',
      body: JSON.stringify(updateData),
    });
  },

  deleteUser: async (userId) => {
    return fetchAPI('/admin/users/' + userId, {
      method: 'DELETE',
    });
  },

  toggleUserAdmin: async (userId) => {
    return fetchAPI('/admin/users/' + userId + '/toggle-admin', {
      method: 'POST',
    });
  },

  // ==================== BRAND ASSET MANAGEMENT v1.0.0 ====================

  // --- Assets ---
  getAssets: async (options = {}) => {
    const params = new URLSearchParams();
    if (options.assetType) params.set('asset_type', options.assetType);
    if (options.includeDeleted) params.set('include_deleted', 'true');
    params.set('limit', options.limit || 50);
    params.set('offset', options.offset || 0);

    return fetchAPI('/admin/assets/?' + params.toString());
  },

  getAsset: async (assetId) => {
    return fetchAPI('/admin/assets/' + assetId);
  },

  uploadAsset: async (file, name, assetType, settingKey = null) => {
    const formData = new FormData();
    formData.append('file', file);
    formData.append('name', name);
    formData.append('asset_type', assetType);
    if (settingKey) formData.append('setting_key', settingKey);

    const url = API_BASE + '/admin/assets/upload';
    const headers = {};

    // Cross-origin auth
    const storedToken = getStoredToken();
    if (storedToken) {
      headers['Authorization'] = 'Bearer ' + storedToken;
    }

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
      throw new Error(error.detail || 'Upload failed');
    }

    return response.json();
  },

  updateAsset: async (assetId, updateData) => {
    return fetchAPI('/admin/assets/' + assetId, {
      method: 'PATCH',
      body: JSON.stringify(updateData),
    });
  },

  deleteAsset: async (assetId) => {
    return fetchAPI('/admin/assets/' + assetId, {
      method: 'DELETE',
    });
  },

  restoreAsset: async (assetId) => {
    return fetchAPI('/admin/assets/' + assetId + '/restore', {
      method: 'POST',
    });
  },

  getAssetVersion: async (assetId, version) => {
    return fetchAPI('/admin/assets/' + assetId + '/versions/' + version);
  },

  // --- Settings ---
  getSettings: async (category = null) => {
    const params = new URLSearchParams();
    if (category) params.set('category', category);
    return fetchAPI('/admin/settings/?' + params.toString());
  },

  getSetting: async (key) => {
    return fetchAPI('/admin/settings/' + key);
  },

  updateSetting: async (key, value, description = null) => {
    const body = { value };
    if (description) body.description = description;
    return fetchAPI('/admin/settings/' + key, {
      method: 'PUT',
      body: JSON.stringify(body),
    });
  },

  createSetting: async (data) => {
    return fetchAPI('/admin/settings/', {
      method: 'POST',
      body: JSON.stringify(data),
    });
  },

  deleteSetting: async (key) => {
    return fetchAPI('/admin/settings/' + key, {
      method: 'DELETE',
    });
  },

  bulkUpdateSettings: async (settings) => {
    return fetchAPI('/admin/settings/bulk', {
      method: 'POST',
      body: JSON.stringify({ settings }),
    });
  },

  // --- Public branding (no auth) ---
  getPublicBranding: async () => {
    return fetchAPI('/admin/settings/public/branding');
  },

  // ==================== PIPELINE MANAGEMENT ====================

  // Get GCD import status
  getGCDStatus: async () => {
    return fetchAPI('/admin/pipeline/gcd/status');
  },

  // Get PriceCharting matching status
  getPriceChartingStatus: async () => {
    return fetchAPI('/admin/pipeline/pricecharting/status');
  },

  // Trigger PriceCharting matching job
  triggerPriceChartingMatch: async (options = {}) => {
    return fetchAPI('/admin/pipeline/pricecharting/match', {
      method: 'POST',
      body: JSON.stringify({
        batch_size: options.batch_size || 500,
        max_records: options.max_records || 0,
      }),
    });
  },

  // Reset stale PriceCharting checkpoints (fixes 409 conflicts)
  resetPriceChartingCheckpoints: async () => {
    return fetchAPI('/admin/pipeline/pricecharting/reset', {
      method: 'POST',
    });
  },

  // Trigger GCD import
  triggerGCDImport: async (options = {}) => {
    return fetchAPI('/admin/pipeline/gcd/import', {
      method: 'POST',
      body: JSON.stringify({
        max_records: options.max_records || 0,
        batch_size: options.batch_size || 5000,
      }),
    });
  },

  // Reset GCD checkpoint (starts from offset 0 - use for full re-import)
  resetGCDCheckpoint: async () => {
    return fetchAPI('/admin/pipeline/gcd/reset-checkpoint', {
      method: 'POST',
    });
  },

  // Clear stale lock (keeps offset, just clears is_running flag)
  clearGCDStaleLock: async () => {
    return fetchAPI('/admin/pipeline/gcd/clear-stale-lock', {
      method: 'POST',
    });
  },

  // Sync offset to actual DB count (skips already-imported records)
  syncGCDOffset: async () => {
    return fetchAPI('/admin/pipeline/gcd/sync-offset', {
      method: 'POST',
    });
  },

  // Validate GCD SQLite dump schema
  validateGCDDump: async () => {
    return fetchAPI('/admin/pipeline/gcd/validate');
  },

  // Get all pipeline checkpoints
  getPipelineCheckpoints: async () => {
    return fetchAPI('/admin/pipeline/checkpoints');
  },

  // Clear a specific pipeline checkpoint
  clearPipelineCheckpoint: async (jobName) => {
    return fetchAPI('/admin/pipeline/checkpoints/' + jobName + '/clear', {
      method: 'POST',
    });
  },

  // Get pipeline stats
  getPipelineStats: async () => {
    return fetchAPI('/admin/pipeline/stats');
  },

  // Get pipeline metrics dashboard data (v1.24.0)
  getPipelineMetrics: async () => {
    return fetchAPI('/admin/pipeline/metrics');
  },

  // Get UPC backfill status
  getUPCBackfillStatus: async () => {
    return fetchAPI('/admin/pipeline/upc-backfill/status');
  },

  // Trigger UPC backfill job
  triggerUPCBackfill: async (options = {}) => {
    return fetchAPI('/admin/pipeline/upc-backfill/run', {
      method: 'POST',
      body: JSON.stringify({
        batch_size: options.batch_size || 100,
        max_records: options.max_records || 0,
      }),
    });
  },

  // Get Sequential Enrichment (MSE) status
  getSequentialEnrichmentStatus: async () => {
    return fetchAPI('/admin/pipeline/sequential-enrichment/status');
  },

  // Trigger Sequential Enrichment (MSE) job
  triggerSequentialEnrichment: async (options = {}) => {
    return fetchAPI('/admin/pipeline/sequential-enrichment/run', {
      method: 'POST',
      body: JSON.stringify({
        batch_size: options.batch_size || 100,
        max_records: options.max_records || 0,
      }),
    });
  },

  // ==================== JOB CONTROL (v1.20.0) ====================

  // Pause a running job - saves checkpoint immediately
  pauseJob: async (jobName) => {
    return fetchAPI('/admin/data-health/jobs/' + jobName + '/pause', {
      method: 'POST',
    });
  },

  // Stop a running job - saves checkpoint, releases lock
  stopJob: async (jobName) => {
    return fetchAPI('/admin/data-health/jobs/' + jobName + '/stop', {
      method: 'POST',
    });
  },

  // Start/resume a job
  startJob: async (jobName) => {
    return fetchAPI('/admin/data-health/jobs/' + jobName + '/start', {
      method: 'POST',
    });
  },

  // Get detailed job status including control signal state
  getJobStatus: async (jobName) => {
    return fetchAPI('/admin/data-health/jobs/' + jobName + '/status');
  },


  // ==================== ADMIN SHIPPING MANAGEMENT ====================

  // Get shipping label for a shipment
  getShippingLabel: async (shipmentId) => {
    return fetchAPI('/shipping/shipments/' + shipmentId + '/label');
  },

  // Void a shipment (admin only)
  voidShipment: async (shipmentId) => {
    return fetchAPI('/shipping/shipments/' + shipmentId + '/void', {
      method: 'POST',
    });
  },

  // Get all shipments (admin view with filters)
  getShipments: async (options = {}) => {
    const params = new URLSearchParams();
    if (options.orderId) params.set('order_id', options.orderId);
    if (options.status) params.set('status', options.status);
    params.set('page', options.page || 1);
    params.set('page_size', options.pageSize || 20);

    return fetchAPI('/shipping/shipments/?' + params.toString());
  },

  // Create shipment for an order (admin)
  createShipment: async (shipmentData) => {
    return fetchAPI('/shipping/shipments', {
      method: 'POST',
      body: JSON.stringify(shipmentData),
    });
  },

  // ==================== BUNDLE BUILDER v1.0.0 ====================

  getBundles: async (options = {}) => {
    const params = new URLSearchParams();
    if (options.search) params.set('search', options.search);
    if (options.status) params.set('status', options.status);
    if (options.category) params.set('category', options.category);
    params.set('page', options.page || 1);
    params.set('per_page', options.per_page || 25);

    return fetchAPI('/admin/bundles/?' + params.toString());
  },

  getBundle: async (bundleId) => {
    return fetchAPI('/admin/bundles/' + bundleId);
  },

  createBundle: async (bundleData) => {
    return fetchAPI('/admin/bundles/', {
      method: 'POST',
      body: JSON.stringify(bundleData),
    });
  },

  updateBundle: async (bundleId, updateData) => {
    return fetchAPI('/admin/bundles/' + bundleId, {
      method: 'PUT',
      body: JSON.stringify(updateData),
    });
  },

  deleteBundle: async (bundleId) => {
    return fetchAPI('/admin/bundles/' + bundleId, {
      method: 'DELETE',
    });
  },

  // Bundle items
  addBundleItem: async (bundleId, itemData) => {
    return fetchAPI('/admin/bundles/' + bundleId + '/items', {
      method: 'POST',
      body: JSON.stringify(itemData),
    });
  },

  updateBundleItem: async (bundleId, itemId, itemData) => {
    return fetchAPI('/admin/bundles/' + bundleId + '/items/' + itemId, {
      method: 'PUT',
      body: JSON.stringify(itemData),
    });
  },

  removeBundleItem: async (bundleId, itemId) => {
    return fetchAPI('/admin/bundles/' + bundleId + '/items/' + itemId, {
      method: 'DELETE',
    });
  },

  // Bundle lifecycle
  publishBundle: async (bundleId) => {
    return fetchAPI('/admin/bundles/' + bundleId + '/publish', {
      method: 'POST',
    });
  },

  unpublishBundle: async (bundleId) => {
    return fetchAPI('/admin/bundles/' + bundleId + '/unpublish', {
      method: 'POST',
    });
  },

  archiveBundle: async (bundleId) => {
    return fetchAPI('/admin/bundles/' + bundleId + '/archive', {
      method: 'POST',
    });
  },

  duplicateBundle: async (bundleId) => {
    return fetchAPI('/admin/bundles/' + bundleId + '/duplicate', {
      method: 'POST',
    });
  },

  uploadBundleImage: async (file, bundleId = null) => {
    const formData = new FormData();
    formData.append('file', file);
    if (bundleId) {
      formData.append('bundle_id', bundleId);
    }

    const url = API_BASE + '/admin/bundles/upload-image';
    const headers = {};

    const storedToken = getStoredToken();
    if (storedToken) {
      headers['Authorization'] = 'Bearer ' + storedToken;
    }

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
      throw new Error(error.detail || 'Bundle image upload failed');
    }

    return response.json();
  },

  // Pricing preview
  calculateBundlePricing: async (items) => {
    return fetchAPI('/admin/bundles/calculate-pricing', {
      method: 'POST',
      body: JSON.stringify({ items }),
    });
  },

  // ==================== COVER INGESTION ====================

  // Preview cover ingestion from a folder
  previewCoverIngestion: async (folderPath, limit = 100) => {
    return fetchAPI('/admin/cover-ingestion/preview', {
      method: 'POST',
      body: JSON.stringify({
        folder_path: folderPath,
        limit: limit,
      }),
    });
  },

  // Ingest covers from a folder (queues to Match Review)
  ingestCovers: async (options) => {
    return fetchAPI('/admin/cover-ingestion/ingest', {
      method: 'POST',
      body: JSON.stringify({
        folder_path: options.folderPath,
        limit: options.limit,
      }),
    });
  },

  // Ingest single cover (queues to Match Review)
  ingestSingleCover: async (filePath, basePath) => {
    return fetchAPI('/admin/cover-ingestion/single', {
      method: 'POST',
      body: JSON.stringify({
        file_path: filePath,
        base_path: basePath,
      }),
    });
  },

  // Get cover ingestion statistics
  getCoverIngestionStats: async () => {
    return fetchAPI('/admin/cover-ingestion/stats');
  },

  // Upload cover from browser (v1.22.0)
  uploadCover: async (file, metadata = {}) => {
    const formData = new FormData();
    formData.append('file', file);
    if (metadata.publisher) formData.append('publisher', metadata.publisher);
    if (metadata.series) formData.append('series', metadata.series);
    if (metadata.volume) formData.append('volume', metadata.volume);
    if (metadata.issue_number) formData.append('issue_number', metadata.issue_number);
    if (metadata.variant_code) formData.append('variant_code', metadata.variant_code);
    if (metadata.cgc_grade) formData.append('cgc_grade', metadata.cgc_grade);

    const url = API_BASE + '/admin/cover-ingestion/upload';
    const headers = {};

    // Cross-origin auth
    const storedToken = getStoredToken();
    if (storedToken) {
      headers['Authorization'] = 'Bearer ' + storedToken;
    }

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
      throw new Error(error.detail || 'Upload failed');
    }

    return response.json();
  },

  // Update cover for existing queue item (v1.22.0)
  updateCover: async (queueId, file) => {
    const formData = new FormData();
    formData.append('file', file);

    const url = API_BASE + '/admin/cover-ingestion/update/' + queueId;
    const headers = {};

    // Cross-origin auth
    const storedToken = getStoredToken();
    if (storedToken) {
      headers['Authorization'] = 'Bearer ' + storedToken;
    }

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
      throw new Error(error.detail || 'Update failed');
    }

    return response.json();
  },

  // ==================== SYSTEM STATUS ====================

  /**
   * Get real system health status (database, jobs, adapters)
   */
  getSystemStatus: async () => {
    return fetchAPI('/admin/data-health/overview');
  },

  /**
   * Get pipeline job statuses
   */
  getPipelineJobs: async () => {
    return fetchAPI('/admin/data-health/pipeline/jobs');
  },

  // ==================== BCW CATALOG v1.0.0 ====================

  searchBCWCatalog: async (options = {}) => {
    const params = new URLSearchParams();
    if (options.q) params.set('q', options.q);
    if (options.category) params.set('category', options.category);
    if (options.in_catalog !== undefined) params.set('in_catalog', options.in_catalog);
    if (options.in_stock !== undefined) params.set('in_stock', options.in_stock);
    params.set('page', options.page || 1);
    params.set('per_page', options.per_page || 20);

    return fetchAPI('/admin/bcw/catalog/search?' + params.toString());
  },

  getBCWProduct: async (mdmSku) => {
    return fetchAPI('/admin/bcw/catalog/' + mdmSku);
  },

  updateBCWPricing: async (mdmSku, pricingData) => {
    return fetchAPI('/admin/bcw/catalog/' + mdmSku + '/pricing', {
      method: 'PATCH',
      body: JSON.stringify(pricingData),
    });
  },

  activateBCWProduct: async (mdmSku, price) => {
    return fetchAPI('/admin/bcw/catalog/' + mdmSku + '/activate?price=' + price, {
      method: 'POST',
    });
  },

  syncBCWInventory: async (skus = null) => {
    return fetchAPI('/admin/bcw/catalog/sync-inventory', {
      method: 'POST',
      body: JSON.stringify({ skus }),
    });
  },

  getBCWCategories: async () => {
    return fetchAPI('/admin/bcw/catalog/categories');
  },
};

export default adminAPI;
