// Centralized API Configuration

// Use environment variable for API_BASE if available, otherwise default to /api for same-origin proxy
// This allows full control over the API base URL in deployed environments.
export const API_BASE = import.meta.env.VITE_API_BASE_URL
  ? import.meta.env.VITE_API_BASE_URL.replace(/^http:\/\//i, 'https://') // Ensure HTTPS
  : '/api';

// Middleware also proxied (if needed)
const isLocalhost = typeof window !== 'undefined' && 
  (window.location.hostname === 'localhost' || window.location.hostname === '127.0.0.1');

export const MIDDLEWARE_BASE = import.meta.env.VITE_MIDDLEWARE_URL
  ? import.meta.env.VITE_MIDDLEWARE_URL.replace(/^http:\/\//i, 'https://')
  : (isLocalhost ? 'http://localhost:8001' : '/middleware');

export const ENVIRONMENT = isLocalhost ? 'development' : 'staging';

// Debug logging
if (typeof window !== 'undefined') {
  console.log('[API Config] API_BASE:', API_BASE);
  console.log('[API Config] MIDDLEWARE_BASE:', MIDDLEWARE_BASE);
}
