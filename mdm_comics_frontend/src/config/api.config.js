/**
 * Centralized API Configuration
 *
 * Same-origin proxy: Frontend proxies /api/* to backend.
 * No more cross-origin issues, no more mixed content.
 */

// Detect if we're on localhost (dev mode with Vite proxy)
const isLocalhost = typeof window !== 'undefined' && 
  (window.location.hostname === 'localhost' || window.location.hostname === '127.0.0.1');

// Use relative /api path - works for both:
// - Localhost: Vite dev server proxies to backend
// - Production: Express server proxies to backend
export const API_BASE = '/api';

// Middleware also proxied (if needed)
export const MIDDLEWARE_BASE = isLocalhost
  ? 'http://localhost:8001'
  : '/middleware';

export const ENVIRONMENT = isLocalhost ? 'development' : 'staging';

// Debug logging
if (typeof window !== 'undefined') {
  console.log('[API Config] Same-origin proxy - API_BASE:', API_BASE);
}
