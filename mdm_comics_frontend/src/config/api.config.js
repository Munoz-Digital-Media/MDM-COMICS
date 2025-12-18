/**
 * Centralized API Configuration
 *
 * GOVERNANCE: constitution_cyberSec.json Section 5
 * "Zero cleartext; HTTP -> 308 to HTTPS enforced"
 *
 * This is the SINGLE SOURCE OF TRUTH for API base URLs.
 * All components must import from here - no inline URL resolution.
 */

// Environment-specific defaults (fallbacks if env vars not set at build time)
const ENV_DEFAULTS = {
  development: {
    API_URL: 'http://localhost:8000/api',
    MIDDLEWARE_URL: 'http://localhost:8001',
  },
  staging: {
    API_URL: 'https://mdm-comics-backend-development.up.railway.app/api',
    MIDDLEWARE_URL: 'https://mdm-comics-middleware-development.up.railway.app',
  },
  production: {
    API_URL: 'https://api.mdmcomics.com/api',
    MIDDLEWARE_URL: 'https://middleware.mdmcomics.com',
  },
};

/**
 * Detect current environment
 */
function detectEnvironment() {
  // Check for localhost first
  if (typeof window !== 'undefined') {
    const hostname = window.location.hostname;
    if (hostname === 'localhost' || hostname === '127.0.0.1') {
      return 'development';
    }
    // Check hostname for staging vs production
    if (hostname.includes('development') || hostname.includes('staging')) {
      return 'staging';
    }
  }

  // Build-time environment from Vite
  if (import.meta.env.MODE === 'production') {
    return 'production';
  }

  return 'development';
}

/**
 * Enforce HTTPS for non-localhost URLs
 * GOVERNANCE: constitution_cyberSec.json Section 5
 */
function enforceHttps(url) {
  if (!url) return url;

  // Allow HTTP for localhost development
  if (url.includes('localhost') || url.includes('127.0.0.1')) {
    return url;
  }

  // Force HTTPS for all other URLs
  if (url.startsWith('http://')) {
    return url.replace('http://', 'https://');
  }

  return url;
}

/**
 * Resolve API base URL with HTTPS enforcement
 */
function resolveApiBase() {
  const env = detectEnvironment();
  const defaults = ENV_DEFAULTS[env] || ENV_DEFAULTS.development;

  // Priority 1: Build-time environment variable
  let baseUrl = import.meta.env.VITE_API_URL;

  // Priority 2: Environment-specific default
  if (!baseUrl) {
    baseUrl = defaults.API_URL;
  }

  // Always enforce HTTPS for non-localhost (regardless of page protocol)
  return enforceHttps(baseUrl);
}

/**
 * Resolve Middleware base URL with HTTPS enforcement
 */
function resolveMiddlewareBase() {
  const env = detectEnvironment();
  const defaults = ENV_DEFAULTS[env] || ENV_DEFAULTS.development;

  let baseUrl = import.meta.env.VITE_MIDDLEWARE_URL;

  if (!baseUrl) {
    baseUrl = defaults.MIDDLEWARE_URL;
  }

  return enforceHttps(baseUrl);
}

// Exported constants - computed once at module load
export const API_BASE = resolveApiBase();
export const MIDDLEWARE_BASE = resolveMiddlewareBase();
export const ENVIRONMENT = detectEnvironment();

// Debug logging (only in development)
if (import.meta.env.DEV) {
  console.log('[API Config] Environment:', ENVIRONMENT);
  console.log('[API Config] API_BASE:', API_BASE);
  console.log('[API Config] MIDDLEWARE_BASE:', MIDDLEWARE_BASE);
}
