/**
 * Centralized API Configuration
 *
 * GOVERNANCE: constitution_cyberSec.json Section 5
 * "Zero cleartext; HTTP -> 308 to HTTPS enforced"
 *
 * HARDCODED HTTPS URLs - No more environment variable bullshit.
 */

// Detect if we're on localhost
const isLocalhost = typeof window !== 'undefined' && 
  (window.location.hostname === 'localhost' || window.location.hostname === '127.0.0.1');

// HARDCODED URLs - guaranteed HTTPS for non-localhost
export const API_BASE = isLocalhost 
  ? 'http://localhost:8000/api'
  : 'https://mdm-comics-backend-development.up.railway.app/api';

export const MIDDLEWARE_BASE = isLocalhost
  ? 'http://localhost:8001'
  : 'https://mdm-comics-middleware-development.up.railway.app';

export const ENVIRONMENT = isLocalhost ? 'development' : 'staging';

// Debug logging
if (typeof window !== 'undefined') {
  console.log('[API Config] HARDCODED - API_BASE:', API_BASE);
}
