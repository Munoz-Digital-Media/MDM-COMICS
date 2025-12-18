/**
 * Auth-aware fetch helper for fulfillment module
 * Includes stored token in Authorization header
 */
import { getStoredToken } from '../../../../services/api';

export async function authFetch(url, options = {}) {
  const token = getStoredToken();
  const headers = {
    ...options.headers,
  };

  if (token) {
    headers['Authorization'] = `Bearer ${token}`;
  }

  // Only set Content-Type if not FormData
  if (!(options.body instanceof FormData)) {
    headers['Content-Type'] = 'application/json';
  }

  return fetch(url, {
    ...options,
    headers,
    credentials: 'include',
  });
}
