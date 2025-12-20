/**
 * useFeaturedBundles hook - Fetches featured bundles for homepage
 *
 * CHARLIE-03: Provides featured bundles for the Bundles section
 *
 * This hook provides:
 * - bundles: Array of featured bundles
 * - loading: Boolean loading state
 * - error: Error message if fetch failed
 * - refetch: Function to manually refetch bundles
 */
import { useState, useEffect, useCallback, useRef } from 'react';
import { bundlesAPI } from '../services/api';

// Cache duration in milliseconds (5 minutes)
const CACHE_DURATION = 5 * 60 * 1000;

// Module-level cache
let cachedBundles = null;
let cacheTimestamp = 0;

/**
 * Transform backend bundle data to frontend format
 */
function transformBundle(bundle) {
  return {
    id: bundle.id,
    slug: bundle.slug,
    name: bundle.name,
    short_description: bundle.short_description || '',
    image_url: bundle.image_url || `https://placehold.co/400x500/27272a/f59e0b?text=${encodeURIComponent('🎁')}`,
    bundle_price: parseFloat(bundle.bundle_price) || 0,
    compare_at_price: bundle.compare_at_price ? parseFloat(bundle.compare_at_price) : null,
    savings_percent: bundle.savings_percent || 0,
    badge_text: bundle.badge_text || null,
    available_qty: bundle.available_qty ?? 0,
    item_count: bundle.item_count || 0,
    category: bundle.category,
  };
}

export function useFeaturedBundles(limit = 5) {
  const [bundles, setBundles] = useState(() => {
    // Initialize from cache if valid
    if (cachedBundles && Date.now() - cacheTimestamp < CACHE_DURATION) {
      return cachedBundles.slice(0, limit);
    }
    return [];
  });
  const [loading, setLoading] = useState(!cachedBundles);
  const [error, setError] = useState(null);

  // Track if component is mounted
  const mountedRef = useRef(true);
  const abortControllerRef = useRef(null);

  const fetchBundles = useCallback(async (forceRefresh = false) => {
    // Use cache if valid and not forcing refresh
    if (!forceRefresh && cachedBundles && Date.now() - cacheTimestamp < CACHE_DURATION) {
      setBundles(cachedBundles.slice(0, limit));
      setLoading(false);
      return;
    }

    // Cancel any in-flight request
    if (abortControllerRef.current) {
      abortControllerRef.current.abort();
    }

    const abortController = new AbortController();
    abortControllerRef.current = abortController;

    setLoading(true);
    setError(null);

    try {
      const data = await bundlesAPI.getFeatured(limit, { signal: abortController.signal });

      if (!mountedRef.current || abortController.signal.aborted) {
        return;
      }

      const bundlesList = Array.isArray(data) ? data : (data.bundles || []);
      const transformed = bundlesList.map(transformBundle);

      // Update cache
      cachedBundles = transformed;
      cacheTimestamp = Date.now();

      setBundles(transformed);
      setError(null);
    } catch (err) {
      if (err.name === 'AbortError') {
        return;
      }

      if (!mountedRef.current) {
        return;
      }

      console.error('Failed to fetch featured bundles:', err);
      setError(err.message || 'Failed to load bundles');
      setBundles([]);
    } finally {
      if (mountedRef.current) {
        setLoading(false);
      }
    }
  }, [limit]);

  useEffect(() => {
    mountedRef.current = true;
    fetchBundles();

    return () => {
      mountedRef.current = false;
      if (abortControllerRef.current) {
        abortControllerRef.current.abort();
      }
    };
  }, [fetchBundles]);

  return {
    bundles,
    loading,
    error,
    refetch: () => fetchBundles(true)
  };
}

export default useFeaturedBundles;
