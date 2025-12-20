/**
 * useHomepageSections hook - Fetches homepage section configuration
 *
 * CHARLIE-02: Provides configurable section ordering and visibility
 *
 * This hook provides:
 * - sections: Array of section configs sorted by display_order, filtered to visible
 * - loading: Boolean loading state
 * - error: Error message if fetch failed
 * - refetch: Function to manually refetch sections
 */
import { useState, useEffect, useCallback, useRef } from 'react';
import { homepageAPI } from '../services/api';

// Default sections - fallback if API fails
const DEFAULT_SECTIONS = [
  {
    id: 'section-1',
    key: 'bagged-boarded',
    title: 'Bagged & Boarded Books',
    emoji: '📚',
    visible: true,
    display_order: 1,
    max_items: 5,
    category_link: '/shop/bagged-boarded',
    data_source: 'products'
  },
  {
    id: 'section-2',
    key: 'graded',
    title: 'Graded Books',
    emoji: '🏆',
    visible: true,
    display_order: 2,
    max_items: 5,
    category_link: '/shop/graded',
    data_source: 'products'
  },
  {
    id: 'section-3',
    key: 'funko',
    title: 'Funko POPs',
    emoji: '🎭',
    visible: true,
    display_order: 3,
    max_items: 5,
    category_link: '/shop/funko',
    data_source: 'products'
  },
  {
    id: 'section-4',
    key: 'supplies',
    title: 'Supplies',
    emoji: '📦',
    visible: true,
    display_order: 4,
    max_items: 5,
    category_link: '/shop/supplies',
    data_source: 'products'
  },
  {
    id: 'section-5',
    key: 'bundles',
    title: 'Bundles',
    emoji: '🎁',
    visible: true,
    display_order: 5,
    max_items: 5,
    category_link: '/shop/bundles',
    data_source: 'bundles'
  },
];

// Cache duration in milliseconds (5 minutes)
const CACHE_DURATION = 5 * 60 * 1000;

// Module-level cache
let cachedSections = null;
let cacheTimestamp = 0;

export function useHomepageSections() {
  const [sections, setSections] = useState(() => {
    // Initialize from cache if valid
    if (cachedSections && Date.now() - cacheTimestamp < CACHE_DURATION) {
      return cachedSections;
    }
    return DEFAULT_SECTIONS;
  });
  const [loading, setLoading] = useState(!cachedSections);
  const [error, setError] = useState(null);

  // Track if component is mounted
  const mountedRef = useRef(true);
  const abortControllerRef = useRef(null);

  const fetchSections = useCallback(async (forceRefresh = false) => {
    // Use cache if valid and not forcing refresh
    if (!forceRefresh && cachedSections && Date.now() - cacheTimestamp < CACHE_DURATION) {
      setSections(cachedSections);
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
      const data = await homepageAPI.getSections({ signal: abortController.signal });

      if (!mountedRef.current || abortController.signal.aborted) {
        return;
      }

      // Filter to visible sections and sort by display_order
      const visibleSections = (data.sections || [])
        .filter(s => s.visible)
        .sort((a, b) => a.display_order - b.display_order);

      // Update cache
      cachedSections = visibleSections;
      cacheTimestamp = Date.now();

      setSections(visibleSections);
      setError(null);
    } catch (err) {
      if (err.name === 'AbortError') {
        return;
      }

      if (!mountedRef.current) {
        return;
      }

      console.error('Failed to fetch homepage sections:', err);
      setError(err.message || 'Failed to load sections');
      // Use defaults on error
      setSections(DEFAULT_SECTIONS.filter(s => s.visible).sort((a, b) => a.display_order - b.display_order));
    } finally {
      if (mountedRef.current) {
        setLoading(false);
      }
    }
  }, []);

  useEffect(() => {
    mountedRef.current = true;
    fetchSections();

    return () => {
      mountedRef.current = false;
      if (abortControllerRef.current) {
        abortControllerRef.current.abort();
      }
    };
  }, [fetchSections]);

  return {
    sections,
    loading,
    error,
    refetch: () => fetchSections(true)
  };
}

export default useHomepageSections;
