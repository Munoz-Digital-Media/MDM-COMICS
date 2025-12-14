/**
 * useProducts hook - Fetches products from backend API
 *
 * FE-004: Replace static PRODUCTS array with API calls
 * 
 * Performance & Stability Improvements:
 * - Uses AbortController to cancel requests on unmount (prevents memory leaks)
 * - Proper cleanup on component unmount
 * - Falls back to empty array on error to prevent UI crashes
 *
 * This hook provides:
 * - products: Array of products from API
 * - loading: Boolean loading state
 * - error: Error message if fetch failed
 * - refetch: Function to manually refetch products
 */
import { useState, useEffect, useCallback, useRef } from 'react';
import { productsAPI } from '../services/api';

// OPT-001: Removed hardcoded FALLBACK_PRODUCTS - use empty array instead
// This prevents data duplication and stale fallback data
// Users will see loading state or empty state with retry option instead

/**
 * Transform backend product data to frontend format
 */
function transformProduct(product) {
  return {
    id: product.id,
    name: product.name,
    category: product.category || 'comics',
    subcategory: product.subcategory || product.category || 'Other',
    price: parseFloat(product.price) || 0,
    originalPrice: product.original_price ? parseFloat(product.original_price) : null,
    image: product.image_url || product.image || `https://placehold.co/400x500/27272a/f59e0b?text=$` + `{encodeURIComponent(product.name?.charAt(0) || '?')}`,
    description: product.description || '',
    stock: product.stock ?? 0,
    featured: product.featured ?? false,
    rating: product.rating ?? 4.0,
    tags: product.tags || [],
    sku: product.sku
  };
}

export function useProducts() {
  const [products, setProducts] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  
  // Track if component is mounted to prevent state updates after unmount
  const mountedRef = useRef(true);
  // Track current abort controller for cleanup
  const abortControllerRef = useRef(null);

  const fetchProducts = useCallback(async () => {
    // Cancel any in-flight request
    if (abortControllerRef.current) {
      abortControllerRef.current.abort();
    }
    
    // Create new abort controller for this request
    const abortController = new AbortController();
    abortControllerRef.current = abortController;
    
    setLoading(true);
    setError(null);

    try {
      const data = await productsAPI.getAll({ signal: abortController.signal });

      // Only update state if component is still mounted and request wasn't aborted
      if (!mountedRef.current || abortController.signal.aborted) {
        return;
      }

      // API returns { products: [], total, page, per_page }
      const productsList = Array.isArray(data) ? data : (data.products || []);
      const transformed = productsList.map(transformProduct);

      setProducts(transformed);
      setError(null);
    } catch (err) {
      // Ignore abort errors - they're intentional
      if (err.name === 'AbortError') {
        return;
      }
      
      // Only update state if component is still mounted
      if (!mountedRef.current) {
        return;
      }
      
      console.error('Failed to fetch products:', err);
      setError(err.message || 'Failed to load products');
      // OPT-001: Use empty array on error - no stale fallback data
      // User will see error state with option to retry
      setProducts([]);
    } finally {
      // Only update loading state if component is still mounted
      if (mountedRef.current) {
        setLoading(false);
      }
    }
  }, []);

  useEffect(() => {
    mountedRef.current = true;
    fetchProducts();

    // Cleanup on unmount
    return () => {
      mountedRef.current = false;
      if (abortControllerRef.current) {
        abortControllerRef.current.abort();
      }
    };
  }, [fetchProducts]);

  return {
    products,
    loading,
    error,
    refetch: fetchProducts
  };
}

export default useProducts;
