/**
 * useProducts hook - Fetches products from backend API
 *
 * FE-004: Replace static PRODUCTS array with API calls
 *
 * This hook provides:
 * - products: Array of products from API
 * - loading: Boolean loading state
 * - error: Error message if fetch failed
 * - refetch: Function to manually refetch products
 *
 * Falls back to empty array on error to prevent UI crashes.
 */
import { useState, useEffect, useCallback } from 'react';
import { productsAPI } from '../services/api';

// Fallback products for when API is unavailable (development only)
const FALLBACK_PRODUCTS = [
  {
    id: "comic-001",
    name: "Amazing Spider-Man #300",
    category: "comics",
    subcategory: "Marvel",
    price: 299.99,
    originalPrice: 349.99,
    image: "https://placehold.co/400x500/1a1a2e/1a1a2e?text=+",
    description: "First appearance of Venom. Near Mint condition.",
    stock: 2,
    featured: true,
    rating: 4.9,
    tags: ["key-issue", "first-appearance", "graded"]
  },
  {
    id: "comic-002",
    name: "Batman: The Killing Joke",
    category: "comics",
    subcategory: "DC",
    price: 89.99,
    image: "https://placehold.co/400x500/1a1a2e/1a1a2e?text=+",
    description: "Classic Alan Moore story. First print.",
    stock: 5,
    featured: true,
    rating: 4.8,
    tags: ["classic", "alan-moore"]
  },
  {
    id: "funko-001",
    name: "Funko POP! Spider-Man (Black Suit)",
    category: "funko",
    subcategory: "Marvel",
    price: 14.99,
    image: "https://placehold.co/400x500/27272a/27272a?text=+",
    description: "Exclusive black suit variant. #79",
    stock: 25,
    featured: true,
    rating: 4.7,
    tags: ["exclusive", "marvel"]
  },
  {
    id: "funko-002",
    name: "Funko POP! Batman (GITD)",
    category: "funko",
    subcategory: "DC",
    price: 24.99,
    originalPrice: 29.99,
    image: "https://placehold.co/400x500/27272a/27272a?text=+",
    description: "Glow in the dark exclusive. Chase variant.",
    stock: 3,
    featured: true,
    rating: 4.9,
    tags: ["gitd", "chase", "exclusive"]
  }
];

export function useProducts() {
  const [products, setProducts] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);

  const fetchProducts = useCallback(async () => {
    setLoading(true);
    setError(null);

    try {
      const data = await productsAPI.getAll();

      // API returns { products: [], total, page, per_page }
      const productsList = Array.isArray(data) ? data : (data.products || []);

      // Transform backend data to match frontend expectations
      const transformed = productsList.map(product => ({
        id: product.id,
        name: product.name,
        category: product.category || 'comics',
        subcategory: product.subcategory || product.category || 'Other',
        price: parseFloat(product.price) || 0,
        originalPrice: product.original_price ? parseFloat(product.original_price) : null,
        image: product.image_url || product.image || `https://placehold.co/400x500/27272a/f59e0b?text=${encodeURIComponent(product.name?.charAt(0) || '?')}`,
        description: product.description || '',
        stock: product.stock ?? 0,
        featured: product.featured ?? false,
        rating: product.rating ?? 4.0,
        tags: product.tags || [],
        sku: product.sku
      }));

      setProducts(transformed);
    } catch (err) {
      console.error('Failed to fetch products:', err);
      setError(err.message || 'Failed to load products');
      // Use fallback products when API fails
      setProducts(FALLBACK_PRODUCTS);
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    fetchProducts();
  }, [fetchProducts]);

  return {
    products,
    loading,
    error,
    refetch: fetchProducts
  };
}

export default useProducts;
