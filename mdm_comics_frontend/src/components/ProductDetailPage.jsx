/**
 * ProductDetailPage Component
 *
 * Full product view for customers with image, description, pricing, and add-to-cart.
 * Also used in admin preview mode with cart actions disabled.
 *
 * @compliance WCAG 2.2 AA - Keyboard navigation, focus indicators, aria labels
 * @compliance constitution_ui.json - <250ms hydration, feedback within 500ms
 */
import React, { useState, useCallback } from 'react';
import {
  ArrowLeft, Star, Plus, Minus, ShoppingCart,
  Package, Truck, Shield, ZoomIn, X, AlertCircle
} from 'lucide-react';

// Image zoom modal component
function ImageZoomModal({ src, alt, onClose }) {
  return (
    <div
      className="fixed inset-0 z-50 flex items-center justify-center bg-black/90"
      onClick={onClose}
      onKeyDown={(e) => e.key === 'Escape' && onClose()}
      role="dialog"
      aria-modal="true"
      aria-label="Zoomed product image"
    >
      <button
        onClick={onClose}
        className="absolute top-4 right-4 p-2 bg-zinc-800 rounded-full hover:bg-zinc-700 transition-colors focus:outline-none focus-visible:ring-2 focus-visible:ring-orange-500"
        aria-label="Close zoom"
      >
        <X className="w-6 h-6 text-white" />
      </button>
      <img
        src={src}
        alt={alt}
        className="max-w-[90vw] max-h-[90vh] object-contain"
        onClick={(e) => e.stopPropagation()}
      />
    </div>
  );
}

export default function ProductDetailPage({
  product,
  onBack,
  onAddToCart,
  previewMode = false
}) {
  const [quantity, setQuantity] = useState(1);
  const [imageLoaded, setImageLoaded] = useState(false);
  const [imageError, setImageError] = useState(false);
  const [showZoom, setShowZoom] = useState(false);
  const [addedFeedback, setAddedFeedback] = useState(false);

  const handleQuantityChange = useCallback((delta) => {
    setQuantity(prev => {
      const newQty = prev + delta;
      if (newQty < 1) return 1;
      if (product.stock && newQty > product.stock) return product.stock;
      return newQty;
    });
  }, [product.stock]);

  const handleAddToCart = useCallback(() => {
    if (previewMode || !onAddToCart) return;

    // Add each item individually to match App.jsx cart logic
    for (let i = 0; i < quantity; i++) {
      onAddToCart(product);
    }

    // Show feedback
    setAddedFeedback(true);
    setTimeout(() => setAddedFeedback(false), 2000);

    // Reset quantity
    setQuantity(1);
  }, [product, quantity, onAddToCart, previewMode]);

  const handleImageError = useCallback(() => {
    setImageError(true);
    setImageLoaded(true);
  }, []);

  const placeholderImage = `https://placehold.co/600x800/27272a/f59e0b?text=${encodeURIComponent(
    product.category === 'comics' || product.category === 'bagged-boarded' || product.category === 'graded'
      ? 'Comic'
      : product.category === 'funko'
        ? 'Funko'
        : 'Product'
  )}`;

  const imageUrl = imageError ? placeholderImage : (product.image || product.image_url || placeholderImage);

  // Stock status
  const inStock = product.stock > 0;
  const lowStock = product.stock > 0 && product.stock <= 5;

  return (
    <div className="min-h-screen bg-zinc-950">
      {/* Preview Mode Banner */}
      {previewMode && (
        <div className="bg-amber-500/90 text-black px-4 py-2 text-center text-sm font-semibold">
          PREVIEW MODE - This is how customers will see this product
        </div>
      )}

      <div className="max-w-6xl mx-auto px-4 py-6 md:py-12">
        {/* Back Button */}
        <button
          onClick={onBack}
          className="flex items-center gap-2 text-orange-500 hover:text-orange-400 mb-6 transition-colors focus:outline-none focus-visible:ring-2 focus-visible:ring-orange-500 focus-visible:ring-offset-2 focus-visible:ring-offset-zinc-950 rounded"
          aria-label="Go back to previous page"
        >
          <ArrowLeft className="w-5 h-5" />
          <span className="font-medium">Back</span>
        </button>

        {/* Main Content Grid */}
        <div className="grid grid-cols-1 md:grid-cols-2 gap-8 lg:gap-12">
          {/* Image Section */}
          <div className="relative">
            {/* Image Container */}
            <div
              className="relative aspect-[3/4] bg-zinc-900 rounded-2xl overflow-hidden border border-zinc-800 cursor-zoom-in group"
              onClick={() => !previewMode && setShowZoom(true)}
              onKeyDown={(e) => e.key === 'Enter' && !previewMode && setShowZoom(true)}
              tabIndex={previewMode ? -1 : 0}
              role={previewMode ? undefined : "button"}
              aria-label={previewMode ? undefined : "Click to zoom image"}
            >
              {/* Skeleton loader */}
              {!imageLoaded && (
                <div className="absolute inset-0 bg-zinc-800 animate-pulse" />
              )}

              <img
                src={imageUrl}
                alt={product.name}
                className={`w-full h-full object-cover transition-all duration-300 ${
                  imageLoaded ? 'opacity-100' : 'opacity-0'
                } group-hover:scale-105`}
                onLoad={() => setImageLoaded(true)}
                onError={handleImageError}
              />

              {/* Zoom indicator */}
              {!previewMode && imageLoaded && (
                <div className="absolute bottom-4 right-4 p-2 bg-black/60 rounded-lg opacity-0 group-hover:opacity-100 transition-opacity">
                  <ZoomIn className="w-5 h-5 text-white" />
                </div>
              )}

              {/* Badges */}
              <div className="absolute top-4 left-4 flex flex-col gap-2">
                {product.featured && (
                  <span className="px-3 py-1.5 bg-orange-500 rounded-full text-xs font-bold text-white shadow-lg">
                    FEATURED
                  </span>
                )}
                {product.originalPrice && (
                  <span className="px-3 py-1.5 bg-green-600 rounded-full text-xs font-bold text-white shadow-lg">
                    SALE
                  </span>
                )}
                {lowStock && (
                  <span className="px-3 py-1.5 bg-red-600 rounded-full text-xs font-bold text-white shadow-lg">
                    {product.stock} left
                  </span>
                )}
              </div>
            </div>
          </div>

          {/* Details Section */}
          <div className="flex flex-col">
            {/* Category Badge */}
            <span className="inline-flex w-fit px-3 py-1 bg-orange-500/20 text-orange-400 rounded-full text-xs font-semibold uppercase tracking-wide mb-3">
              {product.subcategory || product.category}
            </span>

            {/* Title */}
            <h1 className="text-2xl md:text-3xl lg:text-4xl font-bold text-white mb-4 leading-tight">
              {product.name}
            </h1>

            {/* Rating */}
            {product.rating && (
              <div className="flex items-center gap-2 mb-4" aria-label={`Rating: ${product.rating} out of 5 stars`}>
                <div className="flex items-center gap-1">
                  {[1, 2, 3, 4, 5].map((star) => (
                    <Star
                      key={star}
                      className={`w-5 h-5 ${
                        star <= Math.round(product.rating)
                          ? 'fill-orange-500 text-orange-500'
                          : 'fill-zinc-700 text-zinc-700'
                      }`}
                    />
                  ))}
                </div>
                <span className="text-zinc-400 text-sm">({product.rating})</span>
              </div>
            )}

            {/* Price */}
            <div className="flex items-baseline gap-3 mb-6">
              <span className="text-3xl md:text-4xl font-bold text-white">
                ${product.price?.toFixed(2)}
              </span>
              {product.originalPrice && (
                <span className="text-xl text-zinc-500 line-through">
                  ${product.originalPrice.toFixed(2)}
                </span>
              )}
              {product.originalPrice && (
                <span className="px-2 py-0.5 bg-green-500/20 text-green-400 text-sm font-semibold rounded">
                  Save ${(product.originalPrice - product.price).toFixed(2)}
                </span>
              )}
            </div>

            {/* Stock Status */}
            <div className="flex items-center gap-2 mb-6">
              {inStock ? (
                <>
                  <div className={`w-2.5 h-2.5 rounded-full ${lowStock ? 'bg-yellow-500' : 'bg-green-500'}`} />
                  <span className={`text-sm font-medium ${lowStock ? 'text-yellow-400' : 'text-green-400'}`}>
                    {lowStock ? `Only ${product.stock} left in stock` : 'In Stock'}
                  </span>
                </>
              ) : (
                <>
                  <div className="w-2.5 h-2.5 rounded-full bg-red-500" />
                  <span className="text-sm font-medium text-red-400">Out of Stock</span>
                </>
              )}
            </div>

            {/* Description */}
            <p className="text-zinc-400 leading-relaxed mb-8">
              {product.description || 'No description available.'}
            </p>

            {/* Add to Cart Section */}
            <div className="space-y-4">
              {/* Quantity Selector */}
              <div className="flex items-center gap-4">
                <span className="text-sm text-zinc-400">Quantity:</span>
                <div className="flex items-center bg-zinc-900 border border-zinc-800 rounded-lg">
                  <button
                    onClick={() => handleQuantityChange(-1)}
                    disabled={quantity <= 1 || previewMode}
                    className="p-3 hover:bg-zinc-800 transition-colors disabled:opacity-50 disabled:cursor-not-allowed focus:outline-none focus-visible:ring-2 focus-visible:ring-orange-500 focus-visible:ring-inset rounded-l-lg"
                    aria-label="Decrease quantity"
                  >
                    <Minus className="w-4 h-4 text-zinc-400" />
                  </button>
                  <span className="w-12 text-center text-white font-semibold" aria-live="polite">
                    {quantity}
                  </span>
                  <button
                    onClick={() => handleQuantityChange(1)}
                    disabled={quantity >= product.stock || previewMode}
                    className="p-3 hover:bg-zinc-800 transition-colors disabled:opacity-50 disabled:cursor-not-allowed focus:outline-none focus-visible:ring-2 focus-visible:ring-orange-500 focus-visible:ring-inset rounded-r-lg"
                    aria-label="Increase quantity"
                  >
                    <Plus className="w-4 h-4 text-zinc-400" />
                  </button>
                </div>
              </div>

              {/* Add to Cart Button */}
              <button
                onClick={handleAddToCart}
                disabled={!inStock || previewMode}
                className={`w-full py-4 rounded-xl font-bold text-lg flex items-center justify-center gap-3 transition-all focus:outline-none focus-visible:ring-2 focus-visible:ring-orange-500 focus-visible:ring-offset-2 focus-visible:ring-offset-zinc-950 ${
                  !inStock || previewMode
                    ? 'bg-zinc-800 text-zinc-500 cursor-not-allowed'
                    : addedFeedback
                      ? 'bg-green-600 text-white'
                      : 'bg-orange-500 text-white hover:bg-orange-600 hover:shadow-lg hover:shadow-orange-500/25 active:scale-[0.98]'
                }`}
                aria-live="polite"
              >
                {addedFeedback ? (
                  <>
                    <Package className="w-5 h-5" />
                    Added to Cart!
                  </>
                ) : previewMode ? (
                  <>
                    <AlertCircle className="w-5 h-5" />
                    Cart Disabled in Preview
                  </>
                ) : !inStock ? (
                  'Out of Stock'
                ) : (
                  <>
                    <ShoppingCart className="w-5 h-5" />
                    Add to Cart - ${(product.price * quantity).toFixed(2)}
                  </>
                )}
              </button>
            </div>

            {/* Trust Badges */}
            <div className="grid grid-cols-3 gap-4 mt-8 pt-8 border-t border-zinc-800">
              <div className="flex flex-col items-center text-center">
                <div className="w-10 h-10 bg-zinc-900 rounded-full flex items-center justify-center mb-2">
                  <Truck className="w-5 h-5 text-orange-500" />
                </div>
                <span className="text-xs text-zinc-400">Free Shipping $50+</span>
              </div>
              <div className="flex flex-col items-center text-center">
                <div className="w-10 h-10 bg-zinc-900 rounded-full flex items-center justify-center mb-2">
                  <Package className="w-5 h-5 text-orange-500" />
                </div>
                <span className="text-xs text-zinc-400">Secure Packaging</span>
              </div>
              <div className="flex flex-col items-center text-center">
                <div className="w-10 h-10 bg-zinc-900 rounded-full flex items-center justify-center mb-2">
                  <Shield className="w-5 h-5 text-orange-500" />
                </div>
                <span className="text-xs text-zinc-400">Buyer Protection</span>
              </div>
            </div>

            {/* Product Details */}
            {(product.sku || product.category || product.tags) && (
              <div className="mt-8 pt-8 border-t border-zinc-800 space-y-2">
                <h3 className="text-sm font-semibold text-zinc-300 mb-3">Product Details</h3>
                {product.sku && (
                  <div className="flex justify-between text-sm">
                    <span className="text-zinc-500">SKU</span>
                    <span className="text-zinc-300 font-mono">{product.sku}</span>
                  </div>
                )}
                {product.category && (
                  <div className="flex justify-between text-sm">
                    <span className="text-zinc-500">Category</span>
                    <span className="text-zinc-300 capitalize">{product.category}</span>
                  </div>
                )}
                {product.tags && product.tags.length > 0 && (
                  <div className="flex justify-between text-sm">
                    <span className="text-zinc-500">Tags</span>
                    <span className="text-zinc-300">{product.tags.join(', ')}</span>
                  </div>
                )}
              </div>
            )}
          </div>
        </div>
      </div>

      {/* Zoom Modal */}
      {showZoom && (
        <ImageZoomModal
          src={imageUrl}
          alt={product.name}
          onClose={() => setShowZoom(false)}
        />
      )}
    </div>
  );
}
