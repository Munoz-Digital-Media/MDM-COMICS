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
  Package, Truck, Shield, ZoomIn, X, AlertCircle, Ruler
} from 'lucide-react';
import RefundPolicyBadge from './RefundPolicyBadge';

// Format decimal dimension to fraction string (e.g., 3.375 -> "3 ⅜")
// Returns null for 0, null, or undefined values (treat 0 as no data)
const formatDimension = (value) => {
  if (!value || parseFloat(value) === 0) return null;
  const num = parseFloat(value);
  const whole = Math.floor(num);
  const frac = num - whole;

  const fractionMap = {
    0: '',
    0.125: '⅛',
    0.25: '¼',
    0.375: '⅜',
    0.5: '½',
    0.625: '⅝',
    0.75: '¾',
    0.875: '⅞',
  };

  // Find closest fraction
  const closest = Object.keys(fractionMap).reduce((prev, curr) =>
    Math.abs(parseFloat(curr) - frac) < Math.abs(parseFloat(prev) - frac) ? curr : prev
  );

  const fracStr = fractionMap[closest];
  if (whole === 0 && fracStr) return fracStr + '"';
  if (!fracStr) return whole + '"';
  return `${whole} ${fracStr}"`;
};

// Format W x H x L dimensions - only shows dimensions with valid data
// Returns null if all dimensions are empty/zero
const formatDimensions = (w, h, l) => {
  const dims = [
    formatDimension(w),
    formatDimension(h),
    formatDimension(l)
  ].filter(Boolean);

  if (dims.length === 0) return null;
  return dims.join(' × ');
};

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
  const [selectedImageIndex, setSelectedImageIndex] = useState(0);
  const feedbackTimeoutRef = React.useRef(null);

  // Build array of all product images
  const allImages = React.useMemo(() => {
    const primary = product.image || product.image_url;
    const gallery = product.images || [];

    // DEBUG: Log image data to identify issues
    if (import.meta.env.DEV) {
      console.log('[ProductDetailPage] Image data:', {
        productId: product.id,
        image: product.image,
        image_url: product.image_url,
        images: product.images,
        imagesLength: gallery.length
      });
    }

    if (!primary) return [];
    // Primary first, then gallery images (deduplicated)
    return [primary, ...gallery.filter(img => img !== primary)];
  }, [product.id, product.image, product.image_url, product.images]);

  // Cleanup feedback timeout on unmount
  React.useEffect(() => {
    return () => {
      if (feedbackTimeoutRef.current) {
        clearTimeout(feedbackTimeoutRef.current);
      }
    };
  }, []);

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

    // Show feedback with proper cleanup
    setAddedFeedback(true);
    if (feedbackTimeoutRef.current) {
      clearTimeout(feedbackTimeoutRef.current);
    }
    feedbackTimeoutRef.current = setTimeout(() => setAddedFeedback(false), 2000);

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

  // Use selected image from gallery, or fallback to placeholder
  const currentImage = allImages[selectedImageIndex] || allImages[0];
  const imageUrl = imageError ? placeholderImage : (currentImage || placeholderImage);

  // Reset image loaded state when changing images
  const handleImageSelect = useCallback((index) => {
    if (index !== selectedImageIndex) {
      setSelectedImageIndex(index);
      setImageLoaded(false);
      setImageError(false);
    }
  }, [selectedImageIndex]);

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

            {/* Image Thumbnails Gallery */}
            {allImages.length > 1 && (
              <div className="mt-4 flex gap-2 overflow-x-auto pb-2 scrollbar-thin scrollbar-thumb-zinc-700">
                {allImages.map((img, index) => (
                  <button
                    key={img}
                    onClick={() => handleImageSelect(index)}
                    className={`flex-shrink-0 w-16 h-16 rounded-lg overflow-hidden border-2 transition-all ${
                      selectedImageIndex === index
                        ? 'border-orange-500 ring-2 ring-orange-500/30'
                        : 'border-zinc-700 hover:border-zinc-500'
                    }`}
                    aria-label={`View image ${index + 1} of ${allImages.length}`}
                  >
                    <img
                      src={img}
                      alt={`${product.name} - view ${index + 1}`}
                      className="w-full h-full object-cover"
                      loading="lazy"
                      onError={(e) => {
                        e.target.onerror = null;
                        e.target.src = 'data:image/svg+xml,%3Csvg xmlns="http://www.w3.org/2000/svg" width="64" height="64"%3E%3Crect fill="%2327272a" width="64" height="64"/%3E%3Ctext fill="%2371717a" x="50%25" y="50%25" text-anchor="middle" dy=".3em" font-size="10"%3E?%3C/text%3E%3C/svg%3E';
                      }}
                    />
                  </button>
                ))}
              </div>
            )}

            {/* Product Details - Under Image Gallery */}
            <div className="mt-6 pt-6 border-t border-zinc-800 space-y-2">
              <h3 className="text-sm font-semibold text-zinc-300 mb-3">Product Details</h3>
              {product.category && (
                <div className="flex justify-between text-sm">
                  <span className="text-zinc-500">Category</span>
                  <span className="text-zinc-300 capitalize">{product.category}</span>
                </div>
              )}
              {product.upc && (
                <div className="flex justify-between text-sm">
                  <span className="text-zinc-500">UPC</span>
                  <span className="text-zinc-300 font-mono">{product.upc}</span>
                </div>
              )}
              {product.sku && (
                <div className="flex justify-between text-sm">
                  <span className="text-zinc-500">SKU</span>
                  <span className="text-zinc-300 font-mono">{product.sku}</span>
                </div>
              )}
              {/* Dimensions */}
              {(() => {
                const dims = formatDimensions(product.interior_width, product.interior_height, product.interior_length);
                return dims ? (
                  <div className="flex justify-between text-sm">
                    <span className="text-zinc-500">Interior Dimensions</span>
                    <span className="text-zinc-300">{dims}</span>
                  </div>
                ) : null;
              })()}
              {(() => {
                const dims = formatDimensions(product.exterior_width, product.exterior_height, product.exterior_length);
                return dims ? (
                  <div className="flex justify-between text-sm">
                    <span className="text-zinc-500">Exterior Dimensions</span>
                    <span className="text-zinc-300">{dims}</span>
                  </div>
                ) : null;
              })()}
              {product.weight && (
                <div className="flex justify-between text-sm">
                  <span className="text-zinc-500">Weight</span>
                  <span className="text-zinc-300">{product.weight}</span>
                </div>
              )}
              {product.material && (
                <div className="flex justify-between text-sm">
                  <span className="text-zinc-500">Material</span>
                  <span className="text-zinc-300">{product.material}</span>
                </div>
              )}
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
            <p className="text-zinc-400 leading-relaxed mb-8 whitespace-pre-line">
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

          </div>
        </div>

        {/* Return Policy - Full Width, Below Product Details, Above Shipping */}
        <div className="mt-12 pt-8 border-t border-zinc-800">
          <h3 className="text-sm font-semibold text-zinc-300 mb-3">Return Policy</h3>
          <RefundPolicyBadge product={product} showDetails />
        </div>

        {/* Shipping Banner - Full Width Moat */}
        <div className="mt-12 py-8 bg-zinc-900/50 border-y border-zinc-800">
          <div className="grid grid-cols-3 gap-6">
            <div className="flex flex-col items-center text-center">
              <div className="w-12 h-12 bg-zinc-800 rounded-full flex items-center justify-center mb-3">
                <Truck className="w-6 h-6 text-orange-500" />
              </div>
              <span className="text-sm font-medium text-zinc-300">Free Shipping</span>
              <span className="text-xs text-zinc-500">Orders $50+</span>
            </div>
            <div className="flex flex-col items-center text-center">
              <div className="w-12 h-12 bg-zinc-800 rounded-full flex items-center justify-center mb-3">
                <Package className="w-6 h-6 text-orange-500" />
              </div>
              <span className="text-sm font-medium text-zinc-300">Secure Packaging</span>
              <span className="text-xs text-zinc-500">Protected Shipping</span>
            </div>
            <div className="flex flex-col items-center text-center">
              <div className="w-12 h-12 bg-zinc-800 rounded-full flex items-center justify-center mb-3">
                <Shield className="w-6 h-6 text-orange-500" />
              </div>
              <span className="text-sm font-medium text-zinc-300">Buyer Protection</span>
              <span className="text-xs text-zinc-500">Satisfaction Guaranteed</span>
            </div>
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
