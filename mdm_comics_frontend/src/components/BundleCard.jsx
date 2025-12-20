/**
 * BundleCard Component
 *
 * Displays a single bundle in the shop grid.
 * Matches ProductCard visual style for consistency.
 *
 * @param {Object} bundle - Bundle data
 * @param {number} index - Index for animation delay
 * @param {function} onViewBundle - Handler for viewing bundle details
 * @param {function} onAddToCart - Handler for adding bundle to cart
 */
import React, { memo, useCallback } from "react";
import { Package, Plus } from "lucide-react";

const BundleCard = memo(({ bundle, index, onViewBundle, onAddToCart }) => {
  // Handle card click - navigate to bundle detail
  const handleCardClick = useCallback((e) => {
    // Don't navigate if clicking the add-to-cart button
    if (e.target.closest('button')) return;
    if (onViewBundle) {
      onViewBundle(bundle);
    }
  }, [bundle, onViewBundle]);

  // Handle keyboard navigation
  const handleKeyDown = useCallback((e) => {
    if (e.key === 'Enter' && onViewBundle) {
      onViewBundle(bundle);
    }
  }, [bundle, onViewBundle]);

  // Handle add to cart
  const handleAddToCart = useCallback((e) => {
    e.stopPropagation();
    if (onAddToCart && bundle.available_qty > 0) {
      onAddToCart(bundle);
    }
  }, [bundle, onAddToCart]);

  // Calculate if bundle is available
  const isAvailable = bundle.available_qty > 0;

  return (
    <div
      className={`product-card bg-zinc-900 rounded-xl border border-zinc-800 fade-up ${
        onViewBundle ? 'cursor-pointer hover:border-orange-500/50 transition-colors' : ''
      }`}
      style={{ animationDelay: `${0.05 * index}s` }}
      onClick={handleCardClick}
      onKeyDown={handleKeyDown}
      tabIndex={onViewBundle ? 0 : undefined}
      role={onViewBundle ? "button" : undefined}
      aria-label={onViewBundle ? `View ${bundle.name} bundle` : undefined}
    >
      {/* Bundle Image - responsive height */}
      <div className="relative h-32 sm:h-36 md:h-40 bg-zinc-800 rounded-t-xl overflow-hidden">
        <img
          src={bundle.image_url}
          alt={bundle.name}
          className="w-full h-full object-cover"
          onError={(e) => {
            e.target.onerror = null;
            e.target.src = `https://placehold.co/400x500/27272a/f59e0b?text=${encodeURIComponent('🎁')}`;
          }}
        />
        {/* Badges - compact */}
        <div className="absolute top-1.5 sm:top-2 left-1.5 sm:left-2 flex flex-col gap-1">
          {bundle.badge_text && (
            <span className="px-1.5 sm:px-2 py-0.5 sm:py-1 bg-orange-500 rounded-full text-[9px] sm:text-[10px] font-bold text-white shadow-lg">
              {bundle.badge_text}
            </span>
          )}
          {bundle.available_qty <= 3 && bundle.available_qty > 0 && (
            <span className="px-1.5 sm:px-2 py-0.5 sm:py-1 bg-red-600 rounded-full text-[9px] sm:text-[10px] font-bold text-white shadow-lg">
              🔥 {bundle.available_qty} left
            </span>
          )}
        </div>
        {/* Savings badge - top right */}
        {bundle.savings_percent > 0 && (
          <div className="absolute top-1.5 sm:top-2 right-1.5 sm:right-2">
            <span className="px-1.5 sm:px-2 py-0.5 sm:py-1 bg-green-600 rounded-full text-[9px] sm:text-[10px] font-bold text-white shadow-lg">
              {Math.round(bundle.savings_percent)}% OFF
            </span>
          </div>
        )}
      </div>

      {/* Bundle Info */}
      <div className="p-2 sm:p-3">
        {/* Item count indicator */}
        <div className="flex items-center gap-1 text-[9px] sm:text-[10px] text-orange-500 font-semibold mb-0.5">
          <Package className="w-3 h-3" />
          <span>{bundle.item_count} items</span>
        </div>
        <h3 className="font-bold text-xs sm:text-sm text-white mb-1 line-clamp-2 leading-tight">{bundle.name}</h3>
        <p className="text-zinc-500 text-[10px] sm:text-xs mb-2 line-clamp-1">{bundle.short_description}</p>

        {/* Price & Add to Cart */}
        <div className="flex items-center justify-between gap-2">
          <div className="min-w-0 flex-1">
            <span className="text-base sm:text-lg font-bold text-white">${bundle.bundle_price?.toFixed(2)}</span>
            {bundle.compare_at_price && bundle.compare_at_price > bundle.bundle_price && (
              <span className="ml-1 text-[10px] sm:text-xs text-zinc-500 line-through">
                ${bundle.compare_at_price?.toFixed(2)}
              </span>
            )}
          </div>
          <button
            onClick={handleAddToCart}
            disabled={!isAvailable}
            className={`p-2.5 sm:p-2 rounded-lg transition-all flex-shrink-0 min-w-[40px] min-h-[40px] flex items-center justify-center ${
              !isAvailable
                ? "bg-zinc-800 text-zinc-600 cursor-not-allowed"
                : "bg-orange-500 text-white hover:shadow-lg hover:shadow-orange-500/25 active:scale-95"
            }`}
            aria-label={isAvailable ? `Add ${bundle.name} to cart` : 'Bundle unavailable'}
          >
            <Plus className="w-4 h-4 sm:w-5 sm:h-5" />
          </button>
        </div>
      </div>
    </div>
  );
});

BundleCard.displayName = 'BundleCard';

export default BundleCard;
