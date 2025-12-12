/**
 * ProductCard Component
 *
 * Displays a single product in the shop grid.
 * Extracted from App.jsx for better code organization.
 */
import React, { memo } from "react";
import { Star, Plus } from "lucide-react";

const ProductCard = memo(({ product, index, addToCart }) => (
  <div
    className="product-card bg-zinc-900 rounded-xl border border-zinc-800 fade-up"
    style={{ animationDelay: `${0.05 * index}s` }}
  >
    {/* Product Image - responsive height */}
    <div className="relative h-32 sm:h-36 md:h-40 bg-zinc-800 rounded-t-xl overflow-hidden">
      <img
        src={product.image}
        alt={product.name}
        className="w-full h-full object-cover"
        onError={(e) => {
          e.target.onerror = null;
          e.target.src = `https://placehold.co/400x500/27272a/f59e0b?text=${encodeURIComponent(product.category === 'comics' ? '📚' : '🎭')}`;
        }}
      />
      {/* Badges - compact */}
      <div className="absolute top-1.5 sm:top-2 left-1.5 sm:left-2 flex flex-col gap-1">
        {product.featured && (
          <span className="px-1.5 sm:px-2 py-0.5 sm:py-1 bg-orange-500 rounded-full text-[9px] sm:text-[10px] font-bold text-white shadow-lg">
            ⭐ FEATURED
          </span>
        )}
        {product.stock <= 5 && (
          <span className="px-1.5 sm:px-2 py-0.5 sm:py-1 bg-red-600 rounded-full text-[9px] sm:text-[10px] font-bold text-white shadow-lg">
            🔥 {product.stock} left
          </span>
        )}
      </div>
      {/* Sale badge - top right */}
      {product.originalPrice && (
        <div className="absolute top-1.5 sm:top-2 right-1.5 sm:right-2">
          <span className="px-1.5 sm:px-2 py-0.5 sm:py-1 bg-green-600 rounded-full text-[9px] sm:text-[10px] font-bold text-white shadow-lg">
            SALE
          </span>
        </div>
      )}
    </div>

    {/* Product Info */}
    <div className="p-2 sm:p-3">
      <p className="text-[9px] sm:text-[10px] text-orange-500 font-semibold mb-0.5">{product.subcategory}</p>
      <h3 className="font-bold text-xs sm:text-sm text-white mb-1 line-clamp-2 leading-tight">{product.name}</h3>
      <p className="text-zinc-500 text-[10px] sm:text-xs mb-2 line-clamp-1">{product.description}</p>

      {/* Rating */}
      <div className="flex items-center gap-1 mb-2">
        <Star className="w-3 h-3 fill-orange-500 text-orange-500" />
        <span className="text-[10px] sm:text-xs text-zinc-400">{product.rating}</span>
      </div>

      {/* Price & Add to Cart */}
      <div className="flex items-center justify-between gap-2">
        <div className="min-w-0 flex-1">
          <span className="text-base sm:text-lg font-bold text-white">${product.price}</span>
          {product.originalPrice && (
            <span className="ml-1 text-[10px] sm:text-xs text-zinc-500 line-through">
              ${product.originalPrice}
            </span>
          )}
        </div>
        <button
          onClick={() => addToCart(product)}
          disabled={product.stock === 0}
          className={`p-2.5 sm:p-2 rounded-lg transition-all flex-shrink-0 min-w-[40px] min-h-[40px] flex items-center justify-center ${
            product.stock === 0
              ? "bg-zinc-800 text-zinc-600 cursor-not-allowed"
              : "bg-orange-500 text-white hover:shadow-lg hover:shadow-orange-500/25 active:scale-95"
          }`}
        >
          <Plus className="w-4 h-4 sm:w-5 sm:h-5" />
        </button>
      </div>
    </div>
  </div>
));

ProductCard.displayName = 'ProductCard';

export default ProductCard;
