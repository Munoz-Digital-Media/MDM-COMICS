/**
 * HomepageSection Component
 *
 * CHARLIE-04: Generic section component for homepage that renders
 * either products or bundles based on data_source configuration.
 *
 * Features:
 * - Consistent header with emoji, title, and "See More" link
 * - 5-column grid on large screens
 * - Loading skeleton while data fetches
 * - Empty state when no items
 * - Supports both ProductCard and BundleCard rendering
 *
 * @param {Object} section - Section configuration from useHomepageSections
 * @param {Array} items - Array of products or bundles to display
 * @param {boolean} loading - Loading state
 * @param {function} onItemClick - Handler for clicking an item
 * @param {function} onAddToCart - Handler for adding item to cart
 * @param {function} onNavigate - Handler for "See More" navigation
 */
import React, { memo } from "react";
import ProductCard from "./ProductCard";
import BundleCard from "./BundleCard";

// Skeleton loader for product/bundle cards
const CardSkeleton = memo(({ index }) => (
  <div
    className="bg-zinc-900 rounded-xl border border-zinc-800 animate-pulse"
    style={{ animationDelay: `${0.05 * index}s` }}
  >
    {/* Image skeleton */}
    <div className="h-32 sm:h-36 md:h-40 bg-zinc-800 rounded-t-xl" />
    {/* Content skeleton */}
    <div className="p-2 sm:p-3">
      <div className="h-2 bg-zinc-800 rounded w-1/3 mb-2" />
      <div className="h-3 bg-zinc-800 rounded w-3/4 mb-1" />
      <div className="h-3 bg-zinc-800 rounded w-1/2 mb-2" />
      <div className="h-2 bg-zinc-800 rounded w-full mb-2" />
      <div className="flex items-center justify-between">
        <div className="h-5 bg-zinc-800 rounded w-1/3" />
        <div className="h-10 w-10 bg-zinc-800 rounded-lg" />
      </div>
    </div>
  </div>
));

CardSkeleton.displayName = 'CardSkeleton';

// Empty state when no items to display
const EmptyState = memo(({ section }) => (
  <div className="col-span-full flex flex-col items-center justify-center py-8 text-center">
    <span className="text-4xl mb-2">{section.emoji}</span>
    <p className="text-zinc-500 text-sm">No {section.title.toLowerCase()} available right now.</p>
    <p className="text-zinc-600 text-xs mt-1">Check back soon!</p>
  </div>
));

EmptyState.displayName = 'EmptyState';

const HomepageSection = memo(({
  section,
  items = [],
  loading = false,
  onItemClick,
  onAddToCart,
  onNavigate
}) => {
  // Don't render if section is not visible
  if (!section || !section.visible) {
    return null;
  }

  // Handle "See More" click
  const handleSeeMoreClick = (e) => {
    e.preventDefault();
    if (onNavigate) {
      onNavigate(section);
    }
  };

  // Limit items to max_items from section config
  const displayItems = items.slice(0, section.max_items || 5);

  // Determine which card component to use
  const isBundle = section.data_source === 'bundles';

  return (
    <section className="mb-12" aria-labelledby={`section-${section.key}`}>
      {/* Section Header */}
      <div className="flex items-center justify-between mb-4">
        <h3
          id={`section-${section.key}`}
          className="font-comic text-2xl text-white flex items-center gap-2"
        >
          <span aria-hidden="true">{section.emoji}</span>
          <span>{section.title}</span>
        </h3>
        <a
          href={section.category_link}
          onClick={handleSeeMoreClick}
          className="see-more-link text-orange-500 hover:text-orange-400 text-sm font-semibold flex items-center gap-1 transition-colors"
        >
          See More →
        </a>
      </div>

      {/* Product/Bundle Grid - enforces 5 columns on lg+ */}
      <div className="grid grid-cols-2 sm:grid-cols-3 md:grid-cols-4 lg:grid-cols-5 gap-3">
        {loading ? (
          // Loading skeletons
          Array.from({ length: section.max_items || 5 }).map((_, index) => (
            <CardSkeleton key={`skeleton-${index}`} index={index} />
          ))
        ) : displayItems.length > 0 ? (
          // Render items
          displayItems.map((item, index) =>
            isBundle ? (
              <BundleCard
                key={item.id || item.slug}
                bundle={item}
                index={index}
                onViewBundle={onItemClick}
                onAddToCart={onAddToCart}
              />
            ) : (
              <ProductCard
                key={item.id}
                product={item}
                index={index}
                addToCart={onAddToCart}
                onViewProduct={onItemClick}
              />
            )
          )
        ) : (
          // Empty state
          <EmptyState section={section} />
        )}
      </div>
    </section>
  );
});

HomepageSection.displayName = 'HomepageSection';

export default HomepageSection;
