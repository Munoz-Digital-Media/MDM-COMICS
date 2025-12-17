/**
 * RefundPolicyBadge - Displays refund policy info on product pages
 * BCW Refund Request Module v1.0.0
 *
 * Shows whether a product is eligible for refunds or is final sale.
 * BCW Supplies = Refundable (30 days, 15% restocking)
 * Collectibles (comics, Funkos, graded) = FINAL SALE
 */
import React from 'react';
import { RefreshCw, AlertTriangle, Info } from 'lucide-react';

/**
 * Determine product type from product data
 * @param {Object} product - Product object
 * @returns {string} - Product type: 'bcw_supply', 'comic', 'funko', 'graded'
 */
function getProductType(product) {
  if (!product) return 'unknown';

  const source = product.source?.toLowerCase() || '';
  const category = product.category?.toLowerCase() || '';
  const subcategory = product.subcategory?.toLowerCase() || '';

  // BCW Supplies
  if (
    source === 'bcw' ||
    category === 'supplies' ||
    category === 'bcw supplies' ||
    category === 'bcw'
  ) {
    return 'bcw_supply';
  }

  // Graded items
  if (
    category === 'graded' ||
    subcategory.includes('cgc') ||
    subcategory.includes('cbcs') ||
    subcategory.includes('psa')
  ) {
    return 'graded';
  }

  // Funkos
  if (
    category === 'funko' ||
    category === 'funkos' ||
    category.includes('funko pop')
  ) {
    return 'funko';
  }

  // Default to comic for all other collectibles
  return 'comic';
}

/**
 * Get policy config based on product type
 */
function getPolicyConfig(productType) {
  const policies = {
    bcw_supply: {
      isRefundable: true,
      badge: 'Returns Accepted',
      badgeColor: 'bg-green-500/20 text-green-400 border-green-500/30',
      icon: RefreshCw,
      summary: '30-day returns with 15% restocking fee',
      details: 'BCW supply products may be returned within 30 days of delivery for a refund. Items must be unopened and in original packaging. A 15% restocking fee applies.',
    },
    comic: {
      isRefundable: false,
      badge: 'Final Sale',
      badgeColor: 'bg-orange-500/20 text-orange-400 border-orange-500/30',
      icon: AlertTriangle,
      summary: 'All sales final - collectible item',
      details: 'Due to the condition-sensitive nature of comic books, all sales are final. Please review all photos and descriptions before purchase.',
    },
    funko: {
      isRefundable: false,
      badge: 'Final Sale',
      badgeColor: 'bg-orange-500/20 text-orange-400 border-orange-500/30',
      icon: AlertTriangle,
      summary: 'All sales final - collectible item',
      details: 'Due to the collectible nature of Funko Pop! figures, all sales are final. Box condition is noted in the listing.',
    },
    graded: {
      isRefundable: false,
      badge: 'Final Sale',
      badgeColor: 'bg-orange-500/20 text-orange-400 border-orange-500/30',
      icon: AlertTriangle,
      summary: 'All sales final - professionally graded',
      details: 'Professionally graded items have been evaluated by third-party services. All sales are final.',
    },
  };

  return policies[productType] || policies.comic;
}

export default function RefundPolicyBadge({ product, showDetails = false, className = '' }) {
  const productType = getProductType(product);
  const policy = getPolicyConfig(productType);
  const Icon = policy.icon;

  return (
    <div className={`${className}`}>
      {/* Badge */}
      <div className={`inline-flex items-center gap-1.5 px-3 py-1.5 rounded-lg border ${policy.badgeColor}`}>
        <Icon className="w-4 h-4" />
        <span className="text-sm font-medium">{policy.badge}</span>
      </div>

      {/* Summary */}
      <p className="mt-2 text-xs text-zinc-500">
        {policy.summary}
      </p>

      {/* Expanded details (optional) */}
      {showDetails && (
        <div className="mt-3 p-3 bg-zinc-800/50 rounded-lg">
          <div className="flex items-start gap-2">
            <Info className="w-4 h-4 text-zinc-400 flex-shrink-0 mt-0.5" />
            <p className="text-xs text-zinc-400 leading-relaxed">
              {policy.details}
            </p>
          </div>
        </div>
      )}
    </div>
  );
}

/**
 * Compact version for product cards
 */
export function RefundPolicyBadgeCompact({ product }) {
  const productType = getProductType(product);
  const policy = getPolicyConfig(productType);
  const Icon = policy.icon;

  return (
    <span className={`inline-flex items-center gap-1 px-2 py-0.5 text-xs rounded border ${policy.badgeColor}`}>
      <Icon className="w-3 h-3" />
      {policy.badge}
    </span>
  );
}
