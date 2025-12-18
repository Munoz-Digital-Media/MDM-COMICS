/**
 * Coupon Input Component
 *
 * Validates and applies coupon codes with visual feedback.
 * Updated for dark theme consistency.
 */

import { API_BASE } from '../config/api.config.js';
import { useState, useCallback } from 'react';



/**
 * Coupon input with validation and application
 *
 * @param {Object} props
 * @param {Array} props.cartItems - Cart items [{product_id, category, price, quantity}]
 * @param {number} props.cartTotal - Cart subtotal
 * @param {Function} props.onApplied - Callback when coupon is successfully applied
 * @param {Function} props.onRemoved - Callback when coupon is removed
 * @param {Object} props.appliedCoupon - Currently applied coupon (if any)
 */
export default function CouponInput({
  cartItems = [],
  cartTotal = 0,
  onApplied,
  onRemoved,
  appliedCoupon = null,
}) {
  const [code, setCode] = useState('');
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);
  const [success, setSuccess] = useState(null);

  /**
   * Validate coupon code
   */
  const validateCoupon = useCallback(async () => {
    if (!code.trim()) {
      setError('Please enter a coupon code');
      return;
    }

    setLoading(true);
    setError(null);
    setSuccess(null);

    try {
      const response = await fetch(`${API_BASE}/coupons/validate`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        credentials: 'include',
        body: JSON.stringify({
          code: code.trim(),
          cart_items: cartItems.map(item => ({
            product_id: item.product_id || item.id,
            category: item.category || 'general',
            price: item.price,
            quantity: item.quantity,
          })),
          cart_total: cartTotal,
        }),
      });

      const data = await response.json();

      if (data.valid) {
        setSuccess(data.message);
        // Auto-apply after successful validation
        applyCoupon();
      } else {
        setError(data.message || 'Invalid coupon code');
      }
    } catch (e) {
      setError('Failed to validate coupon. Please try again.');
    } finally {
      setLoading(false);
    }
  }, [code, cartItems, cartTotal]);

  /**
   * Apply validated coupon
   */
  const applyCoupon = useCallback(async () => {
    if (!code.trim()) return;

    setLoading(true);
    setError(null);

    try {
      const response = await fetch(`${API_BASE}/coupons/apply`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        credentials: 'include',
        body: JSON.stringify({
          code: code.trim(),
          cart_items: cartItems.map(item => ({
            product_id: item.product_id || item.id,
            category: item.category || 'general',
            price: item.price,
            quantity: item.quantity,
          })),
          cart_total: cartTotal,
        }),
      });

      const data = await response.json();

      if (data.applied) {
        setSuccess(`Coupon applied! You save $${data.discount_amount.toFixed(2)}`);
        setCode('');

        if (onApplied) {
          onApplied({
            code: data.code,
            discount_amount: data.discount_amount,
            new_total: data.new_total,
            usage_id: data.usage_id,
          });
        }
      } else {
        setError(data.detail || 'Failed to apply coupon');
      }
    } catch (e) {
      setError('Failed to apply coupon. Please try again.');
    } finally {
      setLoading(false);
    }
  }, [code, cartItems, cartTotal, onApplied]);

  /**
   * Remove applied coupon
   */
  const removeCoupon = useCallback(() => {
    setSuccess(null);
    setError(null);
    setCode('');

    if (onRemoved) {
      onRemoved();
    }
  }, [onRemoved]);

  /**
   * Handle form submit
   */
  const handleSubmit = (e) => {
    e.preventDefault();
    validateCoupon();
  };

  // Show applied coupon state (dark theme)
  if (appliedCoupon) {
    return (
      <div className="flex items-center justify-between p-3 bg-green-500/10 border border-green-500/30 rounded-lg">
        <div className="flex items-center gap-2">
          <svg
            className="w-5 h-5 text-green-400"
            fill="none"
            stroke="currentColor"
            viewBox="0 0 24 24"
          >
            <path
              strokeLinecap="round"
              strokeLinejoin="round"
              strokeWidth={2}
              d="M9 12l2 2 4-4m6 2a9 9 0 11-18 0 9 9 0 0118 0z"
            />
          </svg>
          <span className="font-medium text-green-400">
            {appliedCoupon.code}
          </span>
          <span className="text-green-300">
            -${appliedCoupon.discount_amount.toFixed(2)}
          </span>
        </div>
        <button
          type="button"
          onClick={removeCoupon}
          className="text-red-400 hover:text-red-300 text-sm font-medium min-h-[44px] min-w-[60px] flex items-center justify-center"
        >
          Remove
        </button>
      </div>
    );
  }

  return (
    <div className="space-y-2">
      <form onSubmit={handleSubmit} className="flex flex-col sm:flex-row gap-2">
        <input
          type="text"
          value={code}
          onChange={(e) => setCode(e.target.value.toUpperCase())}
          placeholder="Enter coupon code"
          disabled={loading}
          className="flex-1 px-3 py-3 sm:py-2 bg-zinc-800 border border-zinc-700 rounded-lg text-white placeholder-zinc-500 focus:ring-2 focus:ring-orange-500 focus:border-transparent disabled:bg-zinc-900 disabled:text-zinc-600 uppercase text-base"
          maxLength={50}
        />
        <button
          type="submit"
          disabled={loading || !code.trim()}
          className="px-4 py-3 sm:py-2 bg-orange-500 text-white rounded-lg hover:bg-orange-600 disabled:bg-zinc-700 disabled:text-zinc-500 disabled:cursor-not-allowed transition-colors font-medium min-h-[44px] min-w-[80px]"
        >
          {loading ? (
            <span className="flex items-center justify-center gap-2">
              <svg
                className="animate-spin h-4 w-4"
                fill="none"
                viewBox="0 0 24 24"
              >
                <circle
                  className="opacity-25"
                  cx="12"
                  cy="12"
                  r="10"
                  stroke="currentColor"
                  strokeWidth="4"
                />
                <path
                  className="opacity-75"
                  fill="currentColor"
                  d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4z"
                />
              </svg>
              <span className="hidden sm:inline">Applying...</span>
            </span>
          ) : (
            'Apply'
          )}
        </button>
      </form>

      {/* Error message */}
      {error && (
        <div className="flex items-center gap-2 text-red-400 text-sm">
          <svg
            className="w-4 h-4 flex-shrink-0"
            fill="none"
            stroke="currentColor"
            viewBox="0 0 24 24"
          >
            <path
              strokeLinecap="round"
              strokeLinejoin="round"
              strokeWidth={2}
              d="M12 8v4m0 4h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z"
            />
          </svg>
          <span>{error}</span>
        </div>
      )}

      {/* Success message */}
      {success && !appliedCoupon && (
        <div className="flex items-center gap-2 text-green-400 text-sm">
          <svg
            className="w-4 h-4 flex-shrink-0"
            fill="none"
            stroke="currentColor"
            viewBox="0 0 24 24"
          >
            <path
              strokeLinecap="round"
              strokeLinejoin="round"
              strokeWidth={2}
              d="M9 12l2 2 4-4m6 2a9 9 0 11-18 0 9 9 0 0118 0z"
            />
          </svg>
          <span>{success}</span>
        </div>
      )}
    </div>
  );
}
