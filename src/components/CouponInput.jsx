/**
 * Coupon Input Component
 *
 * Validates and applies coupon codes with visual feedback.
 */

import { useState, useCallback } from 'react';

const API_BASE = import.meta.env.VITE_API_URL || 'http://localhost:8080/api';

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

  // Show applied coupon state
  if (appliedCoupon) {
    return (
      <div className="flex items-center justify-between p-3 bg-green-50 border border-green-200 rounded-lg">
        <div className="flex items-center gap-2">
          <svg
            className="w-5 h-5 text-green-600"
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
          <span className="font-medium text-green-800">
            {appliedCoupon.code}
          </span>
          <span className="text-green-600">
            -${appliedCoupon.discount_amount.toFixed(2)}
          </span>
        </div>
        <button
          type="button"
          onClick={removeCoupon}
          className="text-red-600 hover:text-red-800 text-sm font-medium"
        >
          Remove
        </button>
      </div>
    );
  }

  return (
    <div className="space-y-2">
      <form onSubmit={handleSubmit} className="flex gap-2">
        <input
          type="text"
          value={code}
          onChange={(e) => setCode(e.target.value.toUpperCase())}
          placeholder="Enter coupon code"
          disabled={loading}
          className="flex-1 px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent disabled:bg-gray-100 uppercase"
          maxLength={50}
        />
        <button
          type="submit"
          disabled={loading || !code.trim()}
          className="px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 disabled:bg-gray-400 disabled:cursor-not-allowed transition-colors font-medium"
        >
          {loading ? (
            <span className="flex items-center gap-2">
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
              Applying...
            </span>
          ) : (
            'Apply'
          )}
        </button>
      </form>

      {/* Error message */}
      {error && (
        <div className="flex items-center gap-2 text-red-600 text-sm">
          <svg
            className="w-4 h-4"
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
          {error}
        </div>
      )}

      {/* Success message */}
      {success && !appliedCoupon && (
        <div className="flex items-center gap-2 text-green-600 text-sm">
          <svg
            className="w-4 h-4"
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
          {success}
        </div>
      )}
    </div>
  );
}
