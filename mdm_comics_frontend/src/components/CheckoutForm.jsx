import React, { useState, useEffect, useRef, useCallback } from 'react';
import { loadStripe } from '@stripe/stripe-js';
import {
  Elements,
  PaymentElement,
  useStripe,
  useElements,
} from '@stripe/react-stripe-js';
import { Loader2, CreditCard, Lock, CheckCircle, AlertCircle } from 'lucide-react';
import { checkoutAPI } from '../services/api';

// Payment form component (inside Elements provider)
// P1-5: Removed token prop - auth handled via HttpOnly cookies
function PaymentForm({ clientSecret, cartItems, total, onSuccess, onCancel }) {
  const stripe = useStripe();
  const elements = useElements();
  const [processing, setProcessing] = useState(false);
  const [error, setError] = useState(null);

  const handleSubmit = async (e) => {
    e.preventDefault();

    if (!stripe || !elements) {
      return;
    }

    setProcessing(true);
    setError(null);

    try {
      // Confirm payment with Stripe
      const { error: stripeError, paymentIntent } = await stripe.confirmPayment({
        elements,
        confirmParams: {
          return_url: window.location.origin + '/order-complete',
        },
        redirect: 'if_required',
      });

      if (stripeError) {
        setError(stripeError.message);
        setProcessing(false);
        return;
      }

      if (paymentIntent && paymentIntent.status === 'succeeded') {
        // Payment successful - create order in our database
        // P1-5: No token needed - auth via cookies
        const orderResult = await checkoutAPI.confirmOrder(
          null,  // P1-5: token no longer needed
          paymentIntent.id,
          cartItems.map(item => ({
            product_id: item.product_id || item.id,
            quantity: item.quantity
          }))
        );
        onSuccess(orderResult);
      }
    } catch (err) {
      setError(err.message || 'Payment failed');
    } finally {
      setProcessing(false);
    }
  };

  return (
    <form onSubmit={handleSubmit} className="space-y-6">
      <div className="bg-zinc-800 rounded-xl p-6">
        <div className="flex items-center gap-2 mb-4">
          <CreditCard className="w-5 h-5 text-orange-500" />
          <h3 className="text-lg font-semibold text-white">Payment Details</h3>
        </div>

        {/* Stripe Payment Element - only card info goes to Stripe */}
        <div className="bg-zinc-900 rounded-lg p-4">
          <PaymentElement
            options={{
              layout: 'tabs',
            }}
          />
        </div>

        <div className="flex items-center gap-2 mt-4 text-xs text-zinc-500">
          <Lock className="w-3 h-3" />
          <span>Your payment info is encrypted and secure</span>
        </div>
      </div>

      {error && (
        <div className="flex items-center gap-2 p-4 bg-red-500/10 border border-red-500/20 rounded-lg text-red-400">
          <AlertCircle className="w-5 h-5 flex-shrink-0" />
          <span>{error}</span>
        </div>
      )}

      <div className="flex gap-4">
        <button
          type="button"
          onClick={onCancel}
          disabled={processing}
          className="flex-1 py-3 bg-zinc-700 text-white rounded-xl font-semibold hover:bg-zinc-600 transition-colors disabled:opacity-50"
        >
          Cancel
        </button>
        <button
          type="submit"
          disabled={!stripe || processing}
          className="flex-1 py-3 bg-orange-500 text-white rounded-xl font-semibold hover:bg-orange-600 transition-colors disabled:opacity-50 flex items-center justify-center gap-2"
        >
          {processing ? (
            <>
              <Loader2 className="w-5 h-5 animate-spin" />
              Processing...
            </>
          ) : (
            <>Pay ${total.toFixed(2)}</>
          )}
        </button>
      </div>
    </form>
  );
}

// Main checkout component
// P1-5: Removed token prop - auth handled via HttpOnly cookies
// OPT-005: Added cleanup to cancel reservation on unmount
export default function CheckoutForm({ cartItems, total, onSuccess, onCancel }) {
  const [stripePromise, setStripePromise] = useState(null);
  const [clientSecret, setClientSecret] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);

  // OPT-005: Track payment intent ID and order completion for cleanup
  const paymentIntentIdRef = useRef(null);
  const orderCompletedRef = useRef(false);

  // OPT-005: Cleanup function to cancel reservation
  const cancelReservation = useCallback(async () => {
    if (paymentIntentIdRef.current && !orderCompletedRef.current) {
      try {
        await checkoutAPI.cancelReservation(paymentIntentIdRef.current);
        console.log('Reservation cancelled successfully');
      } catch (err) {
        // Silently fail - reservation will timeout anyway
        console.debug('Failed to cancel reservation:', err);
      }
    }
  }, []);

  // OPT-005: Cancel reservation on unmount
  useEffect(() => {
    return () => {
      cancelReservation();
    };
  }, [cancelReservation]);

  // Wrap onSuccess to mark order as completed
  const handleSuccess = useCallback((order) => {
    orderCompletedRef.current = true;
    onSuccess(order);
  }, [onSuccess]);

  useEffect(() => {
    initializeCheckout();
  }, []);

  const initializeCheckout = async () => {
    // MED-005: Reset error state at start to clear stale errors
    setError(null);
    try {
      // Get Stripe publishable key from our backend
      const config = await checkoutAPI.getConfig();
      setStripePromise(loadStripe(config.publishable_key));

      // Create payment intent
      const items = cartItems.map(item => ({
        product_id: item.product_id || item.id,
        quantity: item.quantity
      }));

      // P1-5: No token needed - auth via cookies
      const paymentIntent = await checkoutAPI.createPaymentIntent(null, items);
      setClientSecret(paymentIntent.client_secret);

      // OPT-005: Store payment intent ID for cleanup
      // Extract from client_secret format: pi_xxx_secret_yyy
      const piId = paymentIntent.client_secret?.split('_secret_')[0];
      paymentIntentIdRef.current = piId;
    } catch (err) {
      setError(err.message || 'Failed to initialize checkout');
    } finally {
      setLoading(false);
    }
  };

  if (loading) {
    return (
      <div className="flex flex-col items-center justify-center py-12">
        <Loader2 className="w-8 h-8 animate-spin text-orange-500 mb-4" />
        <p className="text-zinc-400">Preparing checkout...</p>
      </div>
    );
  }

  if (error) {
    return (
      <div className="text-center py-12">
        <AlertCircle className="w-12 h-12 text-red-500 mx-auto mb-4" />
        <p className="text-red-400 mb-4">{error}</p>
        <button
          onClick={onCancel}
          className="px-6 py-2 bg-zinc-700 text-white rounded-lg hover:bg-zinc-600"
        >
          Go Back
        </button>
      </div>
    );
  }

  if (!clientSecret || !stripePromise) {
    return null;
  }

  return (
    <Elements
      stripe={stripePromise}
      options={{
        clientSecret,
        appearance: {
          theme: 'night',
          variables: {
            colorPrimary: '#f97316',
            colorBackground: '#18181b',
            colorText: '#ffffff',
            colorDanger: '#ef4444',
            fontFamily: 'system-ui, sans-serif',
            borderRadius: '8px',
          },
        },
      }}
    >
      <PaymentForm
        clientSecret={clientSecret}
        cartItems={cartItems}
        total={total}
        onSuccess={handleSuccess}
        onCancel={onCancel}
      />
    </Elements>
  );
}

// Order success component
export function OrderSuccess({ order, onClose }) {
  return (
    <div className="text-center py-8">
      <CheckCircle className="w-16 h-16 text-green-500 mx-auto mb-4" />
      <h2 className="text-2xl font-bold text-white mb-2">Order Confirmed!</h2>
      <p className="text-zinc-400 mb-4">
        Thank you for your purchase.
      </p>
      <div className="bg-zinc-800 rounded-xl p-4 mb-6 inline-block">
        <p className="text-sm text-zinc-400">Order Number</p>
        <p className="text-lg font-mono font-semibold text-orange-500">
          {order.order_number}
        </p>
      </div>
      <p className="text-sm text-zinc-500 mb-6">
        A confirmation email will be sent to your registered email address.
      </p>
      <button
        onClick={onClose}
        className="px-8 py-3 bg-orange-500 text-white rounded-xl font-semibold hover:bg-orange-600"
      >
        Continue Shopping
      </button>
    </div>
  );
}
