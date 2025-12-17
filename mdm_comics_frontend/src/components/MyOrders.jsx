/**
 * MyOrders Component
 * BCW Refund Request Module v1.0.0
 *
 * Customer order history with refund request functionality.
 * BCW Supplies = Refundable (30 days, 15% restocking)
 * Collectibles (comics, Funkos, graded) = FINAL SALE
 */
import React, { useState, useEffect, useCallback } from 'react';
import {
  X, Package, ChevronRight, Clock, CheckCircle, Truck,
  AlertTriangle, RefreshCw, Loader2, AlertCircle, ArrowLeft, Send
} from 'lucide-react';
import { ordersAPI, refundsAPI } from '../services/api';

// Refund reason options
const REFUND_REASONS = [
  { value: 'DEFECTIVE', label: 'Defective/Damaged Item' },
  { value: 'WRONG_ITEM', label: 'Wrong Item Received' },
  { value: 'NOT_AS_DESCRIBED', label: 'Not as Described' },
  { value: 'CHANGED_MIND', label: 'Changed My Mind' },
  { value: 'OTHER', label: 'Other' },
];

// Order status badge component
function OrderStatusBadge({ status }) {
  const statusConfig = {
    pending: { color: 'bg-yellow-500/20 text-yellow-400 border-yellow-500/30', icon: Clock, label: 'Pending' },
    confirmed: { color: 'bg-blue-500/20 text-blue-400 border-blue-500/30', icon: CheckCircle, label: 'Confirmed' },
    processing: { color: 'bg-purple-500/20 text-purple-400 border-purple-500/30', icon: Package, label: 'Processing' },
    shipped: { color: 'bg-cyan-500/20 text-cyan-400 border-cyan-500/30', icon: Truck, label: 'Shipped' },
    delivered: { color: 'bg-green-500/20 text-green-400 border-green-500/30', icon: CheckCircle, label: 'Delivered' },
    cancelled: { color: 'bg-red-500/20 text-red-400 border-red-500/30', icon: X, label: 'Cancelled' },
  };

  const config = statusConfig[status] || statusConfig.pending;
  const Icon = config.icon;

  return (
    <span className={`inline-flex items-center gap-1.5 px-2.5 py-1 rounded-full text-xs font-medium border ${config.color}`}>
      <Icon className="w-3 h-3" />
      {config.label}
    </span>
  );
}

// Refund eligibility badge
function RefundEligibilityBadge({ item }) {
  const category = (item.category || '').toLowerCase();
  const source = (item.source || '').toLowerCase();

  // BCW supplies are refundable
  const isRefundable = source === 'bcw' || category === 'supplies' || category === 'bcw supplies' || category === 'bcw';

  if (isRefundable) {
    return (
      <span className="inline-flex items-center gap-1 px-2 py-0.5 text-xs rounded bg-green-500/20 text-green-400 border border-green-500/30">
        <RefreshCw className="w-3 h-3" />
        Returns Accepted
      </span>
    );
  }

  return (
    <span className="inline-flex items-center gap-1 px-2 py-0.5 text-xs rounded bg-orange-500/20 text-orange-400 border border-orange-500/30">
      <AlertTriangle className="w-3 h-3" />
      Final Sale
    </span>
  );
}

// Refund request modal
function RefundRequestModal({ item, order, onClose, onSuccess }) {
  const [reason, setReason] = useState('');
  const [details, setDetails] = useState('');
  const [submitting, setSubmitting] = useState(false);
  const [error, setError] = useState(null);

  const handleSubmit = async (e) => {
    e.preventDefault();
    if (!reason) {
      setError('Please select a reason');
      return;
    }

    setSubmitting(true);
    setError(null);

    try {
      await refundsAPI.createRequest(order.id, item.id, reason, details || null);
      onSuccess();
    } catch (err) {
      setError(err.message || 'Failed to submit refund request');
    } finally {
      setSubmitting(false);
    }
  };

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center p-4">
      <div className="absolute inset-0 bg-black/60 backdrop-blur-sm" onClick={onClose} />
      <div className="relative bg-zinc-900 rounded-2xl border border-zinc-800 w-full max-w-lg shadow-2xl">
        {/* Header */}
        <div className="flex items-center justify-between p-4 border-b border-zinc-800">
          <h3 className="text-lg font-semibold text-white">Request Refund</h3>
          <button
            onClick={onClose}
            className="p-2 hover:bg-zinc-800 rounded-lg transition-colors"
          >
            <X className="w-5 h-5 text-zinc-400" />
          </button>
        </div>

        {/* Content */}
        <form onSubmit={handleSubmit} className="p-4 space-y-4">
          {/* Item info */}
          <div className="flex gap-3 p-3 bg-zinc-800/50 rounded-lg">
            <img
              src={item.image || item.image_url || 'https://placehold.co/80x100/27272a/f59e0b?text=Item'}
              alt={item.name}
              className="w-16 h-20 object-cover rounded"
            />
            <div className="flex-1">
              <p className="font-medium text-white">{item.name}</p>
              <p className="text-sm text-zinc-400">Qty: {item.quantity}</p>
              <p className="text-sm text-orange-500 font-semibold">${item.price?.toFixed(2)}</p>
            </div>
          </div>

          {/* Policy reminder */}
          <div className="p-3 bg-orange-500/10 border border-orange-500/30 rounded-lg">
            <div className="flex items-start gap-2">
              <AlertTriangle className="w-4 h-4 text-orange-400 flex-shrink-0 mt-0.5" />
              <div className="text-xs text-zinc-400">
                <p className="text-orange-400 font-medium mb-1">Refund Policy</p>
                <p>BCW supply products may be returned within 30 days for a refund. A 15% restocking fee applies. Items must be unopened and in original packaging.</p>
              </div>
            </div>
          </div>

          {/* Reason select */}
          <div>
            <label className="block text-sm font-medium text-zinc-300 mb-2">
              Reason for Refund <span className="text-red-400">*</span>
            </label>
            <select
              value={reason}
              onChange={(e) => setReason(e.target.value)}
              className="w-full px-3 py-2 bg-zinc-800 border border-zinc-700 rounded-lg text-white focus:outline-none focus:border-orange-500"
              required
            >
              <option value="">Select a reason...</option>
              {REFUND_REASONS.map((r) => (
                <option key={r.value} value={r.value}>{r.label}</option>
              ))}
            </select>
          </div>

          {/* Details textarea */}
          <div>
            <label className="block text-sm font-medium text-zinc-300 mb-2">
              Additional Details (Optional)
            </label>
            <textarea
              value={details}
              onChange={(e) => setDetails(e.target.value)}
              placeholder="Please provide any additional information..."
              rows={3}
              className="w-full px-3 py-2 bg-zinc-800 border border-zinc-700 rounded-lg text-white placeholder-zinc-500 focus:outline-none focus:border-orange-500 resize-none"
            />
          </div>

          {/* Error */}
          {error && (
            <div className="flex items-center gap-2 p-3 bg-red-500/10 border border-red-500/20 rounded-lg text-red-400 text-sm">
              <AlertCircle className="w-4 h-4 flex-shrink-0" />
              {error}
            </div>
          )}

          {/* Actions */}
          <div className="flex gap-3 pt-2">
            <button
              type="button"
              onClick={onClose}
              disabled={submitting}
              className="flex-1 py-2.5 bg-zinc-700 text-white rounded-lg font-medium hover:bg-zinc-600 transition-colors disabled:opacity-50"
            >
              Cancel
            </button>
            <button
              type="submit"
              disabled={submitting || !reason}
              className="flex-1 py-2.5 bg-orange-500 text-white rounded-lg font-medium hover:bg-orange-600 transition-colors disabled:opacity-50 flex items-center justify-center gap-2"
            >
              {submitting ? (
                <>
                  <Loader2 className="w-4 h-4 animate-spin" />
                  Submitting...
                </>
              ) : (
                <>
                  <Send className="w-4 h-4" />
                  Submit Request
                </>
              )}
            </button>
          </div>
        </form>
      </div>
    </div>
  );
}

// Order detail view
function OrderDetail({ order, onBack, onRefundRequested }) {
  const [refundItem, setRefundItem] = useState(null);

  const canRequestRefund = (item) => {
    const category = (item.category || '').toLowerCase();
    const source = (item.source || '').toLowerCase();

    // Only BCW supplies are refundable
    const isRefundable = source === 'bcw' || category === 'supplies' || category === 'bcw supplies' || category === 'bcw';

    // Check if order is delivered and within 30 days
    if (!isRefundable) return false;
    if (order.status !== 'delivered') return false;

    const deliveredDate = new Date(order.delivered_at || order.updated_at);
    const daysSinceDelivery = (Date.now() - deliveredDate.getTime()) / (1000 * 60 * 60 * 24);

    return daysSinceDelivery <= 30;
  };

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex items-center gap-4">
        <button
          onClick={onBack}
          className="p-2 hover:bg-zinc-800 rounded-lg transition-colors"
        >
          <ArrowLeft className="w-5 h-5 text-zinc-400" />
        </button>
        <div>
          <h3 className="text-lg font-semibold text-white">Order #{order.order_number}</h3>
          <p className="text-sm text-zinc-500">
            {new Date(order.created_at).toLocaleDateString()}
          </p>
        </div>
        <div className="ml-auto">
          <OrderStatusBadge status={order.status} />
        </div>
      </div>

      {/* Order items */}
      <div className="bg-zinc-800/50 rounded-xl border border-zinc-700 overflow-hidden">
        <div className="p-4 border-b border-zinc-700">
          <h4 className="font-medium text-white">Items</h4>
        </div>
        <div className="divide-y divide-zinc-700">
          {order.items?.map((item) => (
            <div key={item.id} className="p-4 flex items-center gap-4">
              <img
                src={item.image || item.image_url || 'https://placehold.co/60x80/27272a/f59e0b?text=Item'}
                alt={item.name}
                className="w-16 h-20 object-cover rounded"
              />
              <div className="flex-1 min-w-0">
                <p className="font-medium text-white truncate">{item.name}</p>
                <p className="text-sm text-zinc-400">Qty: {item.quantity}</p>
                <RefundEligibilityBadge item={item} />
              </div>
              <div className="text-right">
                <p className="font-semibold text-white">${(item.price * item.quantity).toFixed(2)}</p>
                {canRequestRefund(item) && (
                  <button
                    onClick={() => setRefundItem(item)}
                    className="mt-2 text-xs text-orange-500 hover:text-orange-400 font-medium"
                  >
                    Request Refund
                  </button>
                )}
              </div>
            </div>
          ))}
        </div>
      </div>

      {/* Order summary */}
      <div className="bg-zinc-800/50 rounded-xl border border-zinc-700 p-4 space-y-2">
        <div className="flex justify-between text-sm">
          <span className="text-zinc-400">Subtotal</span>
          <span className="text-white">${order.subtotal?.toFixed(2)}</span>
        </div>
        <div className="flex justify-between text-sm">
          <span className="text-zinc-400">Shipping</span>
          <span className="text-white">{order.shipping_cost > 0 ? `$${order.shipping_cost?.toFixed(2)}` : 'FREE'}</span>
        </div>
        <div className="flex justify-between text-sm pt-2 border-t border-zinc-700">
          <span className="font-medium text-white">Total</span>
          <span className="font-semibold text-orange-500">${order.total?.toFixed(2)}</span>
        </div>
      </div>

      {/* Refund request modal */}
      {refundItem && (
        <RefundRequestModal
          item={refundItem}
          order={order}
          onClose={() => setRefundItem(null)}
          onSuccess={() => {
            setRefundItem(null);
            onRefundRequested();
          }}
        />
      )}
    </div>
  );
}

// Main component
export default function MyOrders({ onClose }) {
  const [orders, setOrders] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [selectedOrder, setSelectedOrder] = useState(null);
  const [notification, setNotification] = useState(null);

  const fetchOrders = useCallback(async () => {
    try {
      setLoading(true);
      const response = await ordersAPI.getMyOrders();
      setOrders(response.orders || response || []);
    } catch (err) {
      setError(err.message || 'Failed to load orders');
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    fetchOrders();
  }, [fetchOrders]);

  const showNotification = (message, type = 'success') => {
    setNotification({ message, type });
    setTimeout(() => setNotification(null), 3000);
  };

  const handleRefundRequested = () => {
    showNotification('Refund request submitted successfully!');
    fetchOrders();
  };

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center p-4">
      <div className="absolute inset-0 bg-black/60 backdrop-blur-sm" onClick={onClose} />
      <div className="relative bg-zinc-900 rounded-2xl border border-zinc-800 w-full max-w-2xl max-h-[90vh] overflow-hidden shadow-2xl flex flex-col">
        {/* Header */}
        <div className="flex items-center justify-between p-4 border-b border-zinc-800 flex-shrink-0">
          <h2 className="text-xl font-bold text-white flex items-center gap-2">
            <Package className="w-5 h-5 text-orange-500" />
            My Orders
          </h2>
          <button
            onClick={onClose}
            className="p-2 hover:bg-zinc-800 rounded-lg transition-colors"
          >
            <X className="w-5 h-5 text-zinc-400" />
          </button>
        </div>

        {/* Notification */}
        {notification && (
          <div className={`mx-4 mt-4 p-3 rounded-lg flex items-center gap-2 ${
            notification.type === 'error'
              ? 'bg-red-500/10 border border-red-500/20 text-red-400'
              : 'bg-green-500/10 border border-green-500/20 text-green-400'
          }`}>
            {notification.type === 'error' ? <AlertCircle className="w-4 h-4" /> : <CheckCircle className="w-4 h-4" />}
            {notification.message}
          </div>
        )}

        {/* Content */}
        <div className="flex-1 overflow-auto p-4">
          {loading ? (
            <div className="flex flex-col items-center justify-center py-12">
              <Loader2 className="w-8 h-8 animate-spin text-orange-500 mb-4" />
              <p className="text-zinc-400">Loading orders...</p>
            </div>
          ) : error ? (
            <div className="text-center py-12">
              <AlertCircle className="w-12 h-12 text-red-500 mx-auto mb-4" />
              <p className="text-red-400 mb-4">{error}</p>
              <button
                onClick={fetchOrders}
                className="px-4 py-2 bg-orange-500 text-white rounded-lg hover:bg-orange-600"
              >
                Try Again
              </button>
            </div>
          ) : selectedOrder ? (
            <OrderDetail
              order={selectedOrder}
              onBack={() => setSelectedOrder(null)}
              onRefundRequested={handleRefundRequested}
            />
          ) : orders.length === 0 ? (
            <div className="text-center py-12">
              <Package className="w-16 h-16 text-zinc-700 mx-auto mb-4" />
              <p className="text-zinc-400">No orders yet</p>
              <p className="text-sm text-zinc-500 mt-2">Your order history will appear here</p>
            </div>
          ) : (
            <div className="space-y-3">
              {orders.map((order) => (
                <button
                  key={order.id}
                  onClick={() => setSelectedOrder(order)}
                  className="w-full p-4 bg-zinc-800/50 hover:bg-zinc-800 border border-zinc-700 rounded-xl transition-colors text-left group"
                >
                  <div className="flex items-center justify-between mb-2">
                    <span className="font-mono text-sm text-orange-500">#{order.order_number}</span>
                    <OrderStatusBadge status={order.status} />
                  </div>
                  <div className="flex items-center justify-between">
                    <div>
                      <p className="text-white font-medium">
                        {order.items?.length || 0} item{(order.items?.length || 0) !== 1 ? 's' : ''}
                      </p>
                      <p className="text-xs text-zinc-500">
                        {new Date(order.created_at).toLocaleDateString()}
                      </p>
                    </div>
                    <div className="flex items-center gap-2">
                      <span className="font-semibold text-white">${order.total?.toFixed(2)}</span>
                      <ChevronRight className="w-5 h-5 text-zinc-500 group-hover:text-orange-500 transition-colors" />
                    </div>
                  </div>
                </button>
              ))}
            </div>
          )}
        </div>
      </div>
    </div>
  );
}
