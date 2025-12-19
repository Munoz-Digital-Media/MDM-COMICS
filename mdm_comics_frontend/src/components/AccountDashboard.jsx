/**
 * AccountDashboard - Tabbed Account Dashboard
 * MDM Comics v1.6.0
 *
 * Full-featured customer account page with tabs:
 * - Dashboard: Overview, stats, recent orders
 * - Orders: Order history with refund functionality
 * - Addresses: Address book management
 * - Payment: Payment methods (placeholder)
 * - Wishlist: Saved items (placeholder)
 * - Collection: Collection tracker (placeholder)
 * - Rewards: Loyalty rewards (placeholder)
 * - Returns: Return requests (placeholder)
 *
 * Integrates with:
 * - ordersAPI for order history
 * - refundsAPI for refund requests
 * - shippingAPI for address management
 */
import React, { useState, useEffect, useCallback } from 'react';
import {
  User, Package, MapPin, CreditCard, Heart, Gift, RotateCcw, Bell,
  HelpCircle, BookOpen, Eye, TrendingUp, ChevronRight, Search, Download,
  Truck, CheckCircle, Clock, AlertCircle, Plus, Edit2, Trash2, Star, X,
  Settings, LogOut, Home, Loader2, ArrowLeft, Send, RefreshCw, AlertTriangle
} from 'lucide-react';
import { ordersAPI, refundsAPI, shippingAPI, authAPI } from '../services/api';

// =============================================================================
// REFUND CONSTANTS & COMPONENTS (from MyOrders.jsx)
// =============================================================================
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
    <div className="fixed inset-0 z-[60] flex items-center justify-center p-4">
      <div className="absolute inset-0 bg-black/60 backdrop-blur-sm" onClick={onClose} />
      <div className="relative bg-zinc-900 rounded-2xl border border-zinc-800 w-full max-w-lg shadow-2xl">
        <div className="flex items-center justify-between p-4 border-b border-zinc-800">
          <h3 className="text-lg font-semibold text-white">Request Refund</h3>
          <button onClick={onClose} className="p-2 hover:bg-zinc-800 rounded-lg transition-colors">
            <X className="w-5 h-5 text-zinc-400" />
          </button>
        </div>

        <form onSubmit={handleSubmit} className="p-4 space-y-4">
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

          <div className="p-3 bg-orange-500/10 border border-orange-500/30 rounded-lg">
            <div className="flex items-start gap-2">
              <AlertTriangle className="w-4 h-4 text-orange-400 flex-shrink-0 mt-0.5" />
              <div className="text-xs text-zinc-400">
                <p className="text-orange-400 font-medium mb-1">Refund Policy</p>
                <p>BCW supply products may be returned within 30 days for a refund. A 15% restocking fee applies. Items must be unopened and in original packaging.</p>
              </div>
            </div>
          </div>

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

          {error && (
            <div className="flex items-center gap-2 p-3 bg-red-500/10 border border-red-500/20 rounded-lg text-red-400 text-sm">
              <AlertCircle className="w-4 h-4 flex-shrink-0" />
              {error}
            </div>
          )}

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

// =============================================================================
// MAIN COMPONENT
// =============================================================================
export default function AccountDashboard({ user, onClose, onLogout }) {
  const [activeTab, setActiveTab] = useState('dashboard');

  // Data states
  const [orders, setOrders] = useState([]);
  const [addresses, setAddresses] = useState([]);
  const [loading, setLoading] = useState({ orders: true, addresses: true });
  const [error, setError] = useState({ orders: null, addresses: null });

  // UI states
  const [selectedOrder, setSelectedOrder] = useState(null);
  const [refundItem, setRefundItem] = useState(null);
  const [notification, setNotification] = useState(null);
  const [searchQuery, setSearchQuery] = useState('');
  const [statusFilter, setStatusFilter] = useState('all');

  // Tab configuration
  const tabs = [
    { id: 'dashboard', label: 'Dashboard', icon: Home },
    { id: 'orders', label: 'Orders', icon: Package, badge: orders.filter(o => o.status === 'shipped' || o.status === 'processing').length || null },
    { id: 'addresses', label: 'Addresses', icon: MapPin },
    { id: 'payment', label: 'Payment', icon: CreditCard },
    { id: 'wishlist', label: 'Wishlist', icon: Heart },
    { id: 'collection', label: 'Collection', icon: BookOpen },
    { id: 'rewards', label: 'Rewards', icon: Gift },
    { id: 'returns', label: 'Returns', icon: RotateCcw },
  ];

  // =============================================================================
  // DATA FETCHING
  // =============================================================================
  const fetchOrders = useCallback(async () => {
    try {
      setLoading(prev => ({ ...prev, orders: true }));
      setError(prev => ({ ...prev, orders: null }));
      const response = await ordersAPI.getMyOrders();
      setOrders(response.orders || response || []);
    } catch (err) {
      setError(prev => ({ ...prev, orders: err.message || 'Failed to load orders' }));
    } finally {
      setLoading(prev => ({ ...prev, orders: false }));
    }
  }, []);

  const fetchAddresses = useCallback(async () => {
    try {
      setLoading(prev => ({ ...prev, addresses: true }));
      setError(prev => ({ ...prev, addresses: null }));
      const response = await shippingAPI.getAddresses();
      setAddresses(response.addresses || response || []);
    } catch (err) {
      setError(prev => ({ ...prev, addresses: err.message || 'Failed to load addresses' }));
    } finally {
      setLoading(prev => ({ ...prev, addresses: false }));
    }
  }, []);

  useEffect(() => {
    fetchOrders();
    fetchAddresses();
  }, [fetchOrders, fetchAddresses]);

  // =============================================================================
  // NOTIFICATIONS
  // =============================================================================
  const showNotification = (message, type = 'success') => {
    setNotification({ message, type });
    setTimeout(() => setNotification(null), 3000);
  };

  // =============================================================================
  // ORDER HELPERS
  // =============================================================================
  const canRequestRefund = (order, item) => {
    const category = (item.category || '').toLowerCase();
    const source = (item.source || '').toLowerCase();
    const isRefundable = source === 'bcw' || category === 'supplies' || category === 'bcw supplies' || category === 'bcw';

    if (!isRefundable) return false;
    if (order.status !== 'delivered') return false;

    const deliveredDate = new Date(order.delivered_at || order.updated_at);
    const daysSinceDelivery = (Date.now() - deliveredDate.getTime()) / (1000 * 60 * 60 * 24);
    return daysSinceDelivery <= 30;
  };

  const getStatusColor = (status) => {
    switch (status) {
      case 'delivered': return 'text-emerald-400 bg-emerald-400/10';
      case 'shipped': return 'text-blue-400 bg-blue-400/10';
      case 'processing': return 'text-amber-400 bg-amber-400/10';
      case 'pending': return 'text-orange-400 bg-orange-400/10';
      default: return 'text-zinc-400 bg-zinc-400/10';
    }
  };

  const getStatusIcon = (status) => {
    switch (status) {
      case 'delivered': return CheckCircle;
      case 'shipped': return Truck;
      case 'processing': return Clock;
      default: return AlertCircle;
    }
  };

  // Filter orders
  const filteredOrders = orders.filter(order => {
    if (statusFilter !== 'all' && order.status !== statusFilter) return false;
    if (searchQuery) {
      const query = searchQuery.toLowerCase();
      return (
        order.order_number?.toLowerCase().includes(query) ||
        order.items?.some(item => item.name?.toLowerCase().includes(query))
      );
    }
    return true;
  });

  // =============================================================================
  // ADDRESS HELPERS
  // =============================================================================
  const handleDeleteAddress = async (addressId) => {
    if (!confirm('Are you sure you want to delete this address?')) return;

    try {
      await shippingAPI.deleteAddress(addressId);
      setAddresses(prev => prev.filter(a => a.id !== addressId));
      showNotification('Address deleted');
    } catch (err) {
      showNotification(err.message || 'Failed to delete address', 'error');
    }
  };

  // =============================================================================
  // STATS CALCULATIONS
  // =============================================================================
  const stats = {
    totalOrders: orders.length,
    pendingOrders: orders.filter(o => o.status === 'processing' || o.status === 'shipped').length,
    totalSpent: orders.reduce((sum, o) => sum + (o.total || 0), 0),
  };

  // =============================================================================
  // RENDER: DASHBOARD TAB
  // =============================================================================
  const renderDashboard = () => (
    <div className="space-y-6">
      {/* Welcome Header */}
      <div className="bg-gradient-to-r from-orange-600/20 to-amber-600/10 border border-orange-500/30 rounded-xl p-4 sm:p-6">
        <div className="flex items-center justify-between">
          <div>
            <h1 className="text-xl sm:text-2xl font-bold text-white">Welcome back, {user?.name?.split(' ')[0] || 'Guest'}!</h1>
            <p className="text-zinc-400 mt-1 text-sm">Here's what's happening with your account</p>
          </div>
          <div className="hidden sm:flex items-center gap-2 bg-amber-500/20 text-amber-400 px-3 py-1.5 rounded-full text-sm font-medium">
            <Star className="w-4 h-4 fill-amber-400" />
            Member
          </div>
        </div>
      </div>

      {/* Quick Stats */}
      <div className="grid grid-cols-2 lg:grid-cols-4 gap-3 sm:gap-4">
        {[
          { label: 'Total Orders', value: stats.totalOrders.toString(), subValue: stats.pendingOrders > 0 ? `${stats.pendingOrders} pending` : null, icon: Package, color: 'orange', onClick: () => setActiveTab('orders') },
          { label: 'Total Spent', value: `$${stats.totalSpent.toFixed(2)}`, subValue: null, icon: CreditCard, color: 'green', onClick: () => setActiveTab('orders') },
          { label: 'Addresses', value: addresses.length.toString(), subValue: null, icon: MapPin, color: 'blue', onClick: () => setActiveTab('addresses') },
          { label: 'Member Since', value: user?.created_at ? new Date(user.created_at).getFullYear().toString() : new Date().getFullYear().toString(), subValue: null, icon: User, color: 'purple', onClick: null },
        ].map((stat, i) => (
          <div
            key={i}
            onClick={stat.onClick}
            className={`bg-zinc-900 border border-zinc-800 rounded-xl p-3 sm:p-4 transition-colors ${stat.onClick ? 'hover:border-orange-500/50 cursor-pointer' : ''} group`}
          >
            <div className="flex items-center justify-between mb-2 sm:mb-3">
              <div className={`w-8 h-8 sm:w-10 sm:h-10 rounded-lg flex items-center justify-center ${
                stat.color === 'orange' ? 'bg-orange-500/20' :
                stat.color === 'green' ? 'bg-green-500/20' :
                stat.color === 'blue' ? 'bg-blue-500/20' :
                'bg-purple-500/20'
              }`}>
                <stat.icon className={`w-4 h-4 sm:w-5 sm:h-5 ${
                  stat.color === 'orange' ? 'text-orange-500' :
                  stat.color === 'green' ? 'text-green-500' :
                  stat.color === 'blue' ? 'text-blue-500' :
                  'text-purple-500'
                }`} />
              </div>
              {stat.onClick && <ChevronRight className="w-4 h-4 text-zinc-600 group-hover:text-orange-500 transition-colors" />}
            </div>
            <p className="text-xl sm:text-2xl font-bold text-white">{stat.value}</p>
            <p className="text-xs sm:text-sm text-zinc-500">{stat.label}</p>
            {stat.subValue && (
              <p className="text-xs mt-1 text-orange-400">{stat.subValue}</p>
            )}
          </div>
        ))}
      </div>

      {/* Recent Orders */}
      <div className="bg-zinc-900 border border-zinc-800 rounded-xl overflow-hidden">
        <div className="p-4 border-b border-zinc-800 flex items-center justify-between">
          <h2 className="text-base sm:text-lg font-semibold text-white">Recent Orders</h2>
          <button onClick={() => setActiveTab('orders')} className="text-sm text-orange-500 hover:text-orange-400 flex items-center gap-1 transition-colors">
            View All <ChevronRight className="w-4 h-4" />
          </button>
        </div>

        {loading.orders ? (
          <div className="flex items-center justify-center py-12">
            <Loader2 className="w-6 h-6 animate-spin text-orange-500" />
          </div>
        ) : error.orders ? (
          <div className="p-4 text-center text-red-400">{error.orders}</div>
        ) : orders.length === 0 ? (
          <div className="p-8 text-center">
            <Package className="w-12 h-12 text-zinc-700 mx-auto mb-3" />
            <p className="text-zinc-500">No orders yet</p>
          </div>
        ) : (
          <div className="divide-y divide-zinc-800">
            {orders.slice(0, 3).map((order) => {
              const StatusIcon = getStatusIcon(order.status);
              return (
                <div
                  key={order.id}
                  onClick={() => { setSelectedOrder(order); setActiveTab('orders'); }}
                  className="p-3 sm:p-4 hover:bg-zinc-800/50 transition-colors cursor-pointer"
                >
                  <div className="flex items-center justify-between gap-3">
                    <div className="flex items-center gap-3 min-w-0">
                      <div className="w-10 h-10 sm:w-12 sm:h-12 bg-zinc-800 rounded-lg flex items-center justify-center flex-shrink-0">
                        <Package className="w-5 h-5 sm:w-6 sm:h-6 text-zinc-500" />
                      </div>
                      <div className="min-w-0">
                        <p className="font-medium text-white text-sm sm:text-base truncate">#{order.order_number}</p>
                        <p className="text-xs sm:text-sm text-zinc-500">
                          {order.items?.length || 0} items • {new Date(order.created_at).toLocaleDateString()}
                        </p>
                      </div>
                    </div>
                    <div className="flex items-center gap-2 sm:gap-4 flex-shrink-0">
                      <span className={`px-2 sm:px-3 py-1 rounded-full text-xs font-medium capitalize flex items-center gap-1 ${getStatusColor(order.status)}`}>
                        <StatusIcon className="w-3 h-3" />
                        <span className="hidden sm:inline">{order.status}</span>
                      </span>
                      <span className="text-white font-medium text-sm sm:text-base">${order.total?.toFixed(2)}</span>
                    </div>
                  </div>
                </div>
              );
            })}
          </div>
        )}
      </div>

      {/* Quick Actions */}
      <div className="bg-zinc-900 border border-zinc-800 rounded-xl p-4">
        <h3 className="text-sm font-semibold text-zinc-400 mb-3">Quick Actions</h3>
        <div className="grid grid-cols-2 sm:grid-cols-4 gap-2">
          <button
            onClick={() => setActiveTab('orders')}
            className="flex items-center gap-2 px-3 py-2 bg-zinc-800 hover:bg-zinc-700 rounded-lg text-sm text-zinc-300 transition-colors"
          >
            <Package className="w-4 h-4" />
            View Orders
          </button>
          <button
            onClick={() => setActiveTab('addresses')}
            className="flex items-center gap-2 px-3 py-2 bg-zinc-800 hover:bg-zinc-700 rounded-lg text-sm text-zinc-300 transition-colors"
          >
            <MapPin className="w-4 h-4" />
            Addresses
          </button>
          <button
            onClick={() => setActiveTab('returns')}
            className="flex items-center gap-2 px-3 py-2 bg-zinc-800 hover:bg-zinc-700 rounded-lg text-sm text-zinc-300 transition-colors"
          >
            <RotateCcw className="w-4 h-4" />
            Returns
          </button>
          <button
            onClick={onLogout}
            className="flex items-center gap-2 px-3 py-2 bg-zinc-800 hover:bg-red-500/20 rounded-lg text-sm text-zinc-300 hover:text-red-400 transition-colors"
          >
            <LogOut className="w-4 h-4" />
            Sign Out
          </button>
        </div>
      </div>
    </div>
  );

  // =============================================================================
  // RENDER: ORDER DETAIL
  // =============================================================================
  const renderOrderDetail = () => {
    if (!selectedOrder) return null;
    const order = selectedOrder;

    return (
      <div className="space-y-6">
        {/* Header */}
        <div className="flex items-center gap-4">
          <button
            onClick={() => setSelectedOrder(null)}
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

        {/* Tracking info */}
        {order.status === 'shipped' && order.tracking_number && (
          <div className="p-4 bg-blue-500/10 border border-blue-500/30 rounded-xl">
            <div className="flex items-center gap-2">
              <Truck className="w-5 h-5 text-blue-400" />
              <span className="text-zinc-400">Tracking:</span>
              <span className="text-blue-400 font-mono">{order.tracking_number}</span>
            </div>
          </div>
        )}

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
                  {canRequestRefund(order, item) && (
                    <button
                      onClick={() => setRefundItem({ item, order })}
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

        {/* Actions */}
        <div className="flex flex-wrap gap-2">
          <button className="px-4 py-2 bg-zinc-800 hover:bg-zinc-700 text-white text-sm font-medium rounded-lg transition-colors flex items-center gap-1.5">
            <Download className="w-4 h-4" /> Download Invoice
          </button>
        </div>
      </div>
    );
  };

  // =============================================================================
  // RENDER: ORDERS TAB
  // =============================================================================
  const renderOrders = () => {
    if (selectedOrder) return renderOrderDetail();

    return (
      <div className="space-y-4 sm:space-y-6">
        <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-3">
          <h1 className="text-lg sm:text-xl font-bold text-white">Order History</h1>
          <div className="flex items-center gap-2">
            <div className="relative flex-1 sm:flex-initial">
              <Search className="w-4 h-4 absolute left-3 top-1/2 -translate-y-1/2 text-zinc-500" />
              <input
                type="text"
                placeholder="Search..."
                value={searchQuery}
                onChange={(e) => setSearchQuery(e.target.value)}
                className="w-full sm:w-auto pl-9 pr-3 py-2 bg-zinc-900 border border-zinc-800 rounded-lg text-sm text-white placeholder-zinc-500 focus:outline-none focus:border-orange-500"
              />
            </div>
            <select
              value={statusFilter}
              onChange={(e) => setStatusFilter(e.target.value)}
              className="bg-zinc-900 border border-zinc-800 rounded-lg px-3 py-2 text-sm text-white focus:outline-none focus:border-orange-500"
            >
              <option value="all">All</option>
              <option value="pending">Pending</option>
              <option value="processing">Processing</option>
              <option value="shipped">Shipped</option>
              <option value="delivered">Delivered</option>
            </select>
          </div>
        </div>

        {loading.orders ? (
          <div className="flex items-center justify-center py-12">
            <Loader2 className="w-8 h-8 animate-spin text-orange-500" />
          </div>
        ) : error.orders ? (
          <div className="text-center py-12">
            <AlertCircle className="w-12 h-12 text-red-500 mx-auto mb-4" />
            <p className="text-red-400 mb-4">{error.orders}</p>
            <button
              onClick={fetchOrders}
              className="px-4 py-2 bg-orange-500 text-white rounded-lg hover:bg-orange-600"
            >
              Try Again
            </button>
          </div>
        ) : filteredOrders.length === 0 ? (
          <div className="text-center py-12 bg-zinc-900 border border-zinc-800 rounded-xl">
            <Package className="w-16 h-16 text-zinc-700 mx-auto mb-4" />
            <p className="text-zinc-400">{orders.length === 0 ? 'No orders yet' : 'No matching orders'}</p>
          </div>
        ) : (
          <div className="space-y-3 sm:space-y-4">
            {filteredOrders.map((order) => {
              const StatusIcon = getStatusIcon(order.status);
              return (
                <div key={order.id} className="bg-zinc-900 border border-zinc-800 rounded-xl overflow-hidden">
                  <div
                    onClick={() => setSelectedOrder(order)}
                    className="p-3 sm:p-4 border-b border-zinc-800 flex flex-col sm:flex-row sm:items-center justify-between gap-2 cursor-pointer hover:bg-zinc-800/50 transition-colors"
                  >
                    <div>
                      <p className="font-semibold text-white">#{order.order_number}</p>
                      <p className="text-sm text-zinc-500">Placed {new Date(order.created_at).toLocaleDateString()}</p>
                    </div>
                    <div className="flex items-center gap-3">
                      <span className={`px-3 py-1 rounded-full text-xs font-medium capitalize flex items-center gap-1.5 ${getStatusColor(order.status)}`}>
                        <StatusIcon className="w-3 h-3" />
                        {order.status}
                      </span>
                      <span className="text-lg font-semibold text-white">${order.total?.toFixed(2)}</span>
                    </div>
                  </div>

                  {order.status === 'shipped' && order.tracking_number && (
                    <div className="px-3 sm:px-4 py-2 sm:py-3 bg-zinc-800/50 border-b border-zinc-800">
                      <div className="flex items-center gap-2 text-sm">
                        <Truck className="w-4 h-4 text-blue-400" />
                        <span className="text-zinc-400">Tracking:</span>
                        <span className="text-orange-400 font-mono text-xs sm:text-sm">{order.tracking_number}</span>
                      </div>
                    </div>
                  )}

                  <div className="p-3 sm:p-4 flex flex-wrap gap-2">
                    <button
                      onClick={() => setSelectedOrder(order)}
                      className="px-3 sm:px-4 py-2 bg-orange-500 hover:bg-orange-600 text-white text-sm font-medium rounded-lg transition-colors"
                    >
                      View Details
                    </button>
                    <button className="px-3 sm:px-4 py-2 bg-zinc-800 hover:bg-zinc-700 text-white text-sm font-medium rounded-lg transition-colors flex items-center gap-1.5">
                      <Download className="w-4 h-4" /> Invoice
                    </button>
                  </div>
                </div>
              );
            })}
          </div>
        )}
      </div>
    );
  };

  // =============================================================================
  // RENDER: ADDRESSES TAB
  // =============================================================================
  const renderAddresses = () => (
    <div className="space-y-4 sm:space-y-6">
      <div className="flex items-center justify-between">
        <h1 className="text-lg sm:text-xl font-bold text-white">Address Book</h1>
        <button className="px-3 sm:px-4 py-2 bg-orange-500 hover:bg-orange-600 text-white text-sm font-medium rounded-lg transition-colors flex items-center gap-1.5">
          <Plus className="w-4 h-4" /> <span className="hidden sm:inline">Add</span> Address
        </button>
      </div>

      {loading.addresses ? (
        <div className="flex items-center justify-center py-12">
          <Loader2 className="w-8 h-8 animate-spin text-orange-500" />
        </div>
      ) : error.addresses ? (
        <div className="text-center py-12">
          <AlertCircle className="w-12 h-12 text-red-500 mx-auto mb-4" />
          <p className="text-red-400">{error.addresses}</p>
        </div>
      ) : addresses.length === 0 ? (
        <div className="text-center py-12 bg-zinc-900 border border-zinc-800 rounded-xl">
          <MapPin className="w-16 h-16 text-zinc-700 mx-auto mb-4" />
          <p className="text-zinc-400">No addresses saved</p>
          <p className="text-sm text-zinc-500 mt-2">Add an address for faster checkout</p>
        </div>
      ) : (
        <div className="grid sm:grid-cols-2 gap-4">
          {addresses.map((addr) => (
            <div key={addr.id} className={`bg-zinc-900 border rounded-xl p-4 sm:p-5 relative ${addr.is_default ? 'border-orange-500' : 'border-zinc-800'}`}>
              {addr.is_default && (
                <span className="absolute top-3 right-3 bg-orange-500/20 text-orange-400 text-xs font-medium px-2 py-1 rounded-full">
                  Default
                </span>
              )}
              <div className="flex items-start gap-3">
                <div className="w-10 h-10 bg-zinc-800 rounded-lg flex items-center justify-center flex-shrink-0">
                  <MapPin className="w-5 h-5 text-zinc-500" />
                </div>
                <div className="flex-1 min-w-0">
                  <p className="font-semibold text-white">{addr.nickname || addr.address_type || 'Address'}</p>
                  <p className="text-zinc-400 mt-1 text-sm">{addr.contact_name || addr.name}</p>
                  <p className="text-zinc-400 text-sm">{addr.street_address || addr.street}</p>
                  <p className="text-zinc-400 text-sm">{addr.city}, {addr.state} {addr.postal_code || addr.zip}</p>
                </div>
              </div>
              <div className="flex items-center gap-2 mt-4 pt-4 border-t border-zinc-800">
                <button className="px-3 py-1.5 bg-zinc-800 hover:bg-zinc-700 text-white text-sm rounded-lg transition-colors flex items-center gap-1.5">
                  <Edit2 className="w-3.5 h-3.5" /> Edit
                </button>
                {!addr.is_default && (
                  <button
                    onClick={() => handleDeleteAddress(addr.id)}
                    className="px-3 py-1.5 bg-zinc-800 hover:bg-red-500/20 text-zinc-400 hover:text-red-400 text-sm rounded-lg transition-colors flex items-center gap-1.5"
                  >
                    <Trash2 className="w-3.5 h-3.5" /> Delete
                  </button>
                )}
              </div>
            </div>
          ))}
        </div>
      )}
    </div>
  );

  // =============================================================================
  // RENDER: PLACEHOLDER TABS
  // =============================================================================
  const renderPlaceholder = (title, description) => (
    <div className="flex items-center justify-center h-64 bg-zinc-900 border border-zinc-800 rounded-xl">
      <div className="text-center">
        <div className="w-16 h-16 bg-zinc-800 rounded-full flex items-center justify-center mx-auto mb-4">
          {tabs.find(t => t.id === activeTab)?.icon &&
            React.createElement(tabs.find(t => t.id === activeTab).icon, { className: 'w-8 h-8 text-zinc-500' })}
        </div>
        <h2 className="text-xl font-semibold text-white">{title}</h2>
        <p className="text-zinc-500 mt-1">{description || 'Coming soon'}</p>
      </div>
    </div>
  );

  // =============================================================================
  // RENDER: CONTENT ROUTER
  // =============================================================================
  const renderContent = () => {
    switch (activeTab) {
      case 'dashboard': return renderDashboard();
      case 'orders': return renderOrders();
      case 'addresses': return renderAddresses();
      case 'payment': return renderPlaceholder('Payment Methods', 'Manage your payment methods');
      case 'wishlist': return renderPlaceholder('Wishlist', 'Save items for later');
      case 'collection': return renderPlaceholder('My Collection', 'Track your comic collection');
      case 'rewards': return renderPlaceholder('Rewards', 'Earn and redeem rewards');
      case 'returns': return renderPlaceholder('Returns', 'View and manage return requests');
      default: return renderDashboard();
    }
  };

  // =============================================================================
  // MAIN RENDER
  // =============================================================================
  return (
    <div className="fixed inset-0 z-50 bg-zinc-950 text-white overflow-hidden" style={{ fontFamily: "'Barlow', sans-serif" }}>
      {/* Notification */}
      {notification && (
        <div className={`fixed top-4 right-4 z-[70] px-6 py-3 rounded-lg shadow-xl ${
          notification.type === 'error' ? 'bg-red-600' : 'bg-green-600'
        } text-white font-semibold`}>
          {notification.message}
        </div>
      )}

      {/* Header */}
      <header className="bg-zinc-900 border-b border-zinc-800 sticky top-0 z-50">
        {/* Top Bar */}
        <div className="px-4 h-12 sm:h-14 flex items-center justify-between border-b border-zinc-800/50">
          <div className="flex items-center gap-2 sm:gap-3">
            <button
              onClick={onClose}
              className="p-1.5 hover:bg-zinc-800 rounded-lg transition-colors"
            >
              <ArrowLeft className="w-5 h-5 text-zinc-400" />
            </button>
            <div className="w-8 h-8 sm:w-9 sm:h-9 bg-zinc-800 border border-orange-500/30 rounded-lg flex items-center justify-center shadow-lg shadow-orange-500/10">
              <span className="font-comic text-base sm:text-lg text-orange-500">M</span>
            </div>
            <div className="flex items-center gap-2">
              <span className="font-comic text-base sm:text-lg text-orange-500">MDM COMICS</span>
              <span className="text-zinc-600">/</span>
              <span className="text-zinc-400 text-sm">My Account</span>
            </div>
          </div>

          <div className="flex items-center gap-1 sm:gap-2">
            <button className="relative p-2 hover:bg-zinc-800 rounded-lg transition-colors">
              <Bell className="w-4 h-4 sm:w-5 sm:h-5 text-zinc-400" />
            </button>
            <button className="p-2 hover:bg-zinc-800 rounded-lg transition-colors">
              <Settings className="w-4 h-4 sm:w-5 sm:h-5 text-zinc-400" />
            </button>
            <div className="w-7 h-7 sm:w-8 sm:h-8 bg-orange-500 rounded-full flex items-center justify-center font-semibold text-xs sm:text-sm cursor-pointer hover:ring-2 hover:ring-orange-500/50 transition-all ml-1">
              {user?.name?.charAt(0).toUpperCase() || 'U'}
            </div>
          </div>
        </div>

        {/* Tab Navigation */}
        <nav className="flex items-center overflow-x-auto hide-scrollbar px-2 sm:px-4">
          {tabs.map((tab) => (
            <button
              key={tab.id}
              onClick={() => { setActiveTab(tab.id); setSelectedOrder(null); }}
              className={`flex items-center gap-1.5 sm:gap-2 px-3 sm:px-4 py-3 text-sm font-medium border-b-2 transition-colors whitespace-nowrap flex-shrink-0 ${
                activeTab === tab.id
                  ? 'border-orange-500 text-orange-500'
                  : 'border-transparent text-zinc-400 hover:text-white hover:border-zinc-600'
              }`}
            >
              <tab.icon className="w-4 h-4" />
              <span>{tab.label}</span>
              {tab.badge && (
                <span className={`px-1.5 py-0.5 rounded-full text-xs ${
                  activeTab === tab.id ? 'bg-orange-500 text-white' : 'bg-zinc-800 text-zinc-400'
                }`}>
                  {tab.badge}
                </span>
              )}
            </button>
          ))}
        </nav>
      </header>

      {/* Main Content */}
      <main className="h-[calc(100vh-7rem)] overflow-auto">
        <div className="max-w-6xl mx-auto px-3 sm:px-4 py-4 sm:py-6">
          {renderContent()}
        </div>
      </main>

      {/* Refund Modal */}
      {refundItem && (
        <RefundRequestModal
          item={refundItem.item}
          order={refundItem.order}
          onClose={() => setRefundItem(null)}
          onSuccess={() => {
            setRefundItem(null);
            showNotification('Refund request submitted successfully!');
            fetchOrders();
          }}
        />
      )}

      {/* Custom scrollbar hiding CSS */}
      <style>{`
        .hide-scrollbar::-webkit-scrollbar { display: none; }
        .hide-scrollbar { -ms-overflow-style: none; scrollbar-width: none; }
      `}</style>
    </div>
  );
}
