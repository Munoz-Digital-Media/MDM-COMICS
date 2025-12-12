/**
 * OrderList - Order management table with status updates
 * Phase 3: MDM Admin Console Inventory System v1.3.0
 */
import React, { useState, useEffect, useCallback } from 'react';
import {
  Search, ChevronLeft, ChevronRight, Loader2,
  Package, Truck, CheckCircle, X, Eye, AlertTriangle
} from 'lucide-react';
import { adminAPI } from '../../../services/adminApi';

const STATUS_OPTIONS = [
  { value: '', label: 'All Statuses' },
  { value: 'pending', label: 'Pending' },
  { value: 'paid', label: 'Paid' },
  { value: 'shipped', label: 'Shipped' },
  { value: 'delivered', label: 'Delivered' },
  { value: 'cancelled', label: 'Cancelled' },
];

function StatusBadge({ status }) {
  const styles = {
    pending: 'bg-yellow-500/20 text-yellow-400',
    paid: 'bg-green-500/20 text-green-400',
    shipped: 'bg-blue-500/20 text-blue-400',
    delivered: 'bg-purple-500/20 text-purple-400',
    cancelled: 'bg-red-500/20 text-red-400',
  };

  return (
    <span className={`px-2 py-1 text-xs rounded-full capitalize ${styles[status] || 'bg-zinc-500/20 text-zinc-400'}`}>
      {status}
    </span>
  );
}

function OrderDetailModal({ order, onClose, onStatusUpdate }) {
  const [newStatus, setNewStatus] = useState(order.status);
  const [trackingNumber, setTrackingNumber] = useState(order.tracking_number || '');
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);

  const handleUpdateStatus = async () => {
    if (newStatus === order.status && trackingNumber === (order.tracking_number || '')) return;

    setLoading(true);
    setError(null);

    try {
      await adminAPI.updateOrderStatus(order.id, newStatus, trackingNumber || null);
      onStatusUpdate();
    } catch (err) {
      setError(err.message);
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center">
      <div className="absolute inset-0 bg-black/60" onClick={onClose} />
      <div className="relative bg-zinc-900 border border-zinc-800 rounded-xl p-6 w-full max-w-lg max-h-[80vh] overflow-auto">
        <div className="flex items-center justify-between mb-4">
          <h3 className="text-lg font-semibold text-white">Order Details</h3>
          <button onClick={onClose} className="p-1 hover:bg-zinc-800 rounded">
            <X className="w-5 h-5 text-zinc-400" />
          </button>
        </div>

        {/* Order Info */}
        <div className="space-y-4 mb-6">
          <div className="flex items-center justify-between">
            <span className="text-sm text-zinc-400">Order Number</span>
            <span className="text-sm font-mono text-white">{order.order_number || `#${order.id}`}</span>
          </div>
          <div className="flex items-center justify-between">
            <span className="text-sm text-zinc-400">Customer</span>
            <span className="text-sm text-white">{order.customer_email}</span>
          </div>
          <div className="flex items-center justify-between">
            <span className="text-sm text-zinc-400">Date</span>
            <span className="text-sm text-white">{new Date(order.created_at).toLocaleString()}</span>
          </div>
          <div className="flex items-center justify-between">
            <span className="text-sm text-zinc-400">Total</span>
            <span className="text-sm font-semibold text-orange-400">${order.total?.toFixed(2)}</span>
          </div>
          <div className="flex items-center justify-between">
            <span className="text-sm text-zinc-400">Status</span>
            <StatusBadge status={order.status} />
          </div>
        </div>

        {/* Items */}
        {order.items && order.items.length > 0 && (
          <div className="mb-6">
            <h4 className="text-sm font-semibold text-zinc-400 mb-2">Items</h4>
            <div className="space-y-2">
              {order.items.map((item, idx) => (
                <div key={idx} className="flex items-center justify-between p-2 bg-zinc-800/50 rounded">
                  <span className="text-sm text-white truncate flex-1">{item.product_name || item.name}</span>
                  <span className="text-xs text-zinc-400 ml-2">x{item.quantity}</span>
                  <span className="text-sm text-zinc-300 ml-4">${(item.price * item.quantity).toFixed(2)}</span>
                </div>
              ))}
            </div>
          </div>
        )}

        {/* Status Update */}
        <div className="border-t border-zinc-800 pt-4 space-y-4">
          <h4 className="text-sm font-semibold text-zinc-400">Update Status</h4>

          {error && (
            <div className="bg-red-500/10 border border-red-500/20 rounded-lg p-3">
              <p className="text-sm text-red-400">{error}</p>
            </div>
          )}

          <div>
            <label className="block text-sm text-zinc-400 mb-1">Status</label>
            <select
              value={newStatus}
              onChange={(e) => setNewStatus(e.target.value)}
              className="w-full bg-zinc-800 border border-zinc-700 rounded-lg px-3 py-2 text-white focus:outline-none focus:border-orange-500"
            >
              <option value="pending">Pending</option>
              <option value="paid">Paid</option>
              <option value="shipped">Shipped</option>
              <option value="delivered">Delivered</option>
              <option value="cancelled">Cancelled</option>
            </select>
          </div>

          {(newStatus === 'shipped' || order.status === 'shipped') && (
            <div>
              <label className="block text-sm text-zinc-400 mb-1">Tracking Number</label>
              <input
                type="text"
                value={trackingNumber}
                onChange={(e) => setTrackingNumber(e.target.value)}
                placeholder="e.g., 1Z999AA10123456784"
                className="w-full bg-zinc-800 border border-zinc-700 rounded-lg px-3 py-2 text-white placeholder-zinc-500 focus:outline-none focus:border-orange-500"
              />
            </div>
          )}

          <div className="flex gap-3">
            <button
              onClick={onClose}
              className="flex-1 px-4 py-2 bg-zinc-800 text-zinc-300 rounded-lg hover:bg-zinc-700"
            >
              Cancel
            </button>
            <button
              onClick={handleUpdateStatus}
              disabled={loading || (newStatus === order.status && trackingNumber === (order.tracking_number || ''))}
              className="flex-1 px-4 py-2 bg-orange-500 text-white rounded-lg hover:bg-orange-600 disabled:opacity-50 disabled:cursor-not-allowed"
            >
              {loading ? 'Updating...' : 'Update Status'}
            </button>
          </div>
        </div>
      </div>
    </div>
  );
}

export default function OrderList() {
  const [orders, setOrders] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [search, setSearch] = useState('');
  const [statusFilter, setStatusFilter] = useState('');
  const [offset, setOffset] = useState(0);
  const [total, setTotal] = useState(0);
  const [selectedOrder, setSelectedOrder] = useState(null);

  const limit = 25;

  const fetchOrders = useCallback(async () => {
    setLoading(true);
    setError(null);

    try {
      const data = await adminAPI.getOrders({
        search,
        status: statusFilter || undefined,
        limit,
        offset,
      });
      setOrders(data.items || []);
      setTotal(data.total || 0);
    } catch (err) {
      setError(err.message);
    } finally {
      setLoading(false);
    }
  }, [search, statusFilter, offset]);

  useEffect(() => {
    fetchOrders();
  }, [fetchOrders]);

  // Debounced search - resets offset on search change
  useEffect(() => {
    const timer = setTimeout(() => {
      setOffset(0);
    }, 300);
    return () => clearTimeout(timer);
  }, [search]);

  const handleViewOrder = async (order) => {
    try {
      const fullOrder = await adminAPI.getOrder(order.id);
      setSelectedOrder(fullOrder);
    } catch (err) {
      alert('Failed to load order: ' + err.message);
    }
  };

  const totalPages = Math.ceil(total / limit);
  const currentPage = Math.floor(offset / limit) + 1;

  return (
    <div className="space-y-4">
      {/* Filters */}
      <div className="flex flex-wrap gap-3">
        <div className="relative flex-1 min-w-[200px]">
          <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-zinc-500" />
          <input
            type="text"
            value={search}
            onChange={(e) => setSearch(e.target.value)}
            placeholder="Search by order number or email..."
            className="w-full pl-10 pr-4 py-2 bg-zinc-900 border border-zinc-800 rounded-lg text-white placeholder-zinc-500 focus:outline-none focus:border-orange-500"
          />
        </div>

        <select
          value={statusFilter}
          onChange={(e) => { setStatusFilter(e.target.value); setOffset(0); }}
          className="px-3 py-2 bg-zinc-900 border border-zinc-800 rounded-lg text-zinc-300 focus:outline-none focus:border-orange-500"
        >
          {STATUS_OPTIONS.map(opt => (
            <option key={opt.value} value={opt.value}>{opt.label}</option>
          ))}
        </select>
      </div>

      {/* Error state */}
      {error && (
        <div className="bg-red-500/10 border border-red-500/20 rounded-lg p-4">
          <p className="text-sm text-red-400">{error}</p>
        </div>
      )}

      {/* Orders table */}
      <div className="bg-zinc-900 border border-zinc-800 rounded-xl overflow-hidden">
        {loading ? (
          <div className="flex items-center justify-center py-12">
            <Loader2 className="w-6 h-6 text-orange-500 animate-spin" />
          </div>
        ) : orders.length === 0 ? (
          <div className="text-center py-12">
            <Package className="w-12 h-12 text-zinc-700 mx-auto mb-3" />
            <p className="text-zinc-500">No orders found</p>
          </div>
        ) : (
          <div className="overflow-x-auto">
            <table className="w-full">
              <thead>
                <tr className="border-b border-zinc-800 text-left">
                  <th className="px-4 py-3 text-xs font-semibold text-zinc-500 uppercase">Order</th>
                  <th className="px-4 py-3 text-xs font-semibold text-zinc-500 uppercase">Customer</th>
                  <th className="px-4 py-3 text-xs font-semibold text-zinc-500 uppercase">Date</th>
                  <th className="px-4 py-3 text-xs font-semibold text-zinc-500 uppercase">Items</th>
                  <th className="px-4 py-3 text-xs font-semibold text-zinc-500 uppercase">Total</th>
                  <th className="px-4 py-3 text-xs font-semibold text-zinc-500 uppercase">Status</th>
                  <th className="px-4 py-3 text-xs font-semibold text-zinc-500 uppercase">Actions</th>
                </tr>
              </thead>
              <tbody>
                {orders.map(order => (
                  <tr
                    key={order.id}
                    className="border-b border-zinc-800/50 hover:bg-zinc-800/30"
                  >
                    <td className="px-4 py-3">
                      <span className="text-sm font-mono text-white">{order.order_number || `#${order.id}`}</span>
                    </td>
                    <td className="px-4 py-3">
                      <span className="text-sm text-zinc-300">{order.customer_email}</span>
                    </td>
                    <td className="px-4 py-3">
                      <span className="text-sm text-zinc-400">
                        {new Date(order.created_at).toLocaleDateString()}
                      </span>
                    </td>
                    <td className="px-4 py-3">
                      <span className="text-sm text-zinc-400">{order.item_count || '—'}</span>
                    </td>
                    <td className="px-4 py-3">
                      <span className="text-sm font-medium text-white">${order.total?.toFixed(2)}</span>
                    </td>
                    <td className="px-4 py-3">
                      <StatusBadge status={order.status} />
                    </td>
                    <td className="px-4 py-3">
                      <button
                        onClick={() => handleViewOrder(order)}
                        className="p-1.5 hover:bg-zinc-700 rounded text-zinc-400 hover:text-white"
                        title="View Order"
                      >
                        <Eye className="w-4 h-4" />
                      </button>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}

        {/* Pagination */}
        {total > limit && (
          <div className="flex items-center justify-between px-4 py-3 border-t border-zinc-800">
            <p className="text-sm text-zinc-500">
              Showing {offset + 1} - {Math.min(offset + limit, total)} of {total}
            </p>
            <div className="flex items-center gap-2">
              <button
                onClick={() => setOffset(o => Math.max(0, o - limit))}
                disabled={offset === 0}
                className="p-1.5 hover:bg-zinc-800 rounded disabled:opacity-50"
              >
                <ChevronLeft className="w-4 h-4 text-zinc-400" />
              </button>
              <span className="text-sm text-zinc-400">
                Page {currentPage} of {totalPages}
              </span>
              <button
                onClick={() => setOffset(o => o + limit)}
                disabled={offset + limit >= total}
                className="p-1.5 hover:bg-zinc-800 rounded disabled:opacity-50"
              >
                <ChevronRight className="w-4 h-4 text-zinc-400" />
              </button>
            </div>
          </div>
        )}
      </div>

      {/* Order Detail Modal */}
      {selectedOrder && (
        <OrderDetailModal
          order={selectedOrder}
          onClose={() => setSelectedOrder(null)}
          onStatusUpdate={() => { setSelectedOrder(null); fetchOrders(); }}
        />
      )}
    </div>
  );
}
