/**
 * OrdersTab - Orders list with fulfillment actions
 *
 * Per constitution_ui.json:
 * - WCAG 2.2 AA compliant
 * - Full keyboard navigation
 * - ARIA labels and live regions
 */

import { API_BASE } from '../../../../config/api.config.js';
import React, { useState, useEffect, useCallback, useRef } from 'react';
import { Truck, Printer, Mail, MoreHorizontal, ExternalLink } from 'lucide-react';
import StatusBadge from '../shared/StatusBadge';
import FilterBar from '../shared/FilterBar';
import Pagination from '../shared/Pagination';
import EmptyState from '../shared/EmptyState';
import OrderDetailPanel from '../components/OrderDetailPanel';

const ORDER_STATUSES = [
  { value: '', label: 'All Orders' },
  { value: 'pending', label: 'Pending' },
  { value: 'paid', label: 'Awaiting Fulfillment' },
  { value: 'shipped', label: 'Shipped' },
  { value: 'delivered', label: 'Delivered' },
  { value: 'cancelled', label: 'Cancelled' },
];

export default function OrdersTab({ announce }) {
  const [orders, setOrders] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [selectedOrderId, setSelectedOrderId] = useState(null);
  const [filter, setFilter] = useState({
    status: '',
    search: '',
    page: 1,
    pageSize: 25,
  });
  const [total, setTotal] = useState(0);

  const listRef = useRef(null);
  const firstRowRef = useRef(null);

  

  const fetchOrders = useCallback(async () => {
    setLoading(true);
    setError(null);

    try {
      const offset = (filter.page - 1) * filter.pageSize;
      const params = new URLSearchParams({
        limit: filter.pageSize,
        offset,
        ...(filter.status && { status: filter.status }),
        ...(filter.search && { search: filter.search }),
      });

      const response = await fetch(`${API_BASE}/admin/orders/?${params}`, {
        credentials: 'include',
      });

      if (!response.ok) throw new Error('Failed to fetch orders');

      const data = await response.json();
      setOrders(data.items || []);
      setTotal(data.total || 0);
      announce?.(`Loaded ${data.items?.length || 0} orders`);
    } catch (err) {
      setError(err.message);
      announce?.('Error loading orders');
    } finally {
      setLoading(false);
    }
  }, [filter, API_BASE, announce]);

  useEffect(() => {
    fetchOrders();
  }, [fetchOrders]);

  useEffect(() => {
    if (!loading && orders.length > 0 && firstRowRef.current) {
      firstRowRef.current.focus();
    }
  }, [loading, orders]);

  const handleFilterChange = (name, value) => {
    setFilter(prev => ({ ...prev, [name]: value, page: 1 }));
  };

  const handleKeyDown = (e, order, index) => {
    const rows = listRef.current?.querySelectorAll('[data-row]');

    switch (e.key) {
      case 'ArrowDown':
        e.preventDefault();
        if (index < rows.length - 1) rows[index + 1].focus();
        break;
      case 'ArrowUp':
        e.preventDefault();
        if (index > 0) rows[index - 1].focus();
        break;
      case 'Enter':
      case ' ':
        e.preventDefault();
        setSelectedOrderId(order.id);
        break;
      default:
        break;
    }
  };

  const formatDate = (dateStr) => {
    return new Date(dateStr).toLocaleDateString('en-US', {
      month: 'short',
      day: 'numeric',
      year: 'numeric',
    });
  };

  const formatOrderNumber = (order) => {
    return order.order_number || `MDM-${String(order.id).padStart(6, '0')}`;
  };

  return (
    <div className="orders-tab">
      <FilterBar
        filters={[
          {
            type: 'select',
            name: 'status',
            label: 'Filter by status',
            options: ORDER_STATUSES,
            value: filter.status,
          },
          {
            type: 'search',
            name: 'search',
            label: 'Search orders',
            placeholder: 'Order # or customer email...',
            value: filter.search,
          },
        ]}
        onFilterChange={handleFilterChange}
      />

      {error && (
        <div role="alert" className="bg-red-500/10 border border-red-500/30 rounded-lg p-4 mb-4">
          <p className="text-red-400">{error}</p>
          <button
            onClick={fetchOrders}
            className="mt-2 text-sm text-red-400 hover:text-red-300 underline"
          >
            Retry
          </button>
        </div>
      )}

      {loading && (
        <div role="status" className="py-12 text-center">
          <div className="animate-spin w-8 h-8 border-2 border-orange-500 border-t-transparent rounded-full mx-auto mb-4" />
          <p className="text-zinc-400">Loading orders...</p>
        </div>
      )}

      {!loading && orders.length === 0 && (
        <EmptyState
          icon="ShoppingCart"
          title="No orders found"
          description={filter.status || filter.search
            ? "Try adjusting your filters"
            : "Orders will appear here when customers place them"}
        />
      )}

      {!loading && orders.length > 0 && (
        <>
          <div ref={listRef} className="overflow-x-auto">
            <table className="w-full" role="table">
              <thead>
                <tr className="text-left text-xs text-zinc-500 uppercase tracking-wider border-b border-zinc-800">
                  <th className="px-4 py-3">Order</th>
                  <th className="px-4 py-3">Customer</th>
                  <th className="px-4 py-3">Date</th>
                  <th className="px-4 py-3">Items</th>
                  <th className="px-4 py-3">Total</th>
                  <th className="px-4 py-3">Status</th>
                  <th className="px-4 py-3">Actions</th>
                </tr>
              </thead>
              <tbody>
                {orders.map((order, index) => (
                  <tr
                    key={order.id}
                    ref={index === 0 ? firstRowRef : null}
                    data-row
                    tabIndex={0}
                    onClick={() => setSelectedOrderId(order.id)}
                    onKeyDown={(e) => handleKeyDown(e, order, index)}
                    className="border-b border-zinc-800/50 hover:bg-zinc-800/30 cursor-pointer focus:outline-none focus:bg-zinc-800/50 focus:ring-1 focus:ring-orange-500/50 transition-colors"
                  >
                    <td className="px-4 py-3">
                      <span className="font-mono text-sm text-orange-400">
                        {formatOrderNumber(order)}
                      </span>
                    </td>
                    <td className="px-4 py-3">
                      <span className="text-sm text-white">{order.customer_email}</span>
                    </td>
                    <td className="px-4 py-3">
                      <span className="text-sm text-zinc-400">{formatDate(order.created_at)}</span>
                    </td>
                    <td className="px-4 py-3">
                      <span className="text-sm text-zinc-400">{order.item_count || '—'}</span>
                    </td>
                    <td className="px-4 py-3">
                      <span className="text-sm font-medium text-white">
                        ${parseFloat(order.total || 0).toFixed(2)}
                      </span>
                    </td>
                    <td className="px-4 py-3">
                      <StatusBadge status={order.status} />
                    </td>
                    <td className="px-4 py-3">
                      <div className="flex items-center gap-1">
                        <button
                          onClick={(e) => { e.stopPropagation(); setSelectedOrderId(order.id); }}
                          className="p-1.5 rounded hover:bg-zinc-700 text-zinc-400 hover:text-white transition-colors"
                          title="View Details"
                        >
                          <ExternalLink className="w-4 h-4" />
                        </button>
                        {order.status === 'paid' && (
                          <button
                            onClick={(e) => { e.stopPropagation(); /* ship action */ }}
                            className="p-1.5 rounded hover:bg-zinc-700 text-zinc-400 hover:text-orange-400 transition-colors"
                            title="Create Shipment"
                          >
                            <Truck className="w-4 h-4" />
                          </button>
                        )}
                      </div>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>

          <Pagination
            page={filter.page}
            pageSize={filter.pageSize}
            total={total}
            onPageChange={(page) => setFilter(prev => ({ ...prev, page }))}
          />
        </>
      )}

      {selectedOrderId && (
        <OrderDetailPanel
          orderId={selectedOrderId}
          onClose={() => setSelectedOrderId(null)}
          onUpdate={fetchOrders}
          announce={announce}
        />
      )}
    </div>
  );
}
