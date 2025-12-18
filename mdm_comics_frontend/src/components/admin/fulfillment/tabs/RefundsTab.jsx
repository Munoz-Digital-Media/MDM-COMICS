/**
 * RefundsTab - Refunds workflow management
 */

import React, { useState, useEffect, useCallback } from 'react';
import { CheckCircle, X, Truck, DollarSign, Clock, Eye } from 'lucide-react';
import StatusBadge from '../shared/StatusBadge';
import FilterBar from '../shared/FilterBar';
import Pagination from '../shared/Pagination';
import EmptyState from '../shared/EmptyState';
import RefundDetailPanel from '../components/RefundDetailPanel';

const REFUND_STATES = [
  { value: '', label: 'All States' },
  { value: 'REQUESTED', label: 'Requested' },
  { value: 'UNDER_REVIEW', label: 'Under Review' },
  { value: 'APPROVED', label: 'Approved' },
  { value: 'DENIED', label: 'Denied' },
  { value: 'VENDOR_RETURN_INITIATED', label: 'Return Initiated' },
  { value: 'VENDOR_CREDIT_PENDING', label: 'Credit Pending' },
  { value: 'VENDOR_CREDIT_RECEIVED', label: 'Credit Received' },
  { value: 'CUSTOMER_REFUND_PROCESSING', label: 'Refund Processing' },
  { value: 'COMPLETED', label: 'Completed' },
];

export default function RefundsTab({ announce }) {
  const [refunds, setRefunds] = useState([]);
  const [stats, setStats] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [selectedRefund, setSelectedRefund] = useState(null);
  const [filter, setFilter] = useState({
    state: '',
    page: 1,
    pageSize: 20,
  });
  const [total, setTotal] = useState(0);

  const API_BASE = import.meta.env.VITE_API_URL || 'http://localhost:8000/api';

  const fetchRefunds = useCallback(async () => {
    setLoading(true);
    setError(null);

    try {
      const offset = (filter.page - 1) * filter.pageSize;
      const params = new URLSearchParams({
        limit: filter.pageSize,
        offset,
        ...(filter.state && { state: filter.state }),
      });

      const [refundsRes, statsRes] = await Promise.all([
        fetch(`${API_BASE}/admin/refunds?${params}`, { credentials: 'include' }),
        fetch(`${API_BASE}/admin/refunds/stats`, { credentials: 'include' }),
      ]);

      if (!refundsRes.ok) throw new Error('Failed to fetch refunds');

      const refundsData = await refundsRes.json();
      const statsData = statsRes.ok ? await statsRes.json() : null;

      setRefunds(refundsData.items || []);
      setTotal(refundsData.total || 0);
      setStats(statsData);
      announce?.(`Loaded ${refundsData.items?.length || 0} refunds`);
    } catch (err) {
      setError(err.message);
      announce?.('Error loading refunds');
    } finally {
      setLoading(false);
    }
  }, [filter, API_BASE, announce]);

  useEffect(() => {
    fetchRefunds();
  }, [fetchRefunds]);

  const handleFilterChange = (name, value) => {
    setFilter(prev => ({ ...prev, [name]: value, page: 1 }));
  };

  const formatDate = (dateStr) => {
    return new Date(dateStr).toLocaleDateString('en-US', {
      month: 'short',
      day: 'numeric',
      year: 'numeric',
    });
  };

  const getRefundNumber = (refund) => {
    return refund.refund_number || `REF-${String(refund.id).padStart(4, '0')}`;
  };

  const canTakeAction = (state) => {
    return ['REQUESTED', 'UNDER_REVIEW', 'APPROVED', 'VENDOR_CREDIT_RECEIVED'].includes(state);
  };

  return (
    <div className="refunds-tab">
      {/* Stats Cards */}
      {stats && (
        <div className="grid grid-cols-2 md:grid-cols-4 gap-3 mb-4">
          <div className="bg-yellow-500/10 border border-yellow-500/30 rounded-lg p-3">
            <p className="text-2xl font-bold text-yellow-400">{stats.pending_review || 0}</p>
            <p className="text-xs text-yellow-400/80">Pending Review</p>
          </div>
          <div className="bg-purple-500/10 border border-purple-500/30 rounded-lg p-3">
            <p className="text-2xl font-bold text-purple-400">{stats.pending_vendor_credit || 0}</p>
            <p className="text-xs text-purple-400/80">Awaiting Credit</p>
          </div>
          <div className="bg-green-500/10 border border-green-500/30 rounded-lg p-3">
            <p className="text-2xl font-bold text-green-400">{stats.ready_for_refund || 0}</p>
            <p className="text-xs text-green-400/80">Ready to Refund</p>
          </div>
          <div className="bg-blue-500/10 border border-blue-500/30 rounded-lg p-3">
            <p className="text-2xl font-bold text-blue-400">
              ${parseFloat(stats.total_refunded_amount || 0).toFixed(0)}
            </p>
            <p className="text-xs text-blue-400/80">Total Refunded</p>
          </div>
        </div>
      )}

      <FilterBar
        filters={[
          {
            type: 'select',
            name: 'state',
            label: 'Filter by state',
            options: REFUND_STATES,
            value: filter.state,
          },
        ]}
        onFilterChange={handleFilterChange}
      />

      {error && (
        <div role="alert" className="bg-red-500/10 border border-red-500/30 rounded-lg p-4 mb-4">
          <p className="text-red-400">{error}</p>
          <button onClick={fetchRefunds} className="mt-2 text-sm text-red-400 hover:text-red-300 underline">
            Retry
          </button>
        </div>
      )}

      {loading && (
        <div role="status" className="py-12 text-center">
          <div className="animate-spin w-8 h-8 border-2 border-orange-500 border-t-transparent rounded-full mx-auto mb-4" />
          <p className="text-zinc-400">Loading refunds...</p>
        </div>
      )}

      {!loading && refunds.length === 0 && (
        <EmptyState
          icon="RefreshCw"
          title="No refund requests"
          description={filter.state ? "Try adjusting your filters" : "Refund requests will appear here when customers request them"}
        />
      )}

      {!loading && refunds.length > 0 && (
        <>
          <div className="overflow-x-auto">
            <table className="w-full" role="table">
              <thead>
                <tr className="text-left text-xs text-zinc-500 uppercase tracking-wider border-b border-zinc-800">
                  <th className="px-4 py-3">Refund #</th>
                  <th className="px-4 py-3">Order</th>
                  <th className="px-4 py-3">Reason</th>
                  <th className="px-4 py-3">Amount</th>
                  <th className="px-4 py-3">Date</th>
                  <th className="px-4 py-3">State</th>
                  <th className="px-4 py-3">Actions</th>
                </tr>
              </thead>
              <tbody>
                {refunds.map((refund) => (
                  <tr
                    key={refund.id}
                    className="border-b border-zinc-800/50 hover:bg-zinc-800/30 transition-colors"
                  >
                    <td className="px-4 py-3">
                      <span className="font-mono text-sm text-orange-400">
                        {getRefundNumber(refund)}
                      </span>
                    </td>
                    <td className="px-4 py-3">
                      <span className="font-mono text-sm text-zinc-400">
                        #{refund.order_id}
                      </span>
                    </td>
                    <td className="px-4 py-3">
                      <span className="text-sm text-white capitalize">
                        {(refund.reason_code || 'other').replace(/_/g, ' ')}
                      </span>
                    </td>
                    <td className="px-4 py-3">
                      <span className="text-sm font-medium text-white">
                        ${parseFloat(refund.refund_amount || refund.original_amount || 0).toFixed(2)}
                      </span>
                    </td>
                    <td className="px-4 py-3">
                      <span className="text-sm text-zinc-400">{formatDate(refund.created_at)}</span>
                    </td>
                    <td className="px-4 py-3">
                      <StatusBadge status={refund.state} />
                    </td>
                    <td className="px-4 py-3">
                      <button
                        onClick={() => setSelectedRefund(refund)}
                        className={`px-3 py-1 rounded text-xs font-medium transition-colors ${
                          canTakeAction(refund.state)
                            ? 'bg-orange-500/20 text-orange-400 hover:bg-orange-500/30'
                            : 'bg-zinc-700/50 text-zinc-400 hover:bg-zinc-700'
                        }`}
                      >
                        {canTakeAction(refund.state) ? 'Take Action' : 'View'}
                      </button>
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

      {selectedRefund && (
        <RefundDetailPanel
          refund={selectedRefund}
          onClose={() => setSelectedRefund(null)}
          onUpdate={fetchRefunds}
          announce={announce}
        />
      )}
    </div>
  );
}
