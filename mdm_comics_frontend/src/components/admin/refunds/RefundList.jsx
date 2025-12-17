/**
 * RefundList - Admin refund management dashboard
 * BCW Refund Request Module v1.0.0
 *
 * Features:
 * - List all refund requests with filtering
 * - Approve/deny refunds
 * - Record vendor credits
 * - Process customer refunds (GATED by vendor credit)
 *
 * CRITICAL: Customer refund can ONLY be processed after vendor credit is received.
 */
import React, { useState, useEffect, useCallback } from 'react';
import {
  Search, ChevronLeft, ChevronRight, Loader2,
  CheckCircle, X, Eye, AlertTriangle, DollarSign,
  Truck, Package, Clock, Ban, RefreshCw
} from 'lucide-react';
import { API_BASE } from '../../../services/api';

// Refund states with styling
const REFUND_STATES = {
  REQUESTED: { label: 'Requested', color: 'bg-yellow-500/20 text-yellow-400', icon: Clock },
  UNDER_REVIEW: { label: 'Under Review', color: 'bg-blue-500/20 text-blue-400', icon: Eye },
  APPROVED: { label: 'Approved', color: 'bg-green-500/20 text-green-400', icon: CheckCircle },
  DENIED: { label: 'Denied', color: 'bg-red-500/20 text-red-400', icon: Ban },
  VENDOR_RETURN_INITIATED: { label: 'Return Initiated', color: 'bg-orange-500/20 text-orange-400', icon: Truck },
  VENDOR_RETURN_IN_TRANSIT: { label: 'Return In Transit', color: 'bg-orange-500/20 text-orange-400', icon: Truck },
  VENDOR_RETURN_RECEIVED: { label: 'Return Received', color: 'bg-purple-500/20 text-purple-400', icon: Package },
  VENDOR_CREDIT_PENDING: { label: 'Credit Pending', color: 'bg-purple-500/20 text-purple-400', icon: Clock },
  VENDOR_CREDIT_RECEIVED: { label: 'Credit Received', color: 'bg-emerald-500/20 text-emerald-400', icon: DollarSign },
  CUSTOMER_REFUND_PROCESSING: { label: 'Refund Processing', color: 'bg-blue-500/20 text-blue-400', icon: RefreshCw },
  CUSTOMER_REFUND_ISSUED: { label: 'Refund Issued', color: 'bg-green-500/20 text-green-400', icon: DollarSign },
  COMPLETED: { label: 'Completed', color: 'bg-green-500/20 text-green-400', icon: CheckCircle },
  CANCELLED: { label: 'Cancelled', color: 'bg-zinc-500/20 text-zinc-400', icon: X },
  EXCEPTION: { label: 'Exception', color: 'bg-red-500/20 text-red-400', icon: AlertTriangle },
};

function StateBadge({ state }) {
  const config = REFUND_STATES[state] || { label: state, color: 'bg-zinc-500/20 text-zinc-400', icon: Clock };
  const Icon = config.icon;

  return (
    <span className={`inline-flex items-center gap-1.5 px-2.5 py-1 text-xs rounded-full ${config.color}`}>
      <Icon className="w-3 h-3" />
      {config.label}
    </span>
  );
}

function RefundDetailModal({ refund, onClose, onUpdate }) {
  const [action, setAction] = useState('');
  const [denialReason, setDenialReason] = useState('');
  const [creditAmount, setCreditAmount] = useState(refund.refund_amount?.toString() || '');
  const [creditReference, setCreditReference] = useState('');
  const [returnCarrier, setReturnCarrier] = useState('');
  const [returnTracking, setReturnTracking] = useState('');
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);

  const canProcess = refund.state === 'VENDOR_CREDIT_RECEIVED';
  const canReview = ['REQUESTED', 'UNDER_REVIEW'].includes(refund.state);
  const canInitiateReturn = refund.state === 'APPROVED';
  const canRecordCredit = ['VENDOR_RETURN_INITIATED', 'VENDOR_RETURN_IN_TRANSIT', 'VENDOR_RETURN_RECEIVED', 'VENDOR_CREDIT_PENDING'].includes(refund.state);

  const handleAction = async () => {
    setLoading(true);
    setError(null);

    try {
      let endpoint = '';
      let body = {};

      switch (action) {
        case 'approve':
          endpoint = `/api/admin/refunds/${refund.id}/review`;
          body = { action: 'approve' };
          break;
        case 'deny':
          if (!denialReason) {
            setError('Denial reason is required');
            setLoading(false);
            return;
          }
          endpoint = `/api/admin/refunds/${refund.id}/review`;
          body = { action: 'deny', denial_reason: denialReason };
          break;
        case 'initiate_return':
          if (!returnCarrier || !returnTracking) {
            setError('Carrier and tracking number are required');
            setLoading(false);
            return;
          }
          endpoint = `/api/admin/refunds/${refund.id}/vendor-return`;
          body = { return_carrier: returnCarrier, return_tracking_number: returnTracking };
          break;
        case 'record_credit':
          if (!creditAmount || !creditReference) {
            setError('Credit amount and reference are required');
            setLoading(false);
            return;
          }
          endpoint = `/api/admin/refunds/${refund.id}/vendor-credit`;
          body = { credit_amount: parseFloat(creditAmount), credit_reference: creditReference };
          break;
        case 'process_refund':
          endpoint = `/api/admin/refunds/${refund.id}/process-refund`;
          break;
        default:
          setError('Unknown action');
          setLoading(false);
          return;
      }

      const method = action === 'record_credit' ? 'PUT' : 'POST';
      const response = await fetch(`${API_BASE}${endpoint}`, {
        method,
        headers: { 'Content-Type': 'application/json' },
        credentials: 'include',
        body: JSON.stringify(body),
      });

      if (!response.ok) {
        const data = await response.json();
        throw new Error(data.detail || 'Action failed');
      }

      onUpdate();
    } catch (err) {
      setError(err.message);
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center">
      <div className="absolute inset-0 bg-black/60" onClick={onClose} />
      <div className="relative bg-zinc-900 border border-zinc-800 rounded-xl p-6 w-full max-w-2xl max-h-[90vh] overflow-auto">
        <div className="flex items-center justify-between mb-6">
          <div>
            <h3 className="text-lg font-semibold text-white">Refund Request</h3>
            <p className="text-sm text-zinc-400 font-mono">{refund.refund_number}</p>
          </div>
          <button onClick={onClose} className="p-1 hover:bg-zinc-800 rounded">
            <X className="w-5 h-5 text-zinc-400" />
          </button>
        </div>

        {/* Status */}
        <div className="flex items-center gap-4 mb-6">
          <StateBadge state={refund.state} />
          {refund.vendor_credit_received_at && (
            <span className="text-xs text-green-400">Vendor credit: ${refund.vendor_credit_amount}</span>
          )}
        </div>

        {/* Info Grid */}
        <div className="grid grid-cols-2 gap-4 mb-6">
          <div className="p-3 bg-zinc-800/50 rounded-lg">
            <p className="text-xs text-zinc-400 mb-1">Order ID</p>
            <p className="text-sm text-white font-mono">#{refund.order_id}</p>
          </div>
          <div className="p-3 bg-zinc-800/50 rounded-lg">
            <p className="text-xs text-zinc-400 mb-1">Reason</p>
            <p className="text-sm text-white capitalize">{refund.reason_code?.replace('_', ' ')}</p>
          </div>
          <div className="p-3 bg-zinc-800/50 rounded-lg">
            <p className="text-xs text-zinc-400 mb-1">Original Amount</p>
            <p className="text-sm text-white">${refund.original_amount?.toFixed(2)}</p>
          </div>
          <div className="p-3 bg-zinc-800/50 rounded-lg">
            <p className="text-xs text-zinc-400 mb-1">Refund Amount</p>
            <p className="text-sm text-orange-400 font-semibold">${refund.refund_amount?.toFixed(2)}</p>
          </div>
        </div>

        {/* Items */}
        <div className="mb-6">
          <h4 className="text-sm font-semibold text-zinc-400 mb-2">Items</h4>
          <div className="space-y-2">
            {refund.refund_items?.map((item, idx) => (
              <div key={idx} className="flex items-center justify-between p-2 bg-zinc-800/50 rounded">
                <span className="text-sm text-white">{item.product_name}</span>
                <span className="text-sm text-zinc-400">x{item.quantity} @ ${item.unit_price?.toFixed(2)}</span>
              </div>
            ))}
          </div>
        </div>

        {/* Action Panel */}
        {error && (
          <div className="mb-4 p-3 bg-red-500/20 border border-red-500/30 rounded-lg text-red-400 text-sm">
            {error}
          </div>
        )}

        {/* Review Actions */}
        {canReview && (
          <div className="p-4 bg-zinc-800/50 rounded-lg mb-4">
            <h4 className="text-sm font-semibold text-white mb-3">Review Request</h4>
            <div className="flex gap-3">
              <button
                onClick={() => { setAction('approve'); handleAction(); }}
                disabled={loading}
                className="flex-1 flex items-center justify-center gap-2 px-4 py-2 bg-green-600 hover:bg-green-700 text-white rounded-lg transition-colors disabled:opacity-50"
              >
                <CheckCircle className="w-4 h-4" />
                Approve
              </button>
              <button
                onClick={() => setAction('deny')}
                className="flex-1 flex items-center justify-center gap-2 px-4 py-2 bg-red-600 hover:bg-red-700 text-white rounded-lg transition-colors"
              >
                <Ban className="w-4 h-4" />
                Deny
              </button>
            </div>

            {action === 'deny' && (
              <div className="mt-4">
                <label className="block text-sm text-zinc-400 mb-2">Denial Reason</label>
                <textarea
                  value={denialReason}
                  onChange={(e) => setDenialReason(e.target.value)}
                  className="w-full px-3 py-2 bg-zinc-700 border border-zinc-600 rounded-lg text-white text-sm"
                  rows={3}
                  placeholder="Explain why the refund is denied..."
                />
                <button
                  onClick={handleAction}
                  disabled={loading || !denialReason}
                  className="mt-2 px-4 py-2 bg-red-600 hover:bg-red-700 text-white rounded-lg transition-colors disabled:opacity-50"
                >
                  {loading ? 'Processing...' : 'Submit Denial'}
                </button>
              </div>
            )}
          </div>
        )}

        {/* Initiate Return */}
        {canInitiateReturn && (
          <div className="p-4 bg-zinc-800/50 rounded-lg mb-4">
            <h4 className="text-sm font-semibold text-white mb-3">Initiate Vendor Return</h4>
            <div className="grid grid-cols-2 gap-3 mb-3">
              <input
                type="text"
                value={returnCarrier}
                onChange={(e) => setReturnCarrier(e.target.value)}
                placeholder="Carrier (UPS, FedEx, etc.)"
                className="px-3 py-2 bg-zinc-700 border border-zinc-600 rounded-lg text-white text-sm"
              />
              <input
                type="text"
                value={returnTracking}
                onChange={(e) => setReturnTracking(e.target.value)}
                placeholder="Tracking Number"
                className="px-3 py-2 bg-zinc-700 border border-zinc-600 rounded-lg text-white text-sm"
              />
            </div>
            <button
              onClick={() => { setAction('initiate_return'); handleAction(); }}
              disabled={loading || !returnCarrier || !returnTracking}
              className="px-4 py-2 bg-orange-600 hover:bg-orange-700 text-white rounded-lg transition-colors disabled:opacity-50"
            >
              {loading ? 'Processing...' : 'Initiate Return'}
            </button>
          </div>
        )}

        {/* Record Vendor Credit */}
        {canRecordCredit && (
          <div className="p-4 bg-zinc-800/50 rounded-lg mb-4">
            <h4 className="text-sm font-semibold text-white mb-3">Record Vendor Credit</h4>
            <div className="grid grid-cols-2 gap-3 mb-3">
              <input
                type="number"
                value={creditAmount}
                onChange={(e) => setCreditAmount(e.target.value)}
                placeholder="Credit Amount"
                step="0.01"
                className="px-3 py-2 bg-zinc-700 border border-zinc-600 rounded-lg text-white text-sm"
              />
              <input
                type="text"
                value={creditReference}
                onChange={(e) => setCreditReference(e.target.value)}
                placeholder="BCW Credit Reference #"
                className="px-3 py-2 bg-zinc-700 border border-zinc-600 rounded-lg text-white text-sm"
              />
            </div>
            <button
              onClick={() => { setAction('record_credit'); handleAction(); }}
              disabled={loading || !creditAmount || !creditReference}
              className="px-4 py-2 bg-purple-600 hover:bg-purple-700 text-white rounded-lg transition-colors disabled:opacity-50"
            >
              {loading ? 'Processing...' : 'Record Credit'}
            </button>
          </div>
        )}

        {/* Process Customer Refund */}
        {canProcess && (
          <div className="p-4 bg-emerald-500/10 border border-emerald-500/30 rounded-lg mb-4">
            <h4 className="text-sm font-semibold text-emerald-400 mb-2">Ready to Process Customer Refund</h4>
            <p className="text-xs text-zinc-400 mb-3">
              Vendor credit of ${refund.vendor_credit_amount} received. You can now issue the customer refund.
            </p>
            <button
              onClick={() => { setAction('process_refund'); handleAction(); }}
              disabled={loading}
              className="px-4 py-2 bg-emerald-600 hover:bg-emerald-700 text-white rounded-lg transition-colors disabled:opacity-50"
            >
              {loading ? 'Processing...' : `Issue Refund ($${refund.refund_amount?.toFixed(2)})`}
            </button>
          </div>
        )}

        {/* Audit Trail */}
        {refund.events && refund.events.length > 0 && (
          <div className="mt-6">
            <h4 className="text-sm font-semibold text-zinc-400 mb-2">Audit Trail</h4>
            <div className="space-y-2 max-h-48 overflow-auto">
              {refund.events.map((event, idx) => (
                <div key={idx} className="flex items-start gap-3 p-2 bg-zinc-800/30 rounded text-xs">
                  <span className="text-zinc-500 whitespace-nowrap">
                    {new Date(event.created_at).toLocaleString()}
                  </span>
                  <span className="text-zinc-400">
                    {event.from_state && `${event.from_state} → `}{event.to_state}
                  </span>
                  <span className="text-zinc-500">{event.trigger}</span>
                </div>
              ))}
            </div>
          </div>
        )}
      </div>
    </div>
  );
}

export default function RefundList() {
  const [refunds, setRefunds] = useState([]);
  const [stats, setStats] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [stateFilter, setStateFilter] = useState('');
  const [selectedRefund, setSelectedRefund] = useState(null);
  const [page, setPage] = useState(1);
  const pageSize = 20;

  const fetchRefunds = useCallback(async () => {
    try {
      let url = `${API_BASE}/api/admin/refunds?limit=${pageSize}&offset=${(page - 1) * pageSize}`;
      if (stateFilter) url += `&state=${stateFilter}`;

      const response = await fetch(url, { credentials: 'include' });
      if (!response.ok) throw new Error('Failed to fetch refunds');

      const data = await response.json();
      setRefunds(data.refunds || []);
    } catch (err) {
      setError(err.message);
    }
  }, [page, stateFilter]);

  const fetchStats = async () => {
    try {
      const response = await fetch(`${API_BASE}/api/admin/refunds/stats`, { credentials: 'include' });
      if (response.ok) {
        setStats(await response.json());
      }
    } catch (err) {
      console.error('Failed to fetch stats:', err);
    }
  };

  useEffect(() => {
    setLoading(true);
    Promise.all([fetchRefunds(), fetchStats()])
      .finally(() => setLoading(false));
  }, [fetchRefunds]);

  const handleRefundUpdate = () => {
    setSelectedRefund(null);
    fetchRefunds();
    fetchStats();
  };

  if (loading) {
    return (
      <div className="flex items-center justify-center h-64">
        <Loader2 className="w-8 h-8 text-orange-400 animate-spin" />
      </div>
    );
  }

  return (
    <div className="space-y-6">
      {/* Stats Cards */}
      {stats && (
        <div className="grid grid-cols-2 md:grid-cols-4 lg:grid-cols-6 gap-4">
          <div className="p-4 bg-zinc-900 border border-zinc-800 rounded-xl">
            <p className="text-xs text-zinc-400">Total Requests</p>
            <p className="text-2xl font-bold text-white">{stats.total_requests}</p>
          </div>
          <div className="p-4 bg-zinc-900 border border-zinc-800 rounded-xl">
            <p className="text-xs text-zinc-400">Pending Review</p>
            <p className="text-2xl font-bold text-yellow-400">{stats.pending_review}</p>
          </div>
          <div className="p-4 bg-zinc-900 border border-zinc-800 rounded-xl">
            <p className="text-xs text-zinc-400">Awaiting Credit</p>
            <p className="text-2xl font-bold text-purple-400">{stats.pending_vendor_credit}</p>
          </div>
          <div className="p-4 bg-zinc-900 border border-zinc-800 rounded-xl">
            <p className="text-xs text-zinc-400">Ready to Refund</p>
            <p className="text-2xl font-bold text-emerald-400">{stats.ready_for_refund}</p>
          </div>
          <div className="p-4 bg-zinc-900 border border-zinc-800 rounded-xl">
            <p className="text-xs text-zinc-400">Completed</p>
            <p className="text-2xl font-bold text-green-400">{stats.completed}</p>
          </div>
          <div className="p-4 bg-zinc-900 border border-zinc-800 rounded-xl">
            <p className="text-xs text-zinc-400">Total Refunded</p>
            <p className="text-2xl font-bold text-orange-400">${stats.total_refunded_amount?.toFixed(2)}</p>
          </div>
        </div>
      )}

      {/* Filters */}
      <div className="flex items-center gap-4">
        <select
          value={stateFilter}
          onChange={(e) => { setStateFilter(e.target.value); setPage(1); }}
          className="px-4 py-2 bg-zinc-800 border border-zinc-700 rounded-lg text-white text-sm"
        >
          <option value="">All States</option>
          {Object.entries(REFUND_STATES).map(([key, val]) => (
            <option key={key} value={key}>{val.label}</option>
          ))}
        </select>
        <button
          onClick={() => { fetchRefunds(); fetchStats(); }}
          className="p-2 hover:bg-zinc-800 rounded-lg transition-colors"
        >
          <RefreshCw className="w-5 h-5 text-zinc-400" />
        </button>
      </div>

      {/* Table */}
      <div className="bg-zinc-900 border border-zinc-800 rounded-xl overflow-hidden">
        <table className="w-full">
          <thead className="bg-zinc-800/50">
            <tr>
              <th className="px-4 py-3 text-left text-xs font-medium text-zinc-400 uppercase">Refund #</th>
              <th className="px-4 py-3 text-left text-xs font-medium text-zinc-400 uppercase">Order</th>
              <th className="px-4 py-3 text-left text-xs font-medium text-zinc-400 uppercase">State</th>
              <th className="px-4 py-3 text-left text-xs font-medium text-zinc-400 uppercase">Reason</th>
              <th className="px-4 py-3 text-right text-xs font-medium text-zinc-400 uppercase">Amount</th>
              <th className="px-4 py-3 text-left text-xs font-medium text-zinc-400 uppercase">Date</th>
              <th className="px-4 py-3 text-center text-xs font-medium text-zinc-400 uppercase">Action</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-zinc-800">
            {refunds.length === 0 ? (
              <tr>
                <td colSpan={7} className="px-4 py-8 text-center text-zinc-500">
                  No refund requests found
                </td>
              </tr>
            ) : (
              refunds.map((refund) => (
                <tr key={refund.id} className="hover:bg-zinc-800/30 transition-colors">
                  <td className="px-4 py-3 text-sm font-mono text-white">{refund.refund_number}</td>
                  <td className="px-4 py-3 text-sm text-zinc-400">#{refund.order_id}</td>
                  <td className="px-4 py-3"><StateBadge state={refund.state} /></td>
                  <td className="px-4 py-3 text-sm text-zinc-400 capitalize">{refund.reason_code?.replace('_', ' ')}</td>
                  <td className="px-4 py-3 text-sm text-right text-orange-400 font-semibold">
                    ${refund.refund_amount?.toFixed(2)}
                  </td>
                  <td className="px-4 py-3 text-sm text-zinc-500">
                    {new Date(refund.created_at).toLocaleDateString()}
                  </td>
                  <td className="px-4 py-3 text-center">
                    <button
                      onClick={() => setSelectedRefund(refund)}
                      className="p-1.5 hover:bg-zinc-700 rounded transition-colors"
                    >
                      <Eye className="w-4 h-4 text-zinc-400" />
                    </button>
                  </td>
                </tr>
              ))
            )}
          </tbody>
        </table>
      </div>

      {/* Pagination */}
      <div className="flex items-center justify-between">
        <p className="text-sm text-zinc-500">
          Showing {refunds.length} refund(s)
        </p>
        <div className="flex items-center gap-2">
          <button
            onClick={() => setPage(p => Math.max(1, p - 1))}
            disabled={page === 1}
            className="p-2 hover:bg-zinc-800 rounded-lg transition-colors disabled:opacity-50"
          >
            <ChevronLeft className="w-5 h-5 text-zinc-400" />
          </button>
          <span className="text-sm text-zinc-400">Page {page}</span>
          <button
            onClick={() => setPage(p => p + 1)}
            disabled={refunds.length < pageSize}
            className="p-2 hover:bg-zinc-800 rounded-lg transition-colors disabled:opacity-50"
          >
            <ChevronRight className="w-5 h-5 text-zinc-400" />
          </button>
        </div>
      </div>

      {/* Detail Modal */}
      {selectedRefund && (
        <RefundDetailModal
          refund={selectedRefund}
          onClose={() => setSelectedRefund(null)}
          onUpdate={handleRefundUpdate}
        />
      )}
    </div>
  );
}
