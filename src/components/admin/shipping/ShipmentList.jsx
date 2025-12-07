/**
 * ShipmentList - Admin shipment management
 * UPS Shipping Integration v1.28.0
 */
import React, { useState, useEffect } from 'react';
import {
  Package, Truck, Search, Filter, ChevronDown, ChevronRight,
  MapPin, Clock, CheckCircle, AlertTriangle, XCircle, RefreshCw,
  Loader2, ExternalLink, Printer, Ban
} from 'lucide-react';
import { shippingAPI } from '../../../services/api';
import { TrackingDisplay } from '../../shipping/TrackingDisplay';

const STATUS_OPTIONS = [
  { value: '', label: 'All Statuses' },
  { value: 'draft', label: 'Draft' },
  { value: 'label_pending', label: 'Label Pending' },
  { value: 'label_created', label: 'Label Created' },
  { value: 'picked_up', label: 'Picked Up' },
  { value: 'in_transit', label: 'In Transit' },
  { value: 'out_for_delivery', label: 'Out for Delivery' },
  { value: 'delivered', label: 'Delivered' },
  { value: 'exception', label: 'Exception' },
  { value: 'cancelled', label: 'Cancelled' },
];

const STATUS_STYLES = {
  draft: 'bg-zinc-500/20 text-zinc-400',
  label_pending: 'bg-yellow-500/20 text-yellow-400',
  label_created: 'bg-blue-500/20 text-blue-400',
  picked_up: 'bg-blue-500/20 text-blue-400',
  in_transit: 'bg-blue-500/20 text-blue-400',
  out_for_delivery: 'bg-orange-500/20 text-orange-400',
  delivered: 'bg-green-500/20 text-green-400',
  exception: 'bg-red-500/20 text-red-400',
  cancelled: 'bg-zinc-500/20 text-zinc-400',
};

function StatusBadge({ status }) {
  const style = STATUS_STYLES[status] || STATUS_STYLES.draft;
  const label = STATUS_OPTIONS.find(s => s.value === status)?.label || status;

  return (
    <span className={`px-2 py-1 rounded-full text-xs font-medium ${style}`}>
      {label}
    </span>
  );
}

function ShipmentRow({ shipment, expanded, onToggle, onVoid, onPrintLabel }) {
  const formatDate = (dateStr) => {
    if (!dateStr) return '-';
    return new Date(dateStr).toLocaleDateString('en-US', {
      month: 'short',
      day: 'numeric',
      year: 'numeric',
    });
  };

  return (
    <>
      <tr className="border-b border-zinc-800 hover:bg-zinc-800/50 transition-colors">
        <td className="px-4 py-3">
          <button
            onClick={onToggle}
            className="p-1 hover:bg-zinc-700 rounded"
          >
            {expanded ? (
              <ChevronDown className="w-4 h-4 text-zinc-400" />
            ) : (
              <ChevronRight className="w-4 h-4 text-zinc-400" />
            )}
          </button>
        </td>
        <td className="px-4 py-3">
          <div className="font-mono text-sm text-white">#{shipment.id}</div>
          <div className="text-xs text-zinc-500">Order #{shipment.order_id}</div>
        </td>
        <td className="px-4 py-3">
          <StatusBadge status={shipment.status} />
        </td>
        <td className="px-4 py-3">
          {shipment.tracking_number ? (
            <a
              href={`https://www.ups.com/track?tracknum=${shipment.tracking_number}`}
              target="_blank"
              rel="noopener noreferrer"
              className="font-mono text-sm text-orange-400 hover:text-orange-300 flex items-center gap-1"
            >
              {shipment.tracking_number}
              <ExternalLink className="w-3 h-3" />
            </a>
          ) : (
            <span className="text-zinc-500">-</span>
          )}
        </td>
        <td className="px-4 py-3">
          <div className="text-sm text-white">{shipment.service_name}</div>
        </td>
        <td className="px-4 py-3 text-right">
          <div className="text-sm text-white">
            {shipment.shipping_cost ? `$${shipment.shipping_cost.toFixed(2)}` : '-'}
          </div>
        </td>
        <td className="px-4 py-3">
          <div className="text-sm text-zinc-400">{formatDate(shipment.created_at)}</div>
        </td>
        <td className="px-4 py-3">
          <div className="flex items-center gap-2">
            {shipment.has_label && (
              <button
                onClick={() => onPrintLabel(shipment.id)}
                className="p-1.5 hover:bg-zinc-700 rounded text-zinc-400 hover:text-white"
                title="Print Label"
              >
                <Printer className="w-4 h-4" />
              </button>
            )}
            {['draft', 'label_pending', 'label_created'].includes(shipment.status) && (
              <button
                onClick={() => onVoid(shipment.id)}
                className="p-1.5 hover:bg-red-500/20 rounded text-zinc-400 hover:text-red-400"
                title="Void Shipment"
              >
                <Ban className="w-4 h-4" />
              </button>
            )}
          </div>
        </td>
      </tr>
      {expanded && (
        <tr className="bg-zinc-800/30">
          <td colSpan="8" className="px-4 py-4">
            <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
              {/* Shipment Details */}
              <div className="space-y-4">
                <h4 className="text-sm font-medium text-zinc-300">Shipment Details</h4>
                <div className="grid grid-cols-2 gap-4 text-sm">
                  <div>
                    <span className="text-zinc-500">Weight:</span>
                    <span className="ml-2 text-white">{shipment.weight} lbs</span>
                  </div>
                  <div>
                    <span className="text-zinc-500">Packages:</span>
                    <span className="ml-2 text-white">{shipment.package_count}</span>
                  </div>
                  <div>
                    <span className="text-zinc-500">Carrier Cost:</span>
                    <span className="ml-2 text-white">
                      {shipment.carrier_cost ? `$${shipment.carrier_cost.toFixed(2)}` : '-'}
                    </span>
                  </div>
                  <div>
                    <span className="text-zinc-500">Signature:</span>
                    <span className="ml-2 text-white">
                      {shipment.signature_required ? 'Required' : 'Not Required'}
                    </span>
                  </div>
                </div>
              </div>

              {/* Tracking */}
              {shipment.tracking_number && (
                <div>
                  <h4 className="text-sm font-medium text-zinc-300 mb-4">Tracking</h4>
                  <TrackingDisplay shipmentId={shipment.id} compact />
                </div>
              )}
            </div>
          </td>
        </tr>
      )}
    </>
  );
}

export default function ShipmentList() {
  const [shipments, setShipments] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [page, setPage] = useState(1);
  const [totalPages, setTotalPages] = useState(1);
  const [statusFilter, setStatusFilter] = useState('');
  const [expandedIds, setExpandedIds] = useState(new Set());
  const [actionLoading, setActionLoading] = useState(null);

  const loadShipments = async () => {
    setLoading(true);
    setError(null);

    try {
      const response = await shippingAPI.getShipments(null, statusFilter || null, page, 20);
      setShipments(response.shipments || []);
      setTotalPages(Math.ceil((response.total || 0) / 20));
    } catch (err) {
      setError(err.message || 'Failed to load shipments');
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    loadShipments();
  }, [page, statusFilter]);

  const handleToggleExpand = (id) => {
    setExpandedIds(prev => {
      const next = new Set(prev);
      if (next.has(id)) {
        next.delete(id);
      } else {
        next.add(id);
      }
      return next;
    });
  };

  const handleVoidShipment = async (shipmentId) => {
    if (!confirm('Are you sure you want to void this shipment?')) return;

    setActionLoading(shipmentId);
    try {
      await shippingAPI.voidShipment(shipmentId);
      loadShipments();
    } catch (err) {
      alert(err.message || 'Failed to void shipment');
    } finally {
      setActionLoading(null);
    }
  };

  const handlePrintLabel = async (shipmentId) => {
    try {
      const label = await shippingAPI.getLabel(shipmentId);
      // Open label in new window
      const labelWindow = window.open('', '_blank');
      if (label.label_format === 'ZPL') {
        labelWindow.document.write(`<pre>${atob(label.label_data)}</pre>`);
      } else {
        labelWindow.document.write(`<img src="data:image/${label.label_format.toLowerCase()};base64,${label.label_data}" />`);
      }
    } catch (err) {
      alert(err.message || 'Failed to load label');
    }
  };

  return (
    <div className="space-y-4">
      {/* Header */}
      <div className="flex items-center justify-between">
        <h2 className="text-xl font-bold text-white flex items-center gap-2">
          <Truck className="w-6 h-6 text-orange-500" />
          Shipments
        </h2>
        <button
          onClick={loadShipments}
          disabled={loading}
          className="flex items-center gap-2 px-4 py-2 bg-zinc-700 text-white rounded-lg hover:bg-zinc-600 disabled:opacity-50"
        >
          <RefreshCw className={`w-4 h-4 ${loading ? 'animate-spin' : ''}`} />
          Refresh
        </button>
      </div>

      {/* Filters */}
      <div className="flex items-center gap-4">
        <div className="relative">
          <select
            value={statusFilter}
            onChange={(e) => {
              setStatusFilter(e.target.value);
              setPage(1);
            }}
            className="appearance-none px-4 py-2 pr-10 bg-zinc-800 border border-zinc-700 rounded-lg text-white focus:outline-none focus:border-orange-500"
          >
            {STATUS_OPTIONS.map(opt => (
              <option key={opt.value} value={opt.value}>{opt.label}</option>
            ))}
          </select>
          <ChevronDown className="absolute right-3 top-1/2 -translate-y-1/2 w-4 h-4 text-zinc-400 pointer-events-none" />
        </div>
      </div>

      {/* Error */}
      {error && (
        <div className="flex items-center gap-2 p-4 bg-red-500/10 border border-red-500/20 rounded-lg text-red-400">
          <AlertTriangle className="w-5 h-5" />
          <span>{error}</span>
        </div>
      )}

      {/* Table */}
      <div className="bg-zinc-900 rounded-xl border border-zinc-800 overflow-hidden">
        <div className="overflow-x-auto">
          <table className="w-full">
            <thead className="bg-zinc-800/50 border-b border-zinc-800">
              <tr>
                <th className="w-10 px-4 py-3"></th>
                <th className="px-4 py-3 text-left text-xs font-medium text-zinc-400 uppercase tracking-wider">
                  ID
                </th>
                <th className="px-4 py-3 text-left text-xs font-medium text-zinc-400 uppercase tracking-wider">
                  Status
                </th>
                <th className="px-4 py-3 text-left text-xs font-medium text-zinc-400 uppercase tracking-wider">
                  Tracking
                </th>
                <th className="px-4 py-3 text-left text-xs font-medium text-zinc-400 uppercase tracking-wider">
                  Service
                </th>
                <th className="px-4 py-3 text-right text-xs font-medium text-zinc-400 uppercase tracking-wider">
                  Cost
                </th>
                <th className="px-4 py-3 text-left text-xs font-medium text-zinc-400 uppercase tracking-wider">
                  Created
                </th>
                <th className="px-4 py-3 text-left text-xs font-medium text-zinc-400 uppercase tracking-wider">
                  Actions
                </th>
              </tr>
            </thead>
            <tbody>
              {loading ? (
                <tr>
                  <td colSpan="8" className="px-4 py-12 text-center">
                    <Loader2 className="w-8 h-8 animate-spin text-orange-500 mx-auto" />
                  </td>
                </tr>
              ) : shipments.length === 0 ? (
                <tr>
                  <td colSpan="8" className="px-4 py-12 text-center text-zinc-500">
                    No shipments found
                  </td>
                </tr>
              ) : (
                shipments.map(shipment => (
                  <ShipmentRow
                    key={shipment.id}
                    shipment={shipment}
                    expanded={expandedIds.has(shipment.id)}
                    onToggle={() => handleToggleExpand(shipment.id)}
                    onVoid={handleVoidShipment}
                    onPrintLabel={handlePrintLabel}
                  />
                ))
              )}
            </tbody>
          </table>
        </div>

        {/* Pagination */}
        {totalPages > 1 && (
          <div className="px-4 py-3 border-t border-zinc-800 flex items-center justify-between">
            <span className="text-sm text-zinc-400">
              Page {page} of {totalPages}
            </span>
            <div className="flex items-center gap-2">
              <button
                onClick={() => setPage(p => Math.max(1, p - 1))}
                disabled={page === 1}
                className="px-3 py-1 bg-zinc-700 text-white rounded hover:bg-zinc-600 disabled:opacity-50 disabled:cursor-not-allowed"
              >
                Previous
              </button>
              <button
                onClick={() => setPage(p => Math.min(totalPages, p + 1))}
                disabled={page === totalPages}
                className="px-3 py-1 bg-zinc-700 text-white rounded hover:bg-zinc-600 disabled:opacity-50 disabled:cursor-not-allowed"
              >
                Next
              </button>
            </div>
          </div>
        )}
      </div>
    </div>
  );
}
