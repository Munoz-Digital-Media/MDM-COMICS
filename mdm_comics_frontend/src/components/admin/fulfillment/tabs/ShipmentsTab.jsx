/**
 * ShipmentsTab - Shipments list with tracking
 */

import { API_BASE } from '../../../../config/api.config.js';
import { authFetch } from '../utils/authFetch';
import React, { useState, useEffect, useCallback, useRef } from 'react';
import { Printer, XCircle, ExternalLink, ChevronDown, ChevronRight, Truck } from 'lucide-react';
import StatusBadge from '../shared/StatusBadge';
import FilterBar from '../shared/FilterBar';
import Pagination from '../shared/Pagination';
import EmptyState from '../shared/EmptyState';

const SHIPMENT_STATUSES = [
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

export default function ShipmentsTab({ announce }) {
  const [shipments, setShipments] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [expandedIds, setExpandedIds] = useState(new Set());
  const [filter, setFilter] = useState({
    status: '',
    page: 1,
    pageSize: 20,
  });
  const [total, setTotal] = useState(0);
  const [actionLoading, setActionLoading] = useState(null);

  

  const fetchShipments = useCallback(async () => {
    setLoading(true);
    setError(null);

    try {
      const params = new URLSearchParams({
        page: filter.page,
        page_size: filter.pageSize,
        ...(filter.status && { status: filter.status }),
      });

      const response = await authFetch(`${API_BASE}/shipping/shipments?${params}`, {
        credentials: 'include',
      });

      if (!response.ok) throw new Error('Failed to fetch shipments');

      const data = await response.json();
      setShipments(data.shipments || []);
      setTotal(data.total || 0);
      announce?.(`Loaded ${data.shipments?.length || 0} shipments`);
    } catch (err) {
      setError(err.message);
      announce?.('Error loading shipments');
    } finally {
      setLoading(false);
    }
  }, [filter, API_BASE, announce]);

  useEffect(() => {
    fetchShipments();
  }, [fetchShipments]);

  const handleFilterChange = (name, value) => {
    setFilter(prev => ({ ...prev, [name]: value, page: 1 }));
  };

  const toggleExpand = (id) => {
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

  const handlePrintLabel = async (shipmentId) => {
    setActionLoading(shipmentId);
    try {
      const response = await authFetch(`${API_BASE}/shipping/shipments/${shipmentId}/label`, {
        credentials: 'include',
      });

      if (!response.ok) throw new Error('Failed to get label');

      const data = await response.json();

      if (data.label_format === 'ZPL') {
        const blob = new Blob([data.label_data], { type: 'text/plain' });
        const url = URL.createObjectURL(blob);
        const a = document.createElement('a');
        a.href = url;
        a.download = `label-${shipmentId}.zpl`;
        a.click();
        URL.revokeObjectURL(url);
      } else {
        const byteString = atob(data.label_data);
        const ab = new ArrayBuffer(byteString.length);
        const ia = new Uint8Array(ab);
        for (let i = 0; i < byteString.length; i++) {
          ia[i] = byteString.charCodeAt(i);
        }
        const blob = new Blob([ab], { type: 'image/png' });
        const url = URL.createObjectURL(blob);
        window.open(url, '_blank');
      }

      announce?.('Label downloaded');
    } catch (err) {
      announce?.('Failed to print label');
    } finally {
      setActionLoading(null);
    }
  };

  const handleVoidShipment = async (shipmentId) => {
    if (!window.confirm('Are you sure you want to void this shipment?')) return;

    setActionLoading(shipmentId);
    try {
      const response = await authFetch(`${API_BASE}/shipping/shipments/${shipmentId}/void`, {
        method: 'POST',
        credentials: 'include',
      });

      if (!response.ok) throw new Error('Failed to void shipment');

      announce?.('Shipment voided');
      fetchShipments();
    } catch (err) {
      announce?.('Failed to void shipment');
    } finally {
      setActionLoading(null);
    }
  };

  const formatDate = (dateStr) => {
    return new Date(dateStr).toLocaleDateString('en-US', {
      month: 'short',
      day: 'numeric',
      hour: 'numeric',
      minute: '2-digit',
    });
  };

  const getTrackingUrl = (trackingNumber) => {
    return `https://www.ups.com/track?tracknum=${trackingNumber}`;
  };

  return (
    <div className="shipments-tab">
      <FilterBar
        filters={[
          {
            type: 'select',
            name: 'status',
            label: 'Filter by status',
            options: SHIPMENT_STATUSES,
            value: filter.status,
          },
        ]}
        onFilterChange={handleFilterChange}
      />

      {error && (
        <div role="alert" className="bg-red-500/10 border border-red-500/30 rounded-lg p-4 mb-4">
          <p className="text-red-400">{error}</p>
          <button onClick={fetchShipments} className="mt-2 text-sm text-red-400 hover:text-red-300 underline">
            Retry
          </button>
        </div>
      )}

      {loading && (
        <div role="status" className="py-12 text-center">
          <div className="animate-spin w-8 h-8 border-2 border-orange-500 border-t-transparent rounded-full mx-auto mb-4" />
          <p className="text-zinc-400">Loading shipments...</p>
        </div>
      )}

      {!loading && shipments.length === 0 && (
        <EmptyState
          icon="Truck"
          title="No shipments found"
          description={filter.status ? "Try adjusting your filters" : "Shipments will appear here when orders are fulfilled"}
        />
      )}

      {!loading && shipments.length > 0 && (
        <>
          <div className="space-y-2">
            {shipments.map((shipment) => (
              <div
                key={shipment.id}
                className="border border-zinc-800 rounded-lg overflow-hidden"
              >
                <div
                  className="flex items-center gap-4 px-4 py-3 bg-zinc-900/50 cursor-pointer hover:bg-zinc-800/50 transition-colors"
                  onClick={() => toggleExpand(shipment.id)}
                >
                  <button className="text-zinc-400">
                    {expandedIds.has(shipment.id) ? (
                      <ChevronDown className="w-4 h-4" />
                    ) : (
                      <ChevronRight className="w-4 h-4" />
                    )}
                  </button>

                  <div className="flex-1 grid grid-cols-5 gap-4 items-center">
                    <div>
                      <p className="text-xs text-zinc-500">Order</p>
                      <p className="text-sm font-mono text-orange-400">#{shipment.order_id}</p>
                    </div>
                    <div>
                      <p className="text-xs text-zinc-500">Service</p>
                      <p className="text-sm text-white">{shipment.service_name || 'UPS Ground'}</p>
                    </div>
                    <div>
                      <p className="text-xs text-zinc-500">Tracking</p>
                      {shipment.tracking_number ? (
                        <a
                          href={getTrackingUrl(shipment.tracking_number)}
                          target="_blank"
                          rel="noopener noreferrer"
                          onClick={(e) => e.stopPropagation()}
                          className="text-sm text-blue-400 hover:text-blue-300 font-mono"
                        >
                          {shipment.tracking_number}
                        </a>
                      ) : (
                        <p className="text-sm text-zinc-500">—</p>
                      )}
                    </div>
                    <div>
                      <p className="text-xs text-zinc-500">Created</p>
                      <p className="text-sm text-zinc-400">{formatDate(shipment.created_at)}</p>
                    </div>
                    <div>
                      <StatusBadge status={shipment.status} />
                    </div>
                  </div>

                  <div className="flex items-center gap-1" onClick={(e) => e.stopPropagation()}>
                    {shipment.has_label && (
                      <button
                        onClick={() => handlePrintLabel(shipment.id)}
                        disabled={actionLoading === shipment.id}
                        className="p-1.5 rounded hover:bg-zinc-700 text-zinc-400 hover:text-white transition-colors disabled:opacity-50"
                        title="Print Label"
                      >
                        <Printer className="w-4 h-4" />
                      </button>
                    )}
                    {['draft', 'label_pending', 'label_created'].includes(shipment.status) && (
                      <button
                        onClick={() => handleVoidShipment(shipment.id)}
                        disabled={actionLoading === shipment.id}
                        className="p-1.5 rounded hover:bg-zinc-700 text-zinc-400 hover:text-red-400 transition-colors disabled:opacity-50"
                        title="Void Shipment"
                      >
                        <XCircle className="w-4 h-4" />
                      </button>
                    )}
                  </div>
                </div>

                {expandedIds.has(shipment.id) && (
                  <div className="px-4 py-3 border-t border-zinc-800 bg-zinc-900/30">
                    <div className="grid grid-cols-4 gap-4 text-sm">
                      <div>
                        <p className="text-zinc-500">Weight</p>
                        <p className="text-white">{shipment.weight || '—'} lbs</p>
                      </div>
                      <div>
                        <p className="text-zinc-500">Packages</p>
                        <p className="text-white">{shipment.package_count || 1}</p>
                      </div>
                      <div>
                        <p className="text-zinc-500">Shipping Cost</p>
                        <p className="text-white">${parseFloat(shipment.shipping_cost || 0).toFixed(2)}</p>
                      </div>
                      <div>
                        <p className="text-zinc-500">Signature Required</p>
                        <p className="text-white">{shipment.signature_required ? 'Yes' : 'No'}</p>
                      </div>
                    </div>

                    {shipment.tracking_events && shipment.tracking_events.length > 0 && (
                      <div className="mt-4 pt-4 border-t border-zinc-800">
                        <p className="text-xs text-zinc-500 uppercase mb-2">Tracking History</p>
                        <div className="space-y-2">
                          {shipment.tracking_events.slice(0, 5).map((event, i) => (
                            <div key={i} className="flex items-start gap-3 text-sm">
                              <Truck className="w-4 h-4 text-zinc-500 mt-0.5" />
                              <div>
                                <p className="text-white">{event.description}</p>
                                <p className="text-xs text-zinc-500">
                                  {event.location} • {formatDate(event.time)}
                                </p>
                              </div>
                            </div>
                          ))}
                        </div>
                      </div>
                    )}
                  </div>
                )}
              </div>
            ))}
          </div>

          <Pagination
            page={filter.page}
            pageSize={filter.pageSize}
            total={total}
            onPageChange={(page) => setFilter(prev => ({ ...prev, page }))}
          />
        </>
      )}
    </div>
  );
}
