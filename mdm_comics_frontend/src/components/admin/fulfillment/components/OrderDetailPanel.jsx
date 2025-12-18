/**
 * OrderDetailPanel - Side panel for order details and actions
 *
 * Per constitution_ui.json:
 * - WCAG 2.2 AA compliant
 * - Keyboard navigation (Escape to close)
 * - Focus trap within panel
 * - ARIA labels and live regions
 */

import React, { useState, useEffect, useRef, useCallback } from 'react';
import { X, Truck, Mail, Printer, Package, MapPin, CreditCard, Clock, User, AlertCircle } from 'lucide-react';
import StatusBadge from '../shared/StatusBadge';

export default function OrderDetailPanel({ orderId, onClose, onUpdate, announce }) {
  const [order, setOrder] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [actionLoading, setActionLoading] = useState(null);
  const panelRef = useRef(null);
  const closeButtonRef = useRef(null);

  const API_BASE = import.meta.env.VITE_API_URL || 'http://localhost:8000/api';

  const fetchOrder = useCallback(async () => {
    setLoading(true);
    setError(null);

    try {
      const response = await fetch(`${API_BASE}/admin/orders/${orderId}`, {
        credentials: 'include',
      });

      if (!response.ok) throw new Error('Failed to fetch order details');

      const data = await response.json();
      setOrder(data);
      announce?.(`Order ${data.order_number || orderId} details loaded`);
    } catch (err) {
      setError(err.message);
      announce?.('Error loading order details');
    } finally {
      setLoading(false);
    }
  }, [orderId, API_BASE, announce]);

  useEffect(() => {
    fetchOrder();
  }, [fetchOrder]);

  useEffect(() => {
    closeButtonRef.current?.focus();

    const handleKeyDown = (e) => {
      if (e.key === 'Escape') {
        onClose();
      }
    };

    document.addEventListener('keydown', handleKeyDown);
    return () => document.removeEventListener('keydown', handleKeyDown);
  }, [onClose]);

  // Focus trap
  useEffect(() => {
    const panel = panelRef.current;
    if (!panel) return;

    const focusableElements = panel.querySelectorAll(
      'button, [href], input, select, textarea, [tabindex]:not([tabindex="-1"])'
    );
    const firstElement = focusableElements[0];
    const lastElement = focusableElements[focusableElements.length - 1];

    const handleTabKey = (e) => {
      if (e.key !== 'Tab') return;

      if (e.shiftKey && document.activeElement === firstElement) {
        e.preventDefault();
        lastElement?.focus();
      } else if (!e.shiftKey && document.activeElement === lastElement) {
        e.preventDefault();
        firstElement?.focus();
      }
    };

    panel.addEventListener('keydown', handleTabKey);
    return () => panel.removeEventListener('keydown', handleTabKey);
  }, [loading]);

  const handleCreateShipment = async () => {
    setActionLoading('ship');
    try {
      const response = await fetch(`${API_BASE}/shipping/shipments`, {
        method: 'POST',
        credentials: 'include',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ order_id: orderId }),
      });

      if (!response.ok) throw new Error('Failed to create shipment');

      announce?.('Shipment created successfully');
      onUpdate?.();
      fetchOrder();
    } catch (err) {
      announce?.('Failed to create shipment');
    } finally {
      setActionLoading(null);
    }
  };

  const handleSendEmail = async (type) => {
    setActionLoading(`email-${type}`);
    try {
      const response = await fetch(`${API_BASE}/admin/orders/${orderId}/email`, {
        method: 'POST',
        credentials: 'include',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ template: type }),
      });

      if (!response.ok) throw new Error('Failed to send email');

      announce?.(`${type} email sent`);
    } catch (err) {
      announce?.('Failed to send email');
    } finally {
      setActionLoading(null);
    }
  };

  const handlePrintPackingSlip = async () => {
    setActionLoading('print');
    try {
      const response = await fetch(`${API_BASE}/admin/orders/${orderId}/packing-slip`, {
        credentials: 'include',
      });

      if (!response.ok) throw new Error('Failed to generate packing slip');

      const blob = await response.blob();
      const url = URL.createObjectURL(blob);
      window.open(url, '_blank');
      URL.revokeObjectURL(url);

      announce?.('Packing slip opened');
    } catch (err) {
      announce?.('Failed to print packing slip');
    } finally {
      setActionLoading(null);
    }
  };

  const formatDate = (dateStr) => {
    return new Date(dateStr).toLocaleDateString('en-US', {
      month: 'short',
      day: 'numeric',
      year: 'numeric',
      hour: 'numeric',
      minute: '2-digit',
    });
  };

  const formatOrderNumber = (order) => {
    return order?.order_number || `MDM-${String(order?.id || orderId).padStart(6, '0')}`;
  };

  return (
    <>
      {/* Backdrop */}
      <div
        className="fixed inset-0 bg-black/60 z-40"
        onClick={onClose}
        aria-hidden="true"
      />

      {/* Panel */}
      <div
        ref={panelRef}
        role="dialog"
        aria-modal="true"
        aria-labelledby="order-panel-title"
        className="fixed right-0 top-0 h-full w-full max-w-xl bg-zinc-900 border-l border-zinc-800 z-50 overflow-y-auto"
      >
        {/* Header */}
        <div className="sticky top-0 bg-zinc-900 border-b border-zinc-800 px-6 py-4 flex items-center justify-between">
          <h2 id="order-panel-title" className="text-lg font-semibold text-white">
            {loading ? 'Loading...' : `Order ${formatOrderNumber(order)}`}
          </h2>
          <button
            ref={closeButtonRef}
            onClick={onClose}
            className="p-2 rounded-lg hover:bg-zinc-800 text-zinc-400 hover:text-white transition-colors"
            aria-label="Close panel"
          >
            <X className="w-5 h-5" />
          </button>
        </div>

        {/* Content */}
        <div className="p-6">
          {loading && (
            <div role="status" className="py-12 text-center">
              <div className="animate-spin w-8 h-8 border-2 border-orange-500 border-t-transparent rounded-full mx-auto mb-4" />
              <p className="text-zinc-400">Loading order details...</p>
            </div>
          )}

          {error && (
            <div role="alert" className="bg-red-500/10 border border-red-500/30 rounded-lg p-4">
              <div className="flex items-center gap-2 text-red-400">
                <AlertCircle className="w-5 h-5" />
                <p>{error}</p>
              </div>
              <button
                onClick={fetchOrder}
                className="mt-2 text-sm text-red-400 hover:text-red-300 underline"
              >
                Retry
              </button>
            </div>
          )}

          {order && !loading && (
            <div className="space-y-6">
              {/* Status and Actions */}
              <div className="flex items-center justify-between">
                <StatusBadge status={order.status} />
                <div className="flex items-center gap-2">
                  {order.status === 'paid' && (
                    <button
                      onClick={handleCreateShipment}
                      disabled={actionLoading === 'ship'}
                      className="flex items-center gap-2 px-3 py-1.5 rounded-lg bg-orange-500 text-white text-sm font-medium hover:bg-orange-600 disabled:opacity-50 transition-colors"
                    >
                      <Truck className="w-4 h-4" />
                      Create Shipment
                    </button>
                  )}
                </div>
              </div>

              {/* Order Summary */}
              <div className="bg-zinc-800/50 rounded-lg p-4 space-y-3">
                <div className="flex items-center gap-2 text-zinc-400">
                  <Clock className="w-4 h-4" />
                  <span className="text-sm">Placed {formatDate(order.created_at)}</span>
                </div>
                <div className="flex items-center gap-2 text-zinc-400">
                  <CreditCard className="w-4 h-4" />
                  <span className="text-sm">
                    {order.payment_method || 'Card'} ending in {order.card_last_four || '****'}
                  </span>
                </div>
              </div>

              {/* Customer Info */}
              <div>
                <h3 className="text-sm font-medium text-zinc-400 uppercase tracking-wider mb-3">
                  Customer
                </h3>
                <div className="bg-zinc-800/50 rounded-lg p-4">
                  <div className="flex items-center gap-3 mb-3">
                    <div className="w-10 h-10 rounded-full bg-zinc-700 flex items-center justify-center">
                      <User className="w-5 h-5 text-zinc-400" />
                    </div>
                    <div>
                      <p className="text-white font-medium">
                        {order.customer_name || order.shipping_address?.name || 'Customer'}
                      </p>
                      <p className="text-sm text-zinc-400">{order.customer_email}</p>
                    </div>
                  </div>
                  <button
                    onClick={() => handleSendEmail('order_confirmation')}
                    disabled={actionLoading?.startsWith('email')}
                    className="flex items-center gap-2 text-sm text-blue-400 hover:text-blue-300"
                  >
                    <Mail className="w-4 h-4" />
                    Resend Confirmation Email
                  </button>
                </div>
              </div>

              {/* Shipping Address */}
              <div>
                <h3 className="text-sm font-medium text-zinc-400 uppercase tracking-wider mb-3">
                  Shipping Address
                </h3>
                <div className="bg-zinc-800/50 rounded-lg p-4">
                  <div className="flex items-start gap-3">
                    <MapPin className="w-4 h-4 text-zinc-500 mt-0.5" />
                    <div className="text-sm">
                      <p className="text-white">{order.shipping_address?.name}</p>
                      <p className="text-zinc-400">{order.shipping_address?.line1}</p>
                      {order.shipping_address?.line2 && (
                        <p className="text-zinc-400">{order.shipping_address.line2}</p>
                      )}
                      <p className="text-zinc-400">
                        {order.shipping_address?.city}, {order.shipping_address?.state}{' '}
                        {order.shipping_address?.postal_code}
                      </p>
                      <p className="text-zinc-400">{order.shipping_address?.country}</p>
                    </div>
                  </div>
                </div>
              </div>

              {/* Order Items */}
              <div>
                <h3 className="text-sm font-medium text-zinc-400 uppercase tracking-wider mb-3">
                  Items ({order.items?.length || 0})
                </h3>
                <div className="space-y-2">
                  {order.items?.map((item, index) => (
                    <div
                      key={item.id || index}
                      className="bg-zinc-800/50 rounded-lg p-3 flex items-center gap-3"
                    >
                      <div className="w-12 h-12 bg-zinc-700 rounded flex items-center justify-center flex-shrink-0">
                        {item.image_url ? (
                          <img
                            src={item.image_url}
                            alt=""
                            className="w-full h-full object-cover rounded"
                          />
                        ) : (
                          <Package className="w-6 h-6 text-zinc-500" />
                        )}
                      </div>
                      <div className="flex-1 min-w-0">
                        <p className="text-sm text-white truncate">{item.title || item.name}</p>
                        <p className="text-xs text-zinc-500">
                          SKU: {item.sku || 'N/A'} | Qty: {item.quantity}
                        </p>
                      </div>
                      <p className="text-sm font-medium text-white">
                        ${parseFloat(item.price || 0).toFixed(2)}
                      </p>
                    </div>
                  ))}
                </div>
              </div>

              {/* Order Totals */}
              <div className="bg-zinc-800/50 rounded-lg p-4">
                <div className="space-y-2 text-sm">
                  <div className="flex justify-between">
                    <span className="text-zinc-400">Subtotal</span>
                    <span className="text-white">${parseFloat(order.subtotal || 0).toFixed(2)}</span>
                  </div>
                  <div className="flex justify-between">
                    <span className="text-zinc-400">Shipping</span>
                    <span className="text-white">
                      ${parseFloat(order.shipping_cost || 0).toFixed(2)}
                    </span>
                  </div>
                  <div className="flex justify-between">
                    <span className="text-zinc-400">Tax</span>
                    <span className="text-white">${parseFloat(order.tax || 0).toFixed(2)}</span>
                  </div>
                  {order.discount > 0 && (
                    <div className="flex justify-between text-green-400">
                      <span>Discount</span>
                      <span>-${parseFloat(order.discount).toFixed(2)}</span>
                    </div>
                  )}
                  <div className="flex justify-between pt-2 border-t border-zinc-700">
                    <span className="text-white font-medium">Total</span>
                    <span className="text-white font-medium">
                      ${parseFloat(order.total || 0).toFixed(2)}
                    </span>
                  </div>
                </div>
              </div>

              {/* Actions */}
              <div className="flex items-center gap-2 pt-4 border-t border-zinc-800">
                <button
                  onClick={handlePrintPackingSlip}
                  disabled={actionLoading === 'print'}
                  className="flex items-center gap-2 px-3 py-2 rounded-lg border border-zinc-700 text-zinc-400 hover:text-white hover:border-zinc-600 disabled:opacity-50 transition-colors"
                >
                  <Printer className="w-4 h-4" />
                  Print Packing Slip
                </button>
                <button
                  onClick={() => handleSendEmail('shipping_notification')}
                  disabled={actionLoading?.startsWith('email') || order.status !== 'shipped'}
                  className="flex items-center gap-2 px-3 py-2 rounded-lg border border-zinc-700 text-zinc-400 hover:text-white hover:border-zinc-600 disabled:opacity-50 transition-colors"
                >
                  <Mail className="w-4 h-4" />
                  Send Shipping Update
                </button>
              </div>

              {/* Order Notes */}
              {order.notes && (
                <div>
                  <h3 className="text-sm font-medium text-zinc-400 uppercase tracking-wider mb-3">
                    Notes
                  </h3>
                  <div className="bg-zinc-800/50 rounded-lg p-4">
                    <p className="text-sm text-zinc-300 whitespace-pre-wrap">{order.notes}</p>
                  </div>
                </div>
              )}

              {/* Timeline */}
              {order.timeline && order.timeline.length > 0 && (
                <div>
                  <h3 className="text-sm font-medium text-zinc-400 uppercase tracking-wider mb-3">
                    Activity
                  </h3>
                  <div className="space-y-3">
                    {order.timeline.map((event, index) => (
                      <div key={index} className="flex items-start gap-3 text-sm">
                        <div className="w-2 h-2 rounded-full bg-zinc-600 mt-1.5" />
                        <div>
                          <p className="text-white">{event.description}</p>
                          <p className="text-xs text-zinc-500">{formatDate(event.created_at)}</p>
                        </div>
                      </div>
                    ))}
                  </div>
                </div>
              )}
            </div>
          )}
        </div>
      </div>
    </>
  );
}
