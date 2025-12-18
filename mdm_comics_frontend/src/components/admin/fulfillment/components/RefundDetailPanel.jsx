/**
 * RefundDetailPanel - Side panel for refund workflow actions
 *
 * Per constitution_ui.json:
 * - WCAG 2.2 AA compliant
 * - Keyboard navigation (Escape to close)
 * - Focus trap within panel
 * - ARIA labels and live regions
 *
 * Refund State Machine:
 * REQUESTED -> UNDER_REVIEW -> APPROVED/DENIED
 * APPROVED -> VENDOR_RETURN_INITIATED -> VENDOR_CREDIT_PENDING -> VENDOR_CREDIT_RECEIVED
 * VENDOR_CREDIT_RECEIVED -> CUSTOMER_REFUND_PROCESSING -> COMPLETED
 */

import { API_BASE } from '../../../../config/api.config.js';
import React, { useState, useEffect, useRef, useCallback } from 'react';
import {
  X,
  CheckCircle,
  XCircle,
  Clock,
  DollarSign,
  Package,
  Truck,
  AlertCircle,
  ArrowRight,
  MessageSquare,
} from 'lucide-react';
import StatusBadge from '../shared/StatusBadge';

const STATE_TRANSITIONS = {
  REQUESTED: ['UNDER_REVIEW', 'DENIED'],
  UNDER_REVIEW: ['APPROVED', 'DENIED'],
  APPROVED: ['VENDOR_RETURN_INITIATED'],
  VENDOR_RETURN_INITIATED: ['VENDOR_CREDIT_PENDING'],
  VENDOR_CREDIT_PENDING: ['VENDOR_CREDIT_RECEIVED'],
  VENDOR_CREDIT_RECEIVED: ['CUSTOMER_REFUND_PROCESSING'],
  CUSTOMER_REFUND_PROCESSING: ['COMPLETED'],
  DENIED: [],
  COMPLETED: [],
};

const STATE_LABELS = {
  UNDER_REVIEW: 'Start Review',
  APPROVED: 'Approve Refund',
  DENIED: 'Deny Refund',
  VENDOR_RETURN_INITIATED: 'Initiate Return',
  VENDOR_CREDIT_PENDING: 'Mark Credit Pending',
  VENDOR_CREDIT_RECEIVED: 'Credit Received',
  CUSTOMER_REFUND_PROCESSING: 'Process Refund',
  COMPLETED: 'Complete',
};

const STATE_ICONS = {
  UNDER_REVIEW: Clock,
  APPROVED: CheckCircle,
  DENIED: XCircle,
  VENDOR_RETURN_INITIATED: Truck,
  VENDOR_CREDIT_PENDING: Clock,
  VENDOR_CREDIT_RECEIVED: DollarSign,
  CUSTOMER_REFUND_PROCESSING: DollarSign,
  COMPLETED: CheckCircle,
};

export default function RefundDetailPanel({ refund, onClose, onUpdate, announce }) {
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);
  const [note, setNote] = useState('');
  const [showNoteInput, setShowNoteInput] = useState(false);
  const [pendingAction, setPendingAction] = useState(null);
  const panelRef = useRef(null);
  const closeButtonRef = useRef(null);

  

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
  }, []);

  const handleTransition = async (newState) => {
    if (newState === 'DENIED' && !note.trim()) {
      setShowNoteInput(true);
      setPendingAction(newState);
      return;
    }

    setLoading(true);
    setError(null);

    try {
      const response = await fetch(`${API_BASE}/admin/refunds/${refund.id}/transition`, {
        method: 'POST',
        credentials: 'include',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          new_state: newState,
          note: note.trim() || undefined,
        }),
      });

      if (!response.ok) {
        const data = await response.json();
        throw new Error(data.detail || 'Failed to update refund');
      }

      announce?.(`Refund updated to ${STATE_LABELS[newState] || newState}`);
      setNote('');
      setShowNoteInput(false);
      setPendingAction(null);
      onUpdate?.();
      onClose();
    } catch (err) {
      setError(err.message);
      announce?.('Failed to update refund');
    } finally {
      setLoading(false);
    }
  };

  const handleProcessStripeRefund = async () => {
    setLoading(true);
    setError(null);

    try {
      const response = await fetch(`${API_BASE}/admin/refunds/${refund.id}/process-stripe`, {
        method: 'POST',
        credentials: 'include',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          amount: refund.refund_amount || refund.original_amount,
        }),
      });

      if (!response.ok) {
        const data = await response.json();
        throw new Error(data.detail || 'Failed to process Stripe refund');
      }

      announce?.('Stripe refund processed successfully');
      onUpdate?.();
      onClose();
    } catch (err) {
      setError(err.message);
      announce?.('Failed to process Stripe refund');
    } finally {
      setLoading(false);
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

  const getRefundNumber = () => {
    return refund.refund_number || `REF-${String(refund.id).padStart(4, '0')}`;
  };

  const availableTransitions = STATE_TRANSITIONS[refund.state] || [];

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
        aria-labelledby="refund-panel-title"
        className="fixed right-0 top-0 h-full w-full max-w-xl bg-zinc-900 border-l border-zinc-800 z-50 overflow-y-auto"
      >
        {/* Header */}
        <div className="sticky top-0 bg-zinc-900 border-b border-zinc-800 px-6 py-4 flex items-center justify-between">
          <h2 id="refund-panel-title" className="text-lg font-semibold text-white">
            Refund {getRefundNumber()}
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
        <div className="p-6 space-y-6">
          {error && (
            <div role="alert" className="bg-red-500/10 border border-red-500/30 rounded-lg p-4">
              <div className="flex items-center gap-2 text-red-400">
                <AlertCircle className="w-5 h-5" />
                <p>{error}</p>
              </div>
            </div>
          )}

          {/* Current Status */}
          <div className="flex items-center justify-between">
            <StatusBadge status={refund.state} />
            <span className="text-sm text-zinc-500">
              {formatDate(refund.updated_at || refund.created_at)}
            </span>
          </div>

          {/* Refund Summary */}
          <div className="bg-zinc-800/50 rounded-lg p-4 space-y-4">
            <div className="grid grid-cols-2 gap-4">
              <div>
                <p className="text-xs text-zinc-500 uppercase">Order</p>
                <p className="text-sm font-mono text-orange-400">#{refund.order_id}</p>
              </div>
              <div>
                <p className="text-xs text-zinc-500 uppercase">Requested</p>
                <p className="text-sm text-zinc-300">{formatDate(refund.created_at)}</p>
              </div>
            </div>
            <div className="grid grid-cols-2 gap-4">
              <div>
                <p className="text-xs text-zinc-500 uppercase">Original Amount</p>
                <p className="text-sm text-white font-medium">
                  ${parseFloat(refund.original_amount || 0).toFixed(2)}
                </p>
              </div>
              <div>
                <p className="text-xs text-zinc-500 uppercase">Refund Amount</p>
                <p className="text-lg text-green-400 font-bold">
                  ${parseFloat(refund.refund_amount || refund.original_amount || 0).toFixed(2)}
                </p>
              </div>
            </div>
          </div>

          {/* Reason */}
          <div>
            <h3 className="text-sm font-medium text-zinc-400 uppercase tracking-wider mb-2">
              Reason
            </h3>
            <div className="bg-zinc-800/50 rounded-lg p-4">
              <p className="text-sm text-white capitalize mb-2">
                {(refund.reason_code || 'other').replace(/_/g, ' ')}
              </p>
              {refund.reason_description && (
                <p className="text-sm text-zinc-400">{refund.reason_description}</p>
              )}
            </div>
          </div>

          {/* Items */}
          {refund.items && refund.items.length > 0 && (
            <div>
              <h3 className="text-sm font-medium text-zinc-400 uppercase tracking-wider mb-2">
                Items
              </h3>
              <div className="space-y-2">
                {refund.items.map((item, index) => (
                  <div
                    key={item.id || index}
                    className="bg-zinc-800/50 rounded-lg p-3 flex items-center gap-3"
                  >
                    <div className="w-10 h-10 bg-zinc-700 rounded flex items-center justify-center flex-shrink-0">
                      <Package className="w-5 h-5 text-zinc-500" />
                    </div>
                    <div className="flex-1 min-w-0">
                      <p className="text-sm text-white truncate">{item.title || item.name}</p>
                      <p className="text-xs text-zinc-500">Qty: {item.quantity}</p>
                    </div>
                    <p className="text-sm font-medium text-white">
                      ${parseFloat(item.price || 0).toFixed(2)}
                    </p>
                  </div>
                ))}
              </div>
            </div>
          )}

          {/* Vendor Credit Info */}
          {['VENDOR_CREDIT_PENDING', 'VENDOR_CREDIT_RECEIVED', 'CUSTOMER_REFUND_PROCESSING', 'COMPLETED'].includes(refund.state) && (
            <div>
              <h3 className="text-sm font-medium text-zinc-400 uppercase tracking-wider mb-2">
                Vendor Credit
              </h3>
              <div className="bg-zinc-800/50 rounded-lg p-4">
                <div className="grid grid-cols-2 gap-4">
                  <div>
                    <p className="text-xs text-zinc-500">Credit Amount</p>
                    <p className="text-sm text-white">
                      ${parseFloat(refund.vendor_credit_amount || 0).toFixed(2)}
                    </p>
                  </div>
                  <div>
                    <p className="text-xs text-zinc-500">Status</p>
                    <p className="text-sm text-white">
                      {refund.vendor_credit_received ? 'Received' : 'Pending'}
                    </p>
                  </div>
                </div>
              </div>
            </div>
          )}

          {/* Note Input for Denial */}
          {showNoteInput && (
            <div>
              <h3 className="text-sm font-medium text-zinc-400 uppercase tracking-wider mb-2">
                Denial Reason (Required)
              </h3>
              <textarea
                value={note}
                onChange={(e) => setNote(e.target.value)}
                placeholder="Explain why this refund is being denied..."
                className="w-full px-3 py-2 bg-zinc-800 border border-zinc-700 rounded-lg text-white placeholder-zinc-500 focus:outline-none focus:ring-2 focus:ring-orange-500/50 resize-none"
                rows={3}
                aria-label="Denial reason"
              />
            </div>
          )}

          {/* Actions */}
          {availableTransitions.length > 0 && (
            <div>
              <h3 className="text-sm font-medium text-zinc-400 uppercase tracking-wider mb-3">
                Actions
              </h3>
              <div className="space-y-2">
                {availableTransitions.map((state) => {
                  const Icon = STATE_ICONS[state] || ArrowRight;
                  const isDeny = state === 'DENIED';
                  const isApprove = state === 'APPROVED';
                  const isComplete = state === 'COMPLETED';

                  return (
                    <button
                      key={state}
                      onClick={() => handleTransition(state)}
                      disabled={loading || (pendingAction === 'DENIED' && !note.trim() && isDeny)}
                      className={`w-full flex items-center justify-between px-4 py-3 rounded-lg font-medium transition-colors disabled:opacity-50 ${
                        isDeny
                          ? 'bg-red-500/10 text-red-400 hover:bg-red-500/20 border border-red-500/30'
                          : isApprove || isComplete
                          ? 'bg-green-500/10 text-green-400 hover:bg-green-500/20 border border-green-500/30'
                          : 'bg-zinc-800 text-white hover:bg-zinc-700 border border-zinc-700'
                      }`}
                    >
                      <span className="flex items-center gap-2">
                        <Icon className="w-4 h-4" />
                        {STATE_LABELS[state]}
                      </span>
                      <ArrowRight className="w-4 h-4" />
                    </button>
                  );
                })}
              </div>
            </div>
          )}

          {/* Process Stripe Refund Button */}
          {refund.state === 'CUSTOMER_REFUND_PROCESSING' && (
            <div className="pt-4 border-t border-zinc-800">
              <button
                onClick={handleProcessStripeRefund}
                disabled={loading}
                className="w-full flex items-center justify-center gap-2 px-4 py-3 rounded-lg bg-orange-500 text-white font-medium hover:bg-orange-600 disabled:opacity-50 transition-colors"
              >
                <DollarSign className="w-5 h-5" />
                Process Stripe Refund (${parseFloat(refund.refund_amount || refund.original_amount || 0).toFixed(2)})
              </button>
              <p className="text-xs text-zinc-500 text-center mt-2">
                This will issue a refund to the customer's original payment method
              </p>
            </div>
          )}

          {/* Notes History */}
          {refund.notes && refund.notes.length > 0 && (
            <div>
              <h3 className="text-sm font-medium text-zinc-400 uppercase tracking-wider mb-3">
                Notes
              </h3>
              <div className="space-y-2">
                {refund.notes.map((noteItem, index) => (
                  <div key={index} className="bg-zinc-800/50 rounded-lg p-3">
                    <div className="flex items-start gap-2">
                      <MessageSquare className="w-4 h-4 text-zinc-500 mt-0.5" />
                      <div>
                        <p className="text-sm text-white">{noteItem.content}</p>
                        <p className="text-xs text-zinc-500 mt-1">
                          {noteItem.author} • {formatDate(noteItem.created_at)}
                        </p>
                      </div>
                    </div>
                  </div>
                ))}
              </div>
            </div>
          )}

          {/* State History */}
          {refund.history && refund.history.length > 0 && (
            <div>
              <h3 className="text-sm font-medium text-zinc-400 uppercase tracking-wider mb-3">
                History
              </h3>
              <div className="space-y-3">
                {refund.history.map((event, index) => (
                  <div key={index} className="flex items-start gap-3 text-sm">
                    <div className="w-2 h-2 rounded-full bg-zinc-600 mt-1.5" />
                    <div>
                      <p className="text-white">
                        {event.from_state} <ArrowRight className="w-3 h-3 inline mx-1" /> {event.to_state}
                      </p>
                      {event.note && <p className="text-zinc-400 text-xs mt-1">{event.note}</p>}
                      <p className="text-xs text-zinc-500 mt-1">
                        {event.actor} • {formatDate(event.created_at)}
                      </p>
                    </div>
                  </div>
                ))}
              </div>
            </div>
          )}

          {/* Final States Info */}
          {(refund.state === 'COMPLETED' || refund.state === 'DENIED') && (
            <div className={`rounded-lg p-4 ${
              refund.state === 'COMPLETED'
                ? 'bg-green-500/10 border border-green-500/30'
                : 'bg-red-500/10 border border-red-500/30'
            }`}>
              <div className="flex items-center gap-2">
                {refund.state === 'COMPLETED' ? (
                  <CheckCircle className="w-5 h-5 text-green-400" />
                ) : (
                  <XCircle className="w-5 h-5 text-red-400" />
                )}
                <p className={refund.state === 'COMPLETED' ? 'text-green-400' : 'text-red-400'}>
                  {refund.state === 'COMPLETED'
                    ? 'This refund has been completed and the customer has been refunded.'
                    : 'This refund request has been denied.'}
                </p>
              </div>
            </div>
          )}
        </div>
      </div>
    </>
  );
}
