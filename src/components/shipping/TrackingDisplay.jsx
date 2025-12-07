import React, { useState, useEffect } from 'react';
import {
  Package,
  Truck,
  MapPin,
  CheckCircle,
  Clock,
  AlertTriangle,
  ExternalLink,
  RefreshCw,
  Loader2,
  Copy,
  Check
} from 'lucide-react';
import { shippingAPI } from '../../services/api';

const STATUS_CONFIG = {
  draft: { icon: Package, color: 'text-zinc-400', bg: 'bg-zinc-500/20', label: 'Draft' },
  label_pending: { icon: Clock, color: 'text-yellow-400', bg: 'bg-yellow-500/20', label: 'Label Pending' },
  label_created: { icon: Package, color: 'text-blue-400', bg: 'bg-blue-500/20', label: 'Label Created' },
  picked_up: { icon: Truck, color: 'text-blue-400', bg: 'bg-blue-500/20', label: 'Picked Up' },
  in_transit: { icon: Truck, color: 'text-blue-400', bg: 'bg-blue-500/20', label: 'In Transit' },
  out_for_delivery: { icon: Truck, color: 'text-orange-400', bg: 'bg-orange-500/20', label: 'Out for Delivery' },
  delivered: { icon: CheckCircle, color: 'text-green-400', bg: 'bg-green-500/20', label: 'Delivered' },
  exception: { icon: AlertTriangle, color: 'text-red-400', bg: 'bg-red-500/20', label: 'Exception' },
  cancelled: { icon: AlertTriangle, color: 'text-zinc-400', bg: 'bg-zinc-500/20', label: 'Cancelled' },
};

const formatDate = (dateStr) => {
  if (!dateStr) return null;
  const date = new Date(dateStr);
  return date.toLocaleDateString('en-US', {
    weekday: 'short',
    month: 'short',
    day: 'numeric',
    year: 'numeric',
  });
};

const formatDateTime = (dateStr) => {
  if (!dateStr) return null;
  const date = new Date(dateStr);
  return date.toLocaleString('en-US', {
    month: 'short',
    day: 'numeric',
    hour: 'numeric',
    minute: '2-digit',
    hour12: true,
  });
};

function StatusBadge({ status }) {
  const config = STATUS_CONFIG[status] || STATUS_CONFIG.draft;
  const Icon = config.icon;

  return (
    <span className={`inline-flex items-center gap-1.5 px-3 py-1 rounded-full text-sm font-medium ${config.bg} ${config.color}`}>
      <Icon className="w-4 h-4" />
      {config.label}
    </span>
  );
}

function TrackingEvent({ event, isFirst, isLast }) {
  return (
    <div className="relative flex gap-4">
      {/* Timeline line */}
      <div className="flex flex-col items-center">
        <div className={`w-3 h-3 rounded-full ${isFirst ? 'bg-orange-500' : 'bg-zinc-600'}`} />
        {!isLast && <div className="w-0.5 flex-1 bg-zinc-700" />}
      </div>

      {/* Event content */}
      <div className="pb-6 flex-1">
        <div className="flex items-start justify-between gap-4">
          <div>
            <p className={`font-medium ${isFirst ? 'text-white' : 'text-zinc-300'}`}>
              {event.description}
            </p>
            {event.city && (
              <p className="text-sm text-zinc-500 flex items-center gap-1 mt-0.5">
                <MapPin className="w-3 h-3" />
                {[event.city, event.state_province, event.country_code].filter(Boolean).join(', ')}
              </p>
            )}
          </div>
          <span className="text-xs text-zinc-500 whitespace-nowrap">
            {formatDateTime(event.event_time)}
          </span>
        </div>
      </div>
    </div>
  );
}

export default function TrackingDisplay({
  shipmentId = null,
  trackingNumber = null,
  showRefresh = true,
  compact = false,
}) {
  const [tracking, setTracking] = useState(null);
  const [loading, setLoading] = useState(true);
  const [refreshing, setRefreshing] = useState(false);
  const [error, setError] = useState(null);
  const [copied, setCopied] = useState(false);

  const loadTracking = async (refresh = false) => {
    if (refresh) setRefreshing(true);
    else setLoading(true);
    setError(null);

    try {
      let data;
      if (shipmentId) {
        data = await shippingAPI.getTracking(shipmentId, refresh);
      } else if (trackingNumber) {
        data = await shippingAPI.trackByNumber(trackingNumber);
      } else {
        throw new Error('No shipment ID or tracking number provided');
      }
      setTracking(data);
    } catch (err) {
      setError(err.message || 'Failed to load tracking');
    } finally {
      setLoading(false);
      setRefreshing(false);
    }
  };

  useEffect(() => {
    if (shipmentId || trackingNumber) {
      loadTracking();
    }
  }, [shipmentId, trackingNumber]);

  const copyTrackingNumber = async () => {
    if (!tracking?.tracking_number) return;

    try {
      await navigator.clipboard.writeText(tracking.tracking_number);
      setCopied(true);
      setTimeout(() => setCopied(false), 2000);
    } catch (err) {
      console.error('Failed to copy:', err);
    }
  };

  if (!shipmentId && !trackingNumber) {
    return null;
  }

  if (loading) {
    return (
      <div className="flex items-center justify-center py-8">
        <Loader2 className="w-6 h-6 animate-spin text-orange-500" />
      </div>
    );
  }

  if (error) {
    return (
      <div className="text-center py-6">
        <AlertTriangle className="w-10 h-10 text-red-500 mx-auto mb-3" />
        <p className="text-red-400 text-sm mb-4">{error}</p>
        <button
          onClick={() => loadTracking(true)}
          className="text-sm text-orange-500 hover:text-orange-400"
        >
          Try Again
        </button>
      </div>
    );
  }

  if (!tracking) {
    return (
      <div className="text-center py-6 text-zinc-500">
        No tracking information available
      </div>
    );
  }

  return (
    <div className={`${compact ? '' : 'bg-zinc-800 rounded-xl p-6'}`}>
      {/* Header */}
      <div className="flex items-start justify-between gap-4 mb-6">
        <div>
          <div className="flex items-center gap-3 mb-2">
            <StatusBadge status={tracking.status} />
            {tracking.delivered && tracking.delivery_date && (
              <span className="text-sm text-green-400">
                Delivered {formatDate(tracking.delivery_date)}
              </span>
            )}
          </div>

          {tracking.tracking_number && (
            <div className="flex items-center gap-2">
              <span className="text-sm text-zinc-400">Tracking:</span>
              <code className="text-sm font-mono text-white bg-zinc-700 px-2 py-0.5 rounded">
                {tracking.tracking_number}
              </code>
              <button
                onClick={copyTrackingNumber}
                className="p-1 hover:bg-zinc-700 rounded transition-colors"
                title="Copy tracking number"
              >
                {copied ? (
                  <Check className="w-4 h-4 text-green-400" />
                ) : (
                  <Copy className="w-4 h-4 text-zinc-400" />
                )}
              </button>
            </div>
          )}
        </div>

        <div className="flex items-center gap-2">
          {showRefresh && (
            <button
              onClick={() => loadTracking(true)}
              disabled={refreshing}
              className="p-2 hover:bg-zinc-700 rounded-lg transition-colors disabled:opacity-50"
              title="Refresh tracking"
            >
              <RefreshCw className={`w-5 h-5 text-zinc-400 ${refreshing ? 'animate-spin' : ''}`} />
            </button>
          )}
          {tracking.tracking_number && (
            <a
              href={`https://www.ups.com/track?tracknum=${tracking.tracking_number}`}
              target="_blank"
              rel="noopener noreferrer"
              className="p-2 hover:bg-zinc-700 rounded-lg transition-colors"
              title="Track on UPS.com"
            >
              <ExternalLink className="w-5 h-5 text-zinc-400" />
            </a>
          )}
        </div>
      </div>

      {/* Estimated Delivery */}
      {!tracking.delivered && tracking.estimated_delivery && (
        <div className="flex items-center gap-3 p-4 bg-zinc-700/50 rounded-lg mb-6">
          <Clock className="w-5 h-5 text-orange-500" />
          <div>
            <span className="text-sm text-zinc-400">Estimated Delivery:</span>
            <span className="ml-2 font-medium text-white">
              {formatDate(tracking.estimated_delivery)}
            </span>
          </div>
        </div>
      )}

      {/* Signature if delivered */}
      {tracking.delivered && tracking.signature && (
        <div className="flex items-center gap-3 p-4 bg-green-500/10 border border-green-500/20 rounded-lg mb-6">
          <CheckCircle className="w-5 h-5 text-green-400" />
          <div>
            <span className="text-sm text-green-400">Signed by:</span>
            <span className="ml-2 font-medium text-white">{tracking.signature}</span>
          </div>
        </div>
      )}

      {/* Tracking Events */}
      {tracking.events && tracking.events.length > 0 && (
        <div className="mt-6">
          <h4 className="text-sm font-medium text-zinc-400 mb-4">Tracking History</h4>
          <div className="space-y-0">
            {tracking.events.map((event, index) => (
              <TrackingEvent
                key={index}
                event={event}
                isFirst={index === 0}
                isLast={index === tracking.events.length - 1}
              />
            ))}
          </div>
        </div>
      )}
    </div>
  );
}

// Simple public tracking lookup component
export function PublicTrackingLookup() {
  const [trackingNumber, setTrackingNumber] = useState('');
  const [submitted, setSubmitted] = useState(false);

  const handleSubmit = (e) => {
    e.preventDefault();
    if (trackingNumber.trim()) {
      setSubmitted(true);
    }
  };

  if (submitted) {
    return (
      <div>
        <button
          onClick={() => {
            setSubmitted(false);
            setTrackingNumber('');
          }}
          className="text-sm text-orange-500 hover:text-orange-400 mb-4"
        >
          Track another package
        </button>
        <TrackingDisplay trackingNumber={trackingNumber} />
      </div>
    );
  }

  return (
    <form onSubmit={handleSubmit} className="max-w-md mx-auto">
      <label className="block text-sm font-medium text-zinc-300 mb-2">
        Enter Tracking Number
      </label>
      <div className="flex gap-3">
        <input
          type="text"
          value={trackingNumber}
          onChange={(e) => setTrackingNumber(e.target.value)}
          placeholder="1Z999AA10123456784"
          className="flex-1 px-4 py-3 bg-zinc-800 border border-zinc-700 rounded-lg text-white placeholder-zinc-500 focus:outline-none focus:border-orange-500"
        />
        <button
          type="submit"
          disabled={!trackingNumber.trim()}
          className="px-6 py-3 bg-orange-500 text-white rounded-lg font-medium hover:bg-orange-600 transition-colors disabled:opacity-50"
        >
          Track
        </button>
      </div>
    </form>
  );
}
