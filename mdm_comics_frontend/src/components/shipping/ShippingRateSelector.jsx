import React, { useState, useEffect, useCallback } from 'react';
import { Truck, Clock, Shield, CheckCircle, Loader2, AlertCircle, RefreshCw, Package } from 'lucide-react';
import { shippingAPI } from '../../services/api';

// Carrier-specific colors and icons
const CARRIER_STYLES = {
  UPS: {
    bg: 'bg-amber-900/30',
    border: 'border-amber-700',
    text: 'text-amber-400',
    badge: 'bg-amber-500/20 text-amber-400',
  },
  USPS: {
    bg: 'bg-blue-900/30',
    border: 'border-blue-700',
    text: 'text-blue-400',
    badge: 'bg-blue-500/20 text-blue-400',
  },
  DEFAULT: {
    bg: 'bg-zinc-800',
    border: 'border-zinc-700',
    text: 'text-zinc-400',
    badge: 'bg-zinc-500/20 text-zinc-400',
  },
};

const getCarrierStyle = (carrierCode) => {
  return CARRIER_STYLES[carrierCode] || CARRIER_STYLES.DEFAULT;
};

const formatDate = (dateStr) => {
  if (!dateStr) return null;
  const date = new Date(dateStr);
  return date.toLocaleDateString('en-US', {
    weekday: 'short',
    month: 'short',
    day: 'numeric',
  });
};

const formatTimeRemaining = (expiresAt) => {
  const expires = new Date(expiresAt);
  const now = new Date();
  const diffMs = expires - now;
  const diffMins = Math.floor(diffMs / 60000);

  if (diffMins <= 0) return 'Expired';
  if (diffMins < 60) return `${diffMins}m remaining`;
  return `${Math.floor(diffMins / 60)}h ${diffMins % 60}m remaining`;
};

function RateCard({ rate, selected, onSelect, disabled, showCarrier = true }) {
  const isExpired = new Date(rate.expires_at) < new Date();
  const carrierCode = rate.carrier_code || rate.carrierCode || 'UPS';
  const carrierName = rate.carrier_name || rate.carrierName || carrierCode;
  const carrierStyle = getCarrierStyle(carrierCode);

  // Support both multi-carrier and legacy rate formats
  const totalRate = rate.total_rate ?? rate.rate ?? 0;
  const serviceName = rate.service_name ?? rate.serviceName ?? 'Standard';
  const deliveryDate = rate.estimated_delivery_date ?? rate.delivery_date;
  const transitDays = rate.estimated_transit_days ?? rate.delivery_days;
  const guaranteed = rate.guaranteed_delivery ?? rate.guaranteed ?? false;

  return (
    <button
      type="button"
      onClick={() => !disabled && !isExpired && onSelect(rate)}
      disabled={disabled || isExpired}
      className={`w-full p-4 rounded-xl border-2 transition-all text-left ${
        selected
          ? 'border-orange-500 bg-orange-500/10'
          : isExpired
            ? 'border-zinc-700 bg-zinc-800/50 opacity-50 cursor-not-allowed'
            : `${carrierStyle.border} ${carrierStyle.bg} hover:border-zinc-500`
      }`}
    >
      <div className="flex items-start justify-between gap-4">
        <div className="flex-1">
          <div className="flex items-center gap-2 mb-1 flex-wrap">
            {showCarrier && (
              <span className={`px-2 py-0.5 text-xs rounded-full font-medium ${carrierStyle.badge}`}>
                {carrierName}
              </span>
            )}
            <Truck className={`w-5 h-5 ${selected ? 'text-orange-500' : carrierStyle.text}`} />
            <span className="font-semibold text-white">{serviceName}</span>
            {guaranteed && (
              <span className="px-2 py-0.5 text-xs bg-green-500/20 text-green-400 rounded-full">
                Guaranteed
              </span>
            )}
          </div>

          <div className="flex items-center gap-4 text-sm text-zinc-400">
            {deliveryDate && (
              <span className="flex items-center gap-1">
                <Clock className="w-4 h-4" />
                Arrives {formatDate(deliveryDate)}
              </span>
            )}
            {transitDays && !deliveryDate && (
              <span className="flex items-center gap-1">
                <Clock className="w-4 h-4" />
                {transitDays} business day{transitDays !== 1 ? 's' : ''}
              </span>
            )}
          </div>
        </div>

        <div className="text-right">
          <div className="text-xl font-bold text-white">
            ${totalRate.toFixed(2)}
          </div>
          {isExpired ? (
            <div className="text-xs text-red-400">Expired</div>
          ) : (
            <div className="text-xs text-zinc-500">
              {formatTimeRemaining(rate.expires_at)}
            </div>
          )}
        </div>
      </div>

      {selected && (
        <div className="mt-3 pt-3 border-t border-orange-500/30 flex items-center gap-2 text-sm text-orange-400">
          <CheckCircle className="w-4 h-4" />
          Selected shipping method
        </div>
      )}
    </button>
  );
}

export default function ShippingRateSelector({
  destinationAddressId,
  orderId = null,
  packages = null,
  onRateSelected,
  selectedQuoteId = null,
  useMultiCarrier = true, // Default to multi-carrier for best customer experience
  carrierFilter = null, // Optional: filter to specific carrier
}) {
  const [rates, setRates] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [selectedRate, setSelectedRate] = useState(null);
  const [confirming, setConfirming] = useState(false);
  const [carriersQueried, setCarriersQueried] = useState([]);

  const loadRates = useCallback(async () => {
    setLoading(true);
    setError(null);

    try {
      let response;
      if (useMultiCarrier) {
        // Use multi-carrier endpoint to get rates from all enabled carriers
        response = await shippingAPI.getMultiCarrierRates(destinationAddressId, orderId, packages, carrierFilter);
        setCarriersQueried(response.carriers_queried || []);
        // Multi-carrier rates are already sorted by price
        setRates(response.rates || []);
      } else {
        // Legacy single-carrier (UPS) rates
        response = await shippingAPI.getRates(destinationAddressId, orderId, packages);
        setCarriersQueried(['UPS']);
        setRates(response.rates || []);
      }

      // Restore selection if quote_id provided
      if (selectedQuoteId) {
        const found = response.rates?.find(r => r.quote_id === selectedQuoteId);
        if (found) setSelectedRate(found);
      }
    } catch (err) {
      setError(err.message || 'Failed to load shipping rates');
    } finally {
      setLoading(false);
    }
  }, [destinationAddressId, orderId, packages, selectedQuoteId, useMultiCarrier, carrierFilter]);

  useEffect(() => {
    if (destinationAddressId) {
      loadRates();
    }
  }, [destinationAddressId, loadRates]);

  const handleSelectRate = (rate) => {
    setSelectedRate(rate);
  };

  const handleConfirmRate = async () => {
    if (!selectedRate) return;

    setConfirming(true);
    try {
      await shippingAPI.selectRate(selectedRate.quote_id);
      onRateSelected(selectedRate);
    } catch (err) {
      setError(err.message || 'Failed to select rate');
    } finally {
      setConfirming(false);
    }
  };

  if (!destinationAddressId) {
    return (
      <div className="text-center py-8 text-zinc-500">
        Please select a shipping address to view rates.
      </div>
    );
  }

  if (loading) {
    return (
      <div className="flex flex-col items-center justify-center py-12">
        <Loader2 className="w-8 h-8 animate-spin text-orange-500 mb-4" />
        <p className="text-zinc-400">Loading shipping rates...</p>
      </div>
    );
  }

  if (error) {
    return (
      <div className="text-center py-8">
        <AlertCircle className="w-12 h-12 text-red-500 mx-auto mb-4" />
        <p className="text-red-400 mb-4">{error}</p>
        <button
          onClick={loadRates}
          className="inline-flex items-center gap-2 px-4 py-2 bg-zinc-700 text-white rounded-lg hover:bg-zinc-600"
        >
          <RefreshCw className="w-4 h-4" />
          Try Again
        </button>
      </div>
    );
  }

  if (rates.length === 0) {
    return (
      <div className="text-center py-8">
        <Truck className="w-12 h-12 text-zinc-500 mx-auto mb-4" />
        <p className="text-zinc-400 mb-4">No shipping options available for this address.</p>
        <button
          onClick={loadRates}
          className="inline-flex items-center gap-2 px-4 py-2 bg-zinc-700 text-white rounded-lg hover:bg-zinc-600"
        >
          <RefreshCw className="w-4 h-4" />
          Refresh Rates
        </button>
      </div>
    );
  }

  // Determine if we're showing multiple carriers
  const showCarrierBadges = carriersQueried.length > 1 || useMultiCarrier;

  return (
    <div className="space-y-4">
      <div className="flex items-center justify-between">
        <div>
          <h3 className="text-lg font-semibold text-white flex items-center gap-2">
            <Truck className="w-5 h-5 text-orange-500" />
            Shipping Options
          </h3>
          {carriersQueried.length > 0 && (
            <p className="text-xs text-zinc-500 mt-1">
              Comparing rates from {carriersQueried.join(', ')}
            </p>
          )}
        </div>
        <button
          onClick={loadRates}
          disabled={loading}
          className="text-sm text-zinc-400 hover:text-white flex items-center gap-1"
        >
          <RefreshCw className={`w-4 h-4 ${loading ? 'animate-spin' : ''}`} />
          Refresh
        </button>
      </div>

      <div className="space-y-3">
        {rates.map((rate, index) => (
          <RateCard
            key={rate.quote_id || `rate-${index}`}
            rate={rate}
            selected={selectedRate?.quote_id === rate.quote_id || selectedRate === rate}
            onSelect={handleSelectRate}
            disabled={confirming}
            showCarrier={showCarrierBadges}
          />
        ))}
      </div>

      {selectedRate && (
        <div className="pt-4">
          <button
            onClick={handleConfirmRate}
            disabled={confirming}
            className="w-full py-3 bg-orange-500 text-white rounded-xl font-semibold hover:bg-orange-600 transition-colors disabled:opacity-50 flex items-center justify-center gap-2"
          >
            {confirming ? (
              <>
                <Loader2 className="w-5 h-5 animate-spin" />
                Confirming...
              </>
            ) : (
              <>
                <CheckCircle className="w-5 h-5" />
                Continue with {selectedRate.carrier_name || selectedRate.carrierName || 'UPS'} {selectedRate.service_name || selectedRate.serviceName} - ${(selectedRate.total_rate ?? selectedRate.rate ?? 0).toFixed(2)}
              </>
            )}
          </button>
        </div>
      )}

      <div className="flex items-center gap-2 text-xs text-zinc-500 pt-2">
        <Shield className="w-4 h-4" />
        <span>All shipments include $100 declared value coverage. Signature options available at checkout.</span>
      </div>
    </div>
  );
}
