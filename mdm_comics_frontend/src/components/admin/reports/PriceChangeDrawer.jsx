/**
 * PriceChangeDrawer - Slide-out panel for price mover details
 * Allows quick product creation from fast movers
 */
import React, { useState, useEffect } from 'react';
import {
  X, Loader2, TrendingUp, TrendingDown, DollarSign, Package,
  ExternalLink, ShoppingCart, AlertCircle, Book, Box
} from 'lucide-react';
import { adminAPI } from '../../../services/adminApi';

export default function PriceChangeDrawer({ isOpen, onClose, priceChange, onCreateProduct }) {
  const [details, setDetails] = useState(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);
  const [creating, setCreating] = useState(false);

  useEffect(() => {
    if (isOpen && priceChange?.entity_id && priceChange?.entity_type) {
      fetchDetails();
    } else {
      setDetails(null);
    }
  }, [isOpen, priceChange?.entity_id, priceChange?.entity_type]);

  const fetchDetails = async () => {
    setLoading(true);
    setError(null);
    try {
      const data = await adminAPI.getEntityDetails(priceChange.entity_type, priceChange.entity_id);
      setDetails(data);
    } catch (err) {
      setError(err.message);
    } finally {
      setLoading(false);
    }
  };

  const handleCreateProduct = async () => {
    if (!details) return;

    setCreating(true);
    try {
      // Build product data from entity details
      const productData = {
        name: details.name || priceChange.entity_name,
        category: details.entity_type === 'comic' ? 'comics' : 'funko',
        price: details.price_loose || details.price_cib || details.price_new || 0,
        cost_price: 0, // User should fill this in
        stock_quantity: 1,
        image_url: details.image_url,
        upc: details.upc || null,
        isbn: details.isbn || null,
        pricecharting_id: details.pricecharting_id,
        description: details.entity_type === 'comic'
          ? `${details.series_name || ''} #${details.number || ''} (${details.year || ''})`
          : `${details.category || ''} - ${details.product_type || ''}`,
      };

      if (onCreateProduct) {
        await onCreateProduct(productData);
      }
      onClose();
    } catch (err) {
      setError(err.message);
    } finally {
      setCreating(false);
    }
  };

  if (!isOpen) return null;

  const isGainer = priceChange?.change_pct > 0;
  const TypeIcon = priceChange?.entity_type === 'comic' ? Book : Box;

  return (
    <>
      {/* Backdrop */}
      <div
        className="fixed inset-0 bg-black/50 z-40 transition-opacity"
        onClick={onClose}
      />

      {/* Drawer */}
      <div className="fixed right-0 top-0 h-full w-full max-w-md bg-zinc-900 border-l border-zinc-800 z-50 flex flex-col shadow-2xl">
        {/* Header */}
        <div className="flex items-center justify-between p-4 border-b border-zinc-800">
          <div className="flex items-center gap-3">
            <div className={`p-2 rounded-lg ${isGainer ? 'bg-green-500/20' : 'bg-red-500/20'}`}>
              {isGainer ? (
                <TrendingUp className="w-5 h-5 text-green-400" />
              ) : (
                <TrendingDown className="w-5 h-5 text-red-400" />
              )}
            </div>
            <div>
              <h2 className="text-lg font-semibold text-white">Price Mover</h2>
              <p className={`text-sm ${isGainer ? 'text-green-400' : 'text-red-400'}`}>
                {isGainer ? '+' : ''}{priceChange?.change_pct?.toFixed(1)}% change
              </p>
            </div>
          </div>
          <button
            onClick={onClose}
            className="p-2 text-zinc-400 hover:text-white hover:bg-zinc-800 rounded-lg"
          >
            <X className="w-5 h-5" />
          </button>
        </div>

        {/* Content */}
        <div className="flex-1 overflow-auto p-4">
          {loading ? (
            <div className="flex items-center justify-center h-48">
              <Loader2 className="w-8 h-8 text-orange-500 animate-spin" />
            </div>
          ) : error ? (
            <div className="bg-red-500/10 border border-red-500/20 rounded-xl p-4 text-center">
              <AlertCircle className="w-8 h-8 text-red-400 mx-auto mb-2" />
              <p className="text-red-400 text-sm">{error}</p>
              <button
                onClick={fetchDetails}
                className="mt-2 px-3 py-1 bg-red-500/20 text-red-400 rounded text-sm hover:bg-red-500/30"
              >
                Retry
              </button>
            </div>
          ) : details ? (
            <div className="space-y-4">
              {/* Cover Image */}
              {details.image_url && (
                <div className="flex justify-center">
                  <div className="relative">
                    <img
                      src={details.image_url}
                      alt={details.name}
                      className="w-48 h-auto rounded-lg shadow-lg border border-zinc-700"
                      onError={(e) => { e.target.style.display = 'none'; }}
                    />
                    <div className={`absolute -top-2 -right-2 px-2 py-1 rounded-full text-xs font-bold ${
                      isGainer ? 'bg-green-500 text-white' : 'bg-red-500 text-white'
                    }`}>
                      {isGainer ? '+' : ''}{priceChange?.change_pct?.toFixed(1)}%
                    </div>
                  </div>
                </div>
              )}

              {/* Entity Info */}
              <div className="bg-zinc-800/50 rounded-xl p-4 space-y-3">
                <div className="flex items-start gap-3">
                  <TypeIcon className="w-5 h-5 text-orange-400 mt-0.5" />
                  <div>
                    <p className="text-white font-medium">{priceChange.entity_name}</p>
                    <p className="text-sm text-zinc-500 capitalize">{details.entity_type}</p>
                  </div>
                </div>

                {details.entity_type === 'comic' && (
                  <>
                    {details.series_name && (
                      <div className="text-sm">
                        <span className="text-zinc-500">Series:</span>
                        <span className="text-zinc-300 ml-2">{details.series_name}</span>
                      </div>
                    )}
                    {details.publisher_name && (
                      <div className="text-sm">
                        <span className="text-zinc-500">Publisher:</span>
                        <span className="text-zinc-300 ml-2">{details.publisher_name}</span>
                      </div>
                    )}
                    {details.year && (
                      <div className="text-sm">
                        <span className="text-zinc-500">Year:</span>
                        <span className="text-zinc-300 ml-2">{details.year}</span>
                      </div>
                    )}
                    {details.upc && (
                      <div className="text-sm">
                        <span className="text-zinc-500">UPC:</span>
                        <span className="text-zinc-300 ml-2 font-mono text-xs">{details.upc}</span>
                      </div>
                    )}
                  </>
                )}

                {details.entity_type === 'funko' && (
                  <>
                    {details.category && (
                      <div className="text-sm">
                        <span className="text-zinc-500">Category:</span>
                        <span className="text-zinc-300 ml-2">{details.category}</span>
                      </div>
                    )}
                    {details.box_number && (
                      <div className="text-sm">
                        <span className="text-zinc-500">Box #:</span>
                        <span className="text-zinc-300 ml-2">{details.box_number}</span>
                      </div>
                    )}
                    {details.license && (
                      <div className="text-sm">
                        <span className="text-zinc-500">License:</span>
                        <span className="text-zinc-300 ml-2">{details.license}</span>
                      </div>
                    )}
                  </>
                )}
              </div>

              {/* Price Grid */}
              <div className="bg-zinc-800/50 rounded-xl p-4">
                <h3 className="text-sm font-semibold text-zinc-400 mb-3 flex items-center gap-2">
                  <DollarSign className="w-4 h-4" />
                  Current Market Prices
                </h3>
                <div className="grid grid-cols-2 gap-3">
                  {details.price_loose !== null && (
                    <div className="bg-zinc-900 rounded-lg p-3">
                      <p className="text-xs text-zinc-500 mb-1">Loose</p>
                      <p className="text-lg font-bold text-white">${details.price_loose?.toFixed(2)}</p>
                    </div>
                  )}
                  {details.price_cib !== null && (
                    <div className="bg-zinc-900 rounded-lg p-3">
                      <p className="text-xs text-zinc-500 mb-1">CIB</p>
                      <p className="text-lg font-bold text-white">${details.price_cib?.toFixed(2)}</p>
                    </div>
                  )}
                  {details.price_new !== null && (
                    <div className="bg-zinc-900 rounded-lg p-3">
                      <p className="text-xs text-zinc-500 mb-1">New</p>
                      <p className="text-lg font-bold text-white">${details.price_new?.toFixed(2)}</p>
                    </div>
                  )}
                  {details.price_graded !== null && (
                    <div className="bg-zinc-900 rounded-lg p-3">
                      <p className="text-xs text-zinc-500 mb-1">Graded</p>
                      <p className="text-lg font-bold text-white">${details.price_graded?.toFixed(2)}</p>
                    </div>
                  )}
                </div>
              </div>

              {/* Price Change Info */}
              <div className={`rounded-xl p-4 ${isGainer ? 'bg-green-500/10 border border-green-500/20' : 'bg-red-500/10 border border-red-500/20'}`}>
                <h3 className="text-sm font-semibold text-zinc-400 mb-2">Recent Change</h3>
                <div className="flex items-center justify-between">
                  <div>
                    <p className="text-sm text-zinc-500">{priceChange.field}</p>
                    <div className="flex items-center gap-2 mt-1">
                      <span className="text-zinc-400">${priceChange.old_value?.toFixed(2)}</span>
                      <span className="text-zinc-600">→</span>
                      <span className={isGainer ? 'text-green-400' : 'text-red-400'}>
                        ${priceChange.new_value?.toFixed(2)}
                      </span>
                    </div>
                  </div>
                  <div className={`text-2xl font-bold ${isGainer ? 'text-green-400' : 'text-red-400'}`}>
                    {isGainer ? '+' : ''}{priceChange.change_pct?.toFixed(1)}%
                  </div>
                </div>
              </div>

              {/* External Links */}
              {details.pricecharting_id && (
                <a
                  href={`https://www.pricecharting.com/game/${details.handle || details.pricecharting_id}`}
                  target="_blank"
                  rel="noopener noreferrer"
                  className="flex items-center justify-center gap-2 w-full p-3 bg-zinc-800 rounded-lg text-zinc-400 hover:text-white hover:bg-zinc-700 transition-colors"
                >
                  <ExternalLink className="w-4 h-4" />
                  View on PriceCharting
                </a>
              )}
            </div>
          ) : (
            <div className="flex items-center justify-center h-48 text-zinc-500">
              Select a price change to view details
            </div>
          )}
        </div>

        {/* Footer - Create Product Button */}
        {details && (
          <div className="p-4 border-t border-zinc-800">
            <button
              onClick={handleCreateProduct}
              disabled={creating}
              className="w-full flex items-center justify-center gap-2 px-4 py-3 bg-orange-500 hover:bg-orange-600 disabled:bg-orange-500/50 text-white font-medium rounded-xl transition-colors"
            >
              {creating ? (
                <Loader2 className="w-5 h-5 animate-spin" />
              ) : (
                <>
                  <ShoppingCart className="w-5 h-5" />
                  Add to Inventory
                </>
              )}
            </button>
            <p className="text-xs text-zinc-500 text-center mt-2">
              Creates a product pre-filled with market data
            </p>
          </div>
        )}
      </div>
    </>
  );
}
