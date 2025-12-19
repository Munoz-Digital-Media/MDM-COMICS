/**
 * ScanQueue - Barcode queue management with batch processing
 * Phase 3: MDM Admin Console Inventory System v1.3.0
 */
import React, { useState, useEffect, useCallback } from 'react';
import {
  QrCode, Check, X, Plus, ChevronLeft, ChevronRight,
  Loader2, AlertTriangle, Package, RefreshCw, Zap
} from 'lucide-react';
import { adminAPI } from '../../../services/adminApi';

function QueueItem({ item, onProcess, onSkip, selected, onSelect }) {
  const getStatusBadge = () => {
    switch (item.status) {
      case 'pending':
        return <span className="px-2 py-0.5 text-xs bg-yellow-500/20 text-yellow-400 rounded-full">Pending</span>;
      case 'matched':
        return <span className="px-2 py-0.5 text-xs bg-green-500/20 text-green-400 rounded-full">Matched</span>;
      case 'processed':
        return <span className="px-2 py-0.5 text-xs bg-blue-500/20 text-blue-400 rounded-full">Processed</span>;
      case 'failed':
        return <span className="px-2 py-0.5 text-xs bg-red-500/20 text-red-400 rounded-full">Failed</span>;
      case 'skipped':
        return <span className="px-2 py-0.5 text-xs bg-zinc-500/20 text-zinc-400 rounded-full">Skipped</span>;
      default:
        return null;
    }
  };

  const canProcess = item.status === 'pending' || item.status === 'matched';

  return (
    <div className={`p-4 bg-zinc-800/50 rounded-lg border ${selected ? 'border-orange-500' : 'border-transparent'}`}>
      <div className="flex items-start gap-3">
        {canProcess && (
          <input
            type="checkbox"
            checked={selected}
            onChange={(e) => onSelect(item.id, e.target.checked)}
            className="mt-1 rounded border-zinc-600"
          />
        )}

        <div className="flex-1 min-w-0">
          <div className="flex items-center gap-2 mb-1">
            <span className="text-sm font-mono text-white">{item.barcode}</span>
            <span className="text-xs text-zinc-500">{item.barcode_type}</span>
            {getStatusBadge()}
          </div>

          {item.matched_product && (
            <div className="flex items-center gap-2 mt-2 p-2 bg-zinc-900 rounded">
              <img
                src={item.matched_product.image || 'https://placehold.co/40x40/27272a/f59e0b?text=?'}
                alt=""
                className="w-10 h-10 rounded object-contain"
              />
              <div>
                <p className="text-sm text-white truncate">{item.matched_product.name}</p>
                <p className="text-xs text-zinc-500">
                  Stock: {item.matched_product.stock} • ${item.matched_product.price}
                </p>
              </div>
            </div>
          )}

          {item.matched_comic && !item.matched_product && (
            <div className="mt-2 p-2 bg-zinc-900 rounded">
              <p className="text-sm text-white">{item.matched_comic.title}</p>
              <p className="text-xs text-zinc-500">From comic database - can create product</p>
            </div>
          )}

          {!item.matched_product && !item.matched_comic && item.status === 'pending' && (
            <p className="text-xs text-zinc-500 mt-1">No match found - manual creation required</p>
          )}

          <p className="text-xs text-zinc-600 mt-2">
            Scanned {new Date(item.scanned_at).toLocaleString()}
          </p>
        </div>

        {canProcess && (
          <div className="flex flex-col gap-1">
            {item.matched_product ? (
              <button
                onClick={() => onProcess(item, 'add_to_existing')}
                className="px-3 py-1.5 text-xs bg-green-500/20 text-green-400 rounded hover:bg-green-500/30 flex items-center gap-1"
              >
                <Plus className="w-3 h-3" />
                +1 Stock
              </button>
            ) : item.matched_comic ? (
              <button
                onClick={() => onProcess(item, 'create_product')}
                className="px-3 py-1.5 text-xs bg-blue-500/20 text-blue-400 rounded hover:bg-blue-500/30 flex items-center gap-1"
              >
                <Package className="w-3 h-3" />
                Create
              </button>
            ) : null}
            <button
              onClick={() => onSkip(item)}
              className="px-3 py-1.5 text-xs bg-zinc-700 text-zinc-400 rounded hover:bg-zinc-600"
            >
              Skip
            </button>
          </div>
        )}
      </div>
    </div>
  );
}

export default function ScanQueue() {
  const [queue, setQueue] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [statusFilter, setStatusFilter] = useState('pending');
  const [offset, setOffset] = useState(0);
  const [total, setTotal] = useState(0);
  const [counts, setCounts] = useState({ pending: 0, matched: 0, processed: 0 });
  const [selectedIds, setSelectedIds] = useState(new Set());
  const [processing, setProcessing] = useState(false);

  const limit = 20;

  const fetchQueue = useCallback(async () => {
    setLoading(true);
    setError(null);

    try {
      const data = await adminAPI.getBarcodeQueue({
        status: statusFilter || undefined,
        limit,
        offset,
      });
      setQueue(data.items || []);
      setTotal(data.total || 0);
      setCounts({
        pending: data.pending || 0,
        matched: data.matched || 0,
        processed: data.processed || 0,
      });
    } catch (err) {
      setError(err.message);
    } finally {
      setLoading(false);
    }
  }, [statusFilter, offset]);

  useEffect(() => {
    fetchQueue();
  }, [fetchQueue]);

  const handleSelect = (id, checked) => {
    setSelectedIds(prev => {
      const next = new Set(prev);
      if (checked) {
        next.add(id);
      } else {
        next.delete(id);
      }
      return next;
    });
  };

  const handleSelectAll = () => {
    const processableIds = queue
      .filter(item => item.status === 'pending' || item.status === 'matched')
      .map(item => item.id);

    if (selectedIds.size === processableIds.length) {
      setSelectedIds(new Set());
    } else {
      setSelectedIds(new Set(processableIds));
    }
  };

  const handleProcess = async (item, action) => {
    setProcessing(true);
    try {
      await adminAPI.processQueueItem(item.id, action);
      fetchQueue();
    } catch (err) {
      alert('Failed to process: ' + err.message);
    } finally {
      setProcessing(false);
    }
  };

  const handleSkip = async (item) => {
    setProcessing(true);
    try {
      await adminAPI.processQueueItem(item.id, 'skip');
      fetchQueue();
    } catch (err) {
      alert('Failed to skip: ' + err.message);
    } finally {
      setProcessing(false);
    }
  };

  const handleBatchProcess = async (action) => {
    if (selectedIds.size === 0) return;

    setProcessing(true);
    try {
      await adminAPI.batchProcessQueue(Array.from(selectedIds), action);
      setSelectedIds(new Set());
      fetchQueue();
    } catch (err) {
      alert('Batch processing failed: ' + err.message);
    } finally {
      setProcessing(false);
    }
  };

  const totalPages = Math.ceil(total / limit);
  const currentPage = Math.floor(offset / limit) + 1;

  return (
    <div className="space-y-4">
      {/* Summary Cards */}
      <div className="grid grid-cols-3 gap-4">
        <button
          onClick={() => { setStatusFilter('pending'); setOffset(0); }}
          className={`p-4 rounded-xl border transition-colors ${
            statusFilter === 'pending'
              ? 'bg-yellow-500/20 border-yellow-500/30'
              : 'bg-zinc-900 border-zinc-800 hover:border-zinc-700'
          }`}
        >
          <p className="text-2xl font-bold text-yellow-400">{counts.pending}</p>
          <p className="text-sm text-zinc-400">Pending</p>
        </button>
        <button
          onClick={() => { setStatusFilter('matched'); setOffset(0); }}
          className={`p-4 rounded-xl border transition-colors ${
            statusFilter === 'matched'
              ? 'bg-green-500/20 border-green-500/30'
              : 'bg-zinc-900 border-zinc-800 hover:border-zinc-700'
          }`}
        >
          <p className="text-2xl font-bold text-green-400">{counts.matched}</p>
          <p className="text-sm text-zinc-400">Matched</p>
        </button>
        <button
          onClick={() => { setStatusFilter(''); setOffset(0); }}
          className={`p-4 rounded-xl border transition-colors ${
            statusFilter === ''
              ? 'bg-blue-500/20 border-blue-500/30'
              : 'bg-zinc-900 border-zinc-800 hover:border-zinc-700'
          }`}
        >
          <p className="text-2xl font-bold text-blue-400">{total}</p>
          <p className="text-sm text-zinc-400">Total</p>
        </button>
      </div>

      {/* Toolbar */}
      <div className="flex items-center justify-between gap-4 flex-wrap">
        <div className="flex items-center gap-3">
          <button
            onClick={handleSelectAll}
            className="px-3 py-1.5 bg-zinc-800 text-zinc-300 rounded-lg text-sm hover:bg-zinc-700"
          >
            {selectedIds.size > 0 ? 'Deselect All' : 'Select All'}
          </button>

          {selectedIds.size > 0 && (
            <>
              <span className="text-sm text-zinc-500">{selectedIds.size} selected</span>
              <button
                onClick={() => handleBatchProcess('add_to_existing')}
                disabled={processing}
                className="px-3 py-1.5 bg-green-500/20 text-green-400 rounded-lg text-sm hover:bg-green-500/30 flex items-center gap-1 disabled:opacity-50"
              >
                <Zap className="w-3 h-3" />
                Process All Matched
              </button>
            </>
          )}
        </div>

        <button
          onClick={fetchQueue}
          disabled={loading}
          className="flex items-center gap-2 px-3 py-1.5 bg-zinc-800 rounded-lg text-sm text-zinc-400 hover:text-white"
        >
          <RefreshCw className={`w-4 h-4 ${loading ? 'animate-spin' : ''}`} />
          Refresh
        </button>
      </div>

      {/* Error state */}
      {error && (
        <div className="bg-red-500/10 border border-red-500/20 rounded-lg p-4">
          <p className="text-sm text-red-400">{error}</p>
        </div>
      )}

      {/* Queue Items */}
      <div className="space-y-2">
        {loading ? (
          <div className="flex items-center justify-center py-12">
            <Loader2 className="w-6 h-6 text-orange-500 animate-spin" />
          </div>
        ) : queue.length === 0 ? (
          <div className="text-center py-12 bg-zinc-900 border border-zinc-800 rounded-xl">
            <QrCode className="w-12 h-12 text-zinc-700 mx-auto mb-3" />
            <p className="text-zinc-500">No items in queue</p>
            <p className="text-xs text-zinc-600 mt-1">Scan barcodes from the mobile app to add items</p>
          </div>
        ) : (
          queue.map(item => (
            <QueueItem
              key={item.id}
              item={item}
              selected={selectedIds.has(item.id)}
              onSelect={handleSelect}
              onProcess={handleProcess}
              onSkip={handleSkip}
            />
          ))
        )}
      </div>

      {/* Pagination */}
      {total > limit && (
        <div className="flex items-center justify-between">
          <p className="text-sm text-zinc-500">
            Showing {offset + 1} - {Math.min(offset + limit, total)} of {total}
          </p>
          <div className="flex items-center gap-2">
            <button
              onClick={() => setOffset(o => Math.max(0, o - limit))}
              disabled={offset === 0}
              className="p-1.5 hover:bg-zinc-800 rounded disabled:opacity-50"
            >
              <ChevronLeft className="w-4 h-4 text-zinc-400" />
            </button>
            <span className="text-sm text-zinc-400">
              Page {currentPage} of {totalPages}
            </span>
            <button
              onClick={() => setOffset(o => o + limit)}
              disabled={offset + limit >= total}
              className="p-1.5 hover:bg-zinc-800 rounded disabled:opacity-50"
            >
              <ChevronRight className="w-4 h-4 text-zinc-400" />
            </button>
          </div>
        </div>
      )}

      {/* Processing overlay */}
      {processing && (
        <div className="fixed inset-0 z-50 bg-black/50 flex items-center justify-center">
          <div className="bg-zinc-900 border border-zinc-800 rounded-xl p-6 flex items-center gap-3">
            <Loader2 className="w-5 h-5 text-orange-500 animate-spin" />
            <span className="text-white">Processing...</span>
          </div>
        </div>
      )}
    </div>
  );
}
