/**
 * ScannerApp - Main scanner PWA entry point
 * Phase 4: MDM Admin Console Inventory System v1.3.0
 *
 * Mobile-optimized barcode scanner with offline queue support.
 */
import React, { useState, useEffect, useCallback, lazy, Suspense } from 'react';
import {
  QrCode, Wifi, WifiOff, Upload, Check, X,
  List, Keyboard, ChevronDown, ChevronUp, Trash2, Loader2
} from 'lucide-react';
import CameraPermission from './CameraPermission';
import scannerDB, { queueBarcode, getQueueStats, getAllBarcodes, syncQueue, detectBarcodeType, clearQueue } from '../../services/scannerDB';

// Lazy load the scanner to avoid bundle bloat
const BarcodeScanner = lazy(() => import('./BarcodeScanner'));

// API_BASE includes /api suffix - match pattern from api.js
const API_BASE = import.meta.env.VITE_API_URL ||
  (window.location.hostname === 'localhost' ? 'http://localhost:8000/api' : 'https://api.mdmcomics.com/api');

export default function ScannerApp({ onClose }) {
  const [cameraReady, setCameraReady] = useState(false);
  const [lastScan, setLastScan] = useState(null);
  const [queueStats, setQueueStats] = useState({ total: 0, pending: 0, synced: 0 });
  const [isOnline, setIsOnline] = useState(navigator.onLine);
  const [isSyncing, setIsSyncing] = useState(false);
  const [showQueue, setShowQueue] = useState(false);
  const [showManual, setShowManual] = useState(false);
  const [manualBarcode, setManualBarcode] = useState('');
  const [queueItems, setQueueItems] = useState([]);
  const [notification, setNotification] = useState(null);

  // Update queue stats
  const updateStats = useCallback(async () => {
    try {
      const stats = await getQueueStats();
      setQueueStats(stats);
    } catch (e) {
      console.error('[ScannerApp] Failed to get queue stats:', e);
    }
  }, []);

  // Load queue items
  const loadQueueItems = useCallback(async () => {
    try {
      const items = await getAllBarcodes();
      setQueueItems(items);
    } catch (e) {
      console.error('[ScannerApp] Failed to load queue:', e);
    }
  }, []);

  // Online/offline detection
  useEffect(() => {
    const handleOnline = () => setIsOnline(true);
    const handleOffline = () => setIsOnline(false);

    window.addEventListener('online', handleOnline);
    window.addEventListener('offline', handleOffline);

    return () => {
      window.removeEventListener('online', handleOnline);
      window.removeEventListener('offline', handleOffline);
    };
  }, []);

  // Initial load
  useEffect(() => {
    updateStats();
  }, [updateStats]);

  // Auto-sync when online
  useEffect(() => {
    if (isOnline && queueStats.pending > 0 && !isSyncing) {
      handleSync();
    }
  }, [isOnline]);

  // Show notification
  const showNotification = (message, type = 'success') => {
    setNotification({ message, type });
    setTimeout(() => setNotification(null), 3000);
  };

  // Handle successful scan
  const handleScan = async (barcode, format) => {
    try {
      const barcodeType = detectBarcodeType(barcode);

      await queueBarcode(barcode, barcodeType);

      setLastScan({
        barcode,
        type: barcodeType,
        format,
        time: new Date().toLocaleTimeString(),
      });

      updateStats();
      showNotification(`Queued: ${barcode}`);

      // Try to sync immediately if online
      if (isOnline) {
        // Small delay to prevent overwhelming the server
        setTimeout(() => handleSync(), 500);
      }
    } catch (e) {
      console.error('[ScannerApp] Failed to queue barcode:', e);
      showNotification('Failed to save barcode', 'error');
    }
  };

  // Handle manual entry
  const handleManualSubmit = async (e) => {
    e.preventDefault();
    if (!manualBarcode.trim()) return;

    const barcode = manualBarcode.trim();
    const barcodeType = detectBarcodeType(barcode);

    try {
      await queueBarcode(barcode, barcodeType);
      setManualBarcode('');
      setShowManual(false);
      updateStats();
      showNotification(`Queued: ${barcode}`);

      if (isOnline) {
        setTimeout(() => handleSync(), 500);
      }
    } catch (e) {
      showNotification('Failed to save barcode', 'error');
    }
  };

  // Handle sync
  const handleSync = async () => {
    if (isSyncing || !isOnline) return;

    setIsSyncing(true);
    try {
      const result = await syncQueue(`${API_BASE}/admin/barcode-queue`);

      if (result.synced > 0) {
        showNotification(`Synced ${result.synced} barcodes`);
        updateStats();
      } else if (result.failed > 0) {
        showNotification(`Sync failed: ${result.error}`, 'error');
      }
    } catch (e) {
      showNotification('Sync failed', 'error');
    } finally {
      setIsSyncing(false);
    }
  };

  // Handle clear queue
  const handleClearQueue = async () => {
    if (!confirm('Clear all scanned barcodes? This cannot be undone.')) return;

    try {
      await clearQueue();
      updateStats();
      setQueueItems([]);
      showNotification('Queue cleared');
    } catch (e) {
      showNotification('Failed to clear queue', 'error');
    }
  };

  // Toggle queue view
  const toggleQueue = () => {
    setShowQueue(!showQueue);
    if (!showQueue) {
      loadQueueItems();
    }
  };

  return (
    <div className="fixed inset-0 z-50 bg-zinc-950 flex flex-col">
      {/* Header */}
      <header className="bg-zinc-900 border-b border-zinc-800 px-4 py-3 flex items-center justify-between">
        <div className="flex items-center gap-2">
          <div className="w-8 h-8 bg-orange-500/20 rounded-lg flex items-center justify-center">
            <QrCode className="w-5 h-5 text-orange-400" />
          </div>
          <span className="font-bold text-white">MDM Scanner</span>
        </div>

        <div className="flex items-center gap-2">
          {/* Online indicator */}
          <div className={`flex items-center gap-1 px-2 py-1 rounded-full text-xs ${
            isOnline ? 'bg-green-500/20 text-green-400' : 'bg-red-500/20 text-red-400'
          }`}>
            {isOnline ? <Wifi className="w-3 h-3" /> : <WifiOff className="w-3 h-3" />}
            {isOnline ? 'Online' : 'Offline'}
          </div>

          {/* Close button */}
          <button
            onClick={onClose}
            className="p-2 hover:bg-zinc-800 rounded-lg"
          >
            <X className="w-5 h-5 text-zinc-400" />
          </button>
        </div>
      </header>

      {/* Notification */}
      {notification && (
        <div className={`absolute top-16 left-4 right-4 z-50 px-4 py-3 rounded-lg shadow-lg ${
          notification.type === 'error' ? 'bg-red-500' : 'bg-green-500'
        } text-white font-medium flex items-center gap-2`}>
          {notification.type === 'error' ? (
            <X className="w-4 h-4" />
          ) : (
            <Check className="w-4 h-4" />
          )}
          {notification.message}
        </div>
      )}

      {/* Main content */}
      <main className="flex-1 overflow-auto">
        <CameraPermission
          onGranted={() => setCameraReady(true)}
          onDenied={() => setCameraReady(false)}
        >
          {/* Scanner */}
          <div className="p-4">
            <Suspense
              fallback={
                <div className="w-full h-64 bg-zinc-800 rounded-xl flex items-center justify-center">
                  <Loader2 className="w-8 h-8 text-orange-500 animate-spin" />
                </div>
              }
            >
              <BarcodeScanner onScan={handleScan} />
            </Suspense>
          </div>

          {/* Last scan display */}
          {lastScan && (
            <div className="px-4 py-3 bg-zinc-900/50 border-y border-zinc-800">
              <p className="text-xs text-zinc-500 mb-1">Last scan</p>
              <div className="flex items-center justify-between">
                <span className="font-mono text-white">{lastScan.barcode}</span>
                <span className="text-xs text-zinc-500">
                  {lastScan.type} • {lastScan.time}
                </span>
              </div>
            </div>
          )}

          {/* Queue stats */}
          <div className="p-4">
            <div className="bg-zinc-900 border border-zinc-800 rounded-xl p-4">
              <div className="flex items-center justify-between mb-3">
                <span className="text-sm font-medium text-zinc-400">Queue Status</span>
                <button
                  onClick={handleSync}
                  disabled={!isOnline || isSyncing || queueStats.pending === 0}
                  className="flex items-center gap-1 px-3 py-1.5 bg-orange-500/20 text-orange-400 rounded-lg text-sm disabled:opacity-50"
                >
                  {isSyncing ? (
                    <Loader2 className="w-4 h-4 animate-spin" />
                  ) : (
                    <Upload className="w-4 h-4" />
                  )}
                  {isSyncing ? 'Syncing...' : 'Sync Now'}
                </button>
              </div>

              <div className="grid grid-cols-3 gap-4">
                <div className="text-center">
                  <p className="text-2xl font-bold text-white">{queueStats.total}</p>
                  <p className="text-xs text-zinc-500">Total</p>
                </div>
                <div className="text-center">
                  <p className="text-2xl font-bold text-yellow-400">{queueStats.pending}</p>
                  <p className="text-xs text-zinc-500">Pending</p>
                </div>
                <div className="text-center">
                  <p className="text-2xl font-bold text-green-400">{queueStats.synced}</p>
                  <p className="text-xs text-zinc-500">Synced</p>
                </div>
              </div>
            </div>
          </div>

          {/* Action buttons */}
          <div className="px-4 flex gap-3">
            <button
              onClick={toggleQueue}
              className="flex-1 flex items-center justify-center gap-2 px-4 py-3 bg-zinc-800 rounded-xl text-zinc-300 hover:bg-zinc-700"
            >
              <List className="w-5 h-5" />
              View Queue
              {showQueue ? <ChevronUp className="w-4 h-4" /> : <ChevronDown className="w-4 h-4" />}
            </button>

            <button
              onClick={() => setShowManual(!showManual)}
              className="flex-1 flex items-center justify-center gap-2 px-4 py-3 bg-zinc-800 rounded-xl text-zinc-300 hover:bg-zinc-700"
            >
              <Keyboard className="w-5 h-5" />
              Manual Entry
            </button>
          </div>

          {/* Manual entry form */}
          {showManual && (
            <div className="px-4 pt-4">
              <form onSubmit={handleManualSubmit} className="flex gap-2">
                <input
                  type="text"
                  value={manualBarcode}
                  onChange={(e) => setManualBarcode(e.target.value)}
                  placeholder="Enter barcode..."
                  className="flex-1 px-4 py-3 bg-zinc-800 border border-zinc-700 rounded-xl text-white placeholder-zinc-500 focus:outline-none focus:border-orange-500"
                  autoFocus
                />
                <button
                  type="submit"
                  disabled={!manualBarcode.trim()}
                  className="px-4 py-3 bg-orange-500 text-white rounded-xl font-semibold disabled:opacity-50"
                >
                  Add
                </button>
              </form>
            </div>
          )}

          {/* Queue list */}
          {showQueue && (
            <div className="px-4 pt-4 pb-8">
              <div className="bg-zinc-900 border border-zinc-800 rounded-xl overflow-hidden">
                <div className="flex items-center justify-between px-4 py-3 border-b border-zinc-800">
                  <span className="text-sm font-medium text-zinc-400">
                    Scanned Barcodes ({queueItems.length})
                  </span>
                  {queueItems.length > 0 && (
                    <button
                      onClick={handleClearQueue}
                      className="flex items-center gap-1 text-xs text-red-400 hover:text-red-300"
                    >
                      <Trash2 className="w-3 h-3" />
                      Clear All
                    </button>
                  )}
                </div>

                {queueItems.length === 0 ? (
                  <p className="text-sm text-zinc-500 text-center py-8">No barcodes scanned yet</p>
                ) : (
                  <div className="divide-y divide-zinc-800 max-h-64 overflow-auto">
                    {queueItems.slice(0, 50).map(item => (
                      <div key={item.id} className="px-4 py-2 flex items-center justify-between">
                        <div>
                          <p className="font-mono text-sm text-white">{item.barcode}</p>
                          <p className="text-xs text-zinc-500">
                            {item.barcode_type} • {new Date(item.scanned_at).toLocaleString()}
                          </p>
                        </div>
                        <div className={`w-2 h-2 rounded-full ${item.synced ? 'bg-green-500' : 'bg-yellow-500'}`} />
                      </div>
                    ))}
                  </div>
                )}
              </div>
            </div>
          )}
        </CameraPermission>
      </main>

      {/* Custom styles */}
      <style>{`
        @import url('https://fonts.googleapis.com/css2?family=Barlow:wght@400;500;600;700&display=swap');
        .font-comic { font-family: 'Bangers', cursive; }
      `}</style>
    </div>
  );
}
