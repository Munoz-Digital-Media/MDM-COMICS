/**
 * Scanner IndexedDB Service
 * Phase 4: MDM Admin Console Inventory System v1.3.0
 *
 * Handles offline barcode queue storage with schema versioning.
 * NASTY-006 FIX: Proper DB_VERSION and onupgradeneeded handler.
 * MW-002: Console logging gated behind DEV mode per constitution_logging.json
 */

const DB_NAME = 'mdm_scanner';
const DB_VERSION = 2; // Increment on schema changes
const STORE_NAME = 'scan_queue';

let dbPromise = null;

// MW-002: Dev-only logging per constitution_logging.json Section 2
const devLog = (...args) => { if (import.meta.env.DEV) console.log(...args); };
const devError = (...args) => { if (import.meta.env.DEV) console.error(...args); };

/**
 * Open IndexedDB connection with schema migration support.
 */
export async function openDB() {
  if (dbPromise) return dbPromise;

  dbPromise = new Promise((resolve, reject) => {
    const request = indexedDB.open(DB_NAME, DB_VERSION);

    // Handle schema upgrades
    request.onupgradeneeded = (event) => {
      const db = event.target.result;
      const oldVersion = event.oldVersion;

      devLog(`[ScannerDB] Upgrading from v${oldVersion} to v${DB_VERSION}`);

      if (oldVersion < 1) {
        // Initial schema
        const store = db.createObjectStore(STORE_NAME, {
          keyPath: 'id',
          autoIncrement: true
        });
        store.createIndex('synced', 'synced', { unique: false });
        store.createIndex('barcode', 'barcode', { unique: false });
        devLog('[ScannerDB] Created initial schema');
      }

      if (oldVersion < 2) {
        // v2: Add scanned_at index for sorting
        const tx = event.target.transaction;
        const store = tx.objectStore(STORE_NAME);
        if (!store.indexNames.contains('scanned_at')) {
          store.createIndex('scanned_at', 'scanned_at', { unique: false });
          devLog('[ScannerDB] Added scanned_at index');
        }
      }
    };

    request.onsuccess = () => {
      devLog('[ScannerDB] Database opened successfully');
      resolve(request.result);
    };

    request.onerror = () => {
      devError('[ScannerDB] Failed to open database:', request.error);
      dbPromise = null;
      reject(request.error);
    };
  });

  return dbPromise;
}

/**
 * Queue a barcode for later sync.
 */
export async function queueBarcode(barcode, barcodeType = 'UPC') {
  const db = await openDB();

  return new Promise((resolve, reject) => {
    const tx = db.transaction(STORE_NAME, 'readwrite');
    const store = tx.objectStore(STORE_NAME);

    const record = {
      barcode,
      barcode_type: barcodeType,
      scanned_at: new Date().toISOString(),
      synced: false,
      sync_attempts: 0,
      created_at: new Date().toISOString(),
    };

    const request = store.add(record);

    request.onsuccess = () => {
      devLog(`[ScannerDB] Queued barcode: ${barcode}`);
      resolve(request.result);
    };

    request.onerror = () => {
      devError('[ScannerDB] Failed to queue barcode:', request.error);
      reject(request.error);
    };
  });
}

/**
 * Get all pending (unsynced) barcodes.
 */
export async function getPendingBarcodes() {
  const db = await openDB();

  return new Promise((resolve, reject) => {
    const tx = db.transaction(STORE_NAME, 'readonly');
    const store = tx.objectStore(STORE_NAME);
    const index = store.index('synced');

    const request = index.getAll(IDBKeyRange.only(false));

    request.onsuccess = () => {
      resolve(request.result || []);
    };

    request.onerror = () => {
      reject(request.error);
    };
  });
}

/**
 * Get all barcodes (synced and pending).
 */
export async function getAllBarcodes() {
  const db = await openDB();

  return new Promise((resolve, reject) => {
    const tx = db.transaction(STORE_NAME, 'readonly');
    const store = tx.objectStore(STORE_NAME);

    const request = store.getAll();

    request.onsuccess = () => {
      // Sort by scanned_at descending
      const results = request.result || [];
      results.sort((a, b) => new Date(b.scanned_at) - new Date(a.scanned_at));
      resolve(results);
    };

    request.onerror = () => {
      reject(request.error);
    };
  });
}

/**
 * Get queue statistics.
 */
export async function getQueueStats() {
  const all = await getAllBarcodes();
  const pending = all.filter(b => !b.synced);
  const synced = all.filter(b => b.synced);

  return {
    total: all.length,
    pending: pending.length,
    synced: synced.length,
    lastScan: all[0]?.scanned_at || null,
  };
}

/**
 * Mark barcodes as synced.
 */
export async function markAsSynced(ids) {
  const db = await openDB();

  return new Promise((resolve, reject) => {
    const tx = db.transaction(STORE_NAME, 'readwrite');
    const store = tx.objectStore(STORE_NAME);

    let completed = 0;
    const total = ids.length;

    ids.forEach(id => {
      const getRequest = store.get(id);

      getRequest.onsuccess = () => {
        const record = getRequest.result;
        if (record) {
          record.synced = true;
          record.synced_at = new Date().toISOString();
          store.put(record);
        }

        completed++;
        if (completed === total) {
          resolve();
        }
      };

      getRequest.onerror = () => {
        completed++;
        if (completed === total) {
          resolve();
        }
      };
    });

    if (total === 0) {
      resolve();
    }
  });
}

/**
 * Delete synced barcodes older than specified days.
 */
export async function cleanupSyncedBarcodes(olderThanDays = 7) {
  const db = await openDB();
  const cutoff = new Date();
  cutoff.setDate(cutoff.getDate() - olderThanDays);

  return new Promise((resolve, reject) => {
    const tx = db.transaction(STORE_NAME, 'readwrite');
    const store = tx.objectStore(STORE_NAME);
    const index = store.index('synced');

    const request = index.openCursor(IDBKeyRange.only(true));
    let deleted = 0;

    request.onsuccess = (event) => {
      const cursor = event.target.result;
      if (cursor) {
        const record = cursor.value;
        if (record.synced_at && new Date(record.synced_at) < cutoff) {
          cursor.delete();
          deleted++;
        }
        cursor.continue();
      } else {
        devLog(`[ScannerDB] Cleaned up ${deleted} old synced barcodes`);
        resolve(deleted);
      }
    };

    request.onerror = () => {
      reject(request.error);
    };
  });
}

/**
 * Clear all data from the queue.
 */
export async function clearQueue() {
  const db = await openDB();

  return new Promise((resolve, reject) => {
    const tx = db.transaction(STORE_NAME, 'readwrite');
    const store = tx.objectStore(STORE_NAME);

    const request = store.clear();

    request.onsuccess = () => {
      devLog('[ScannerDB] Queue cleared');
      resolve();
    };

    request.onerror = () => {
      reject(request.error);
    };
  });
}

/**
 * Sync pending barcodes to the server.
 */
export async function syncQueue(apiEndpoint) {
  const pending = await getPendingBarcodes();

  if (pending.length === 0) {
    devLog('[ScannerDB] No pending barcodes to sync');
    return { synced: 0, failed: 0 };
  }

  devLog(`[ScannerDB] Syncing ${pending.length} barcodes...`);

  try {
    const response = await fetch(apiEndpoint, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      credentials: 'include',
      body: JSON.stringify({
        barcodes: pending.map(b => ({
          barcode: b.barcode,
          barcode_type: b.barcode_type,
          scanned_at: b.scanned_at,
        })),
      }),
    });

    if (!response.ok) {
      throw new Error(`Sync failed: ${response.status}`);
    }

    const result = await response.json();

    // Mark successfully synced items
    await markAsSynced(pending.map(b => b.id));

    devLog(`[ScannerDB] Synced ${pending.length} barcodes`);
    return { synced: pending.length, failed: 0 };
  } catch (error) {
    devError('[ScannerDB] Sync failed:', error);
    return { synced: 0, failed: pending.length, error: error.message };
  }
}

/**
 * Detect barcode type from the barcode string.
 */
export function detectBarcodeType(barcode) {
  const cleaned = barcode.replace(/[^0-9X]/gi, '');

  // ISBN-13 (starts with 978 or 979)
  if (cleaned.length === 13 && (cleaned.startsWith('978') || cleaned.startsWith('979'))) {
    return 'ISBN';
  }

  // ISBN-10
  if (cleaned.length === 10) {
    return 'ISBN';
  }

  // UPC-A
  if (cleaned.length === 12) {
    return 'UPC';
  }

  // EAN-13
  if (cleaned.length === 13) {
    return 'EAN';
  }

  // UPC-E
  if (cleaned.length === 8) {
    return 'UPC';
  }

  return 'UPC'; // Default
}

export default {
  openDB,
  queueBarcode,
  getPendingBarcodes,
  getAllBarcodes,
  getQueueStats,
  markAsSynced,
  cleanupSyncedBarcodes,
  clearQueue,
  syncQueue,
  detectBarcodeType,
};
