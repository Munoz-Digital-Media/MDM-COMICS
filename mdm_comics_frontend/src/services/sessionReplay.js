/**
 * Session Replay using rrweb
 *
 * Full DOM recording with PII masking.
 */

import { record } from 'rrweb';
import { analytics } from './analyticsCollector';

const API_BASE = import.meta.env.VITE_API_URL || 'http://localhost:8080/api';
const REPLAY_URL = `${API_BASE}/analytics/beacon/replay`;

// Recording state
let stopRecordingFn = null;
let eventBuffer = [];
let chunkIndex = 0;
let flushIntervalId = null;  // FE-001: Track interval for cleanup
const CHUNK_SIZE = 100; // Events per chunk
const CHUNK_INTERVAL = 10000; // 10 seconds

// PII masking configuration
const MASK_TEXT_SELECTOR = 'input[type="password"], input[type="email"], input[name*="card"], input[name*="cvv"], input[name*="ssn"], .pii-mask';
const BLOCK_SELECTOR = '.pii-block, [data-pii="block"]';

/**
 * Start session recording
 */
function startRecording() {
  if (analytics.isOptedOut()) return;
  if (stopRecordingFn) return; // Already recording

  const sessionId = analytics.getSessionId();
  let hasErrors = false;

  stopRecordingFn = record({
    emit(event) {
      eventBuffer.push(event);

      // Check for errors in the event
      if (event.type === 5 && event.data?.plugin === 'rrweb/console@1') {
        if (event.data.payload?.level === 'error') {
          hasErrors = true;
        }
      }

      // Flush if buffer is full
      if (eventBuffer.length >= CHUNK_SIZE) {
        flushReplayChunk(sessionId, hasErrors);
        hasErrors = false;
      }
    },

    // PII masking
    maskTextSelector: MASK_TEXT_SELECTOR,
    blockSelector: BLOCK_SELECTOR,
    maskAllInputs: true,

    // Sampling (record all mutations)
    sampling: {
      scroll: 150, // Throttle scroll events
      media: 800,  // Throttle media time updates
      input: 'last', // Only last input value
    },

    // Record console logs for debugging
    plugins: [],

    // Inline styles and images
    inlineStylesheet: true,

    // Collect fonts
    collectFonts: true,
  });

  // FE-001: Set up periodic flush with tracked interval ID for cleanup
  flushIntervalId = setInterval(() => {
    if (eventBuffer.length > 0) {
      flushReplayChunk(sessionId, hasErrors);
      hasErrors = false;
    }
  }, CHUNK_INTERVAL);

  if (import.meta.env.DEV) {
    console.debug('Session replay started');
  }
}

/**
 * Send replay chunk to server
 */
async function flushReplayChunk(sessionId, hasErrors = false) {
  if (eventBuffer.length === 0) return;

  const events = [...eventBuffer];
  eventBuffer = [];

  const startTimestamp = events[0].timestamp;
  const endTimestamp = events[events.length - 1].timestamp;

  // Compress events
  const data = JSON.stringify(events);
  const compressed = await compressData(data);

  try {
    if (navigator.sendBeacon) {
      // Use fetch with headers for reliability
      await fetch(REPLAY_URL, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/octet-stream',
          'Content-Encoding': 'gzip',
          'X-Session-ID': sessionId,
          'X-Chunk-Index': String(chunkIndex),
          'X-Event-Count': String(events.length),
          'X-Start-Timestamp': String(startTimestamp),
          'X-End-Timestamp': String(endTimestamp),
          'X-Has-Errors': String(hasErrors),
        },
        body: compressed,
        keepalive: true,
        credentials: 'include',
      });
    }

    chunkIndex++;
  } catch (e) {
    console.debug('Replay chunk send failed:', e);
  }
}

/**
 * Compress data using CompressionStream (or fallback)
 */
async function compressData(data) {
  if (typeof CompressionStream !== 'undefined') {
    const stream = new Blob([data]).stream();
    const compressedStream = stream.pipeThrough(new CompressionStream('gzip'));
    return new Response(compressedStream).blob();
  }

  // Fallback: send uncompressed
  return new Blob([data]);
}

/**
 * Stop recording
 * FE-001: Now properly cleans up interval to prevent memory leaks
 */
function stopRecording() {
  // FE-001: Clear the flush interval first
  if (flushIntervalId) {
    clearInterval(flushIntervalId);
    flushIntervalId = null;
  }

  if (stopRecordingFn) {
    stopRecordingFn();
    stopRecordingFn = null;

    // Flush remaining events
    if (eventBuffer.length > 0) {
      flushReplayChunk(analytics.getSessionId(), false);
    }
  }
}

export const replay = {
  start: startRecording,
  stop: stopRecording,
};

export default replay;
