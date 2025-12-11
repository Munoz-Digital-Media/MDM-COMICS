/**
 * BarcodeScanner - Camera-based barcode scanner using html5-qrcode
 * Phase 4: MDM Admin Console Inventory System v1.3.0
 *
 * NASTY-017 FIX: Dynamic import to avoid loading ~100KB for non-scanner pages.
 */
import React, { useEffect, useRef, useState, useCallback } from 'react';
import { Camera, Pause, Play, Lightbulb, LightbulbOff } from 'lucide-react';

export default function BarcodeScanner({ onScan, onError, paused = false }) {
  const containerRef = useRef(null);
  const scannerRef = useRef(null);
  const [isReady, setIsReady] = useState(false);
  const [isPaused, setIsPaused] = useState(paused);
  const [torchOn, setTorchOn] = useState(false);
  const [torchAvailable, setTorchAvailable] = useState(false);
  const lastScanRef = useRef(null);

  // Debounce duplicate scans
  const handleScan = useCallback((decodedText, decodedResult) => {
    // Ignore if same code within 2 seconds
    if (lastScanRef.current === decodedText) return;

    lastScanRef.current = decodedText;
    setTimeout(() => {
      if (lastScanRef.current === decodedText) {
        lastScanRef.current = null;
      }
    }, 2000);

    // Haptic feedback
    if (navigator.vibrate) {
      navigator.vibrate(100);
    }

    // Detect barcode format
    const format = decodedResult?.result?.format?.formatName || 'UNKNOWN';

    onScan?.(decodedText, format);
  }, [onScan]);

  useEffect(() => {
    let html5QrCode = null;

    const initScanner = async () => {
      try {
        // Dynamic import to avoid bundle bloat
        const { Html5Qrcode, Html5QrcodeSupportedFormats } = await import('html5-qrcode');

        if (!containerRef.current) return;

        html5QrCode = new Html5Qrcode('scanner-container');
        scannerRef.current = html5QrCode;

        const config = {
          fps: 10,
          qrbox: { width: 250, height: 120 },
          formatsToSupport: [
            Html5QrcodeSupportedFormats.UPC_A,
            Html5QrcodeSupportedFormats.UPC_E,
            Html5QrcodeSupportedFormats.EAN_13,
            Html5QrcodeSupportedFormats.EAN_8,
            Html5QrcodeSupportedFormats.CODE_128,
            Html5QrcodeSupportedFormats.CODE_39,
          ],
          experimentalFeatures: {
            useBarCodeDetectorIfSupported: true,
          },
        };

        await html5QrCode.start(
          { facingMode: 'environment' },
          config,
          handleScan,
          (errorMessage) => {
            // Ignore common errors during scanning
          }
        );

        setIsReady(true);

        // Check if torch is available
        try {
          const capabilities = html5QrCode.getRunningTrackCameraCapabilities?.();
          if (capabilities?.torchFeature?.isSupported?.()) {
            setTorchAvailable(true);
          }
        } catch (e) {
          // Torch not available
        }
      } catch (error) {
        console.error('[BarcodeScanner] Init error:', error);
        onError?.(error);
      }
    };

    initScanner();

    return () => {
      if (html5QrCode) {
        html5QrCode.stop().catch(() => {});
      }
    };
  }, [handleScan, onError]);

  // Handle pause/resume
  useEffect(() => {
    if (!scannerRef.current || !isReady) return;

    if (isPaused) {
      scannerRef.current.pause();
    } else {
      scannerRef.current.resume();
    }
  }, [isPaused, isReady]);

  // External pause control
  useEffect(() => {
    setIsPaused(paused);
  }, [paused]);

  const togglePause = () => {
    setIsPaused(!isPaused);
  };

  const toggleTorch = async () => {
    if (!scannerRef.current || !torchAvailable) return;

    try {
      const capabilities = scannerRef.current.getRunningTrackCameraCapabilities();
      if (capabilities?.torchFeature) {
        await capabilities.torchFeature.apply(!torchOn);
        setTorchOn(!torchOn);
      }
    } catch (e) {
      console.error('[BarcodeScanner] Torch toggle error:', e);
    }
  };

  return (
    <div className="relative">
      {/* Scanner container */}
      <div
        id="scanner-container"
        ref={containerRef}
        className="w-full bg-black rounded-xl overflow-hidden"
        style={{ minHeight: '280px' }}
      />

      {/* Overlay with scan guide */}
      {isReady && (
        <div className="absolute inset-0 pointer-events-none flex items-center justify-center">
          {/* Scan area guide */}
          <div className="relative w-64 h-28 border-2 border-orange-500/50 rounded-lg">
            {/* Corner indicators */}
            <div className="absolute -top-1 -left-1 w-4 h-4 border-t-2 border-l-2 border-orange-500" />
            <div className="absolute -top-1 -right-1 w-4 h-4 border-t-2 border-r-2 border-orange-500" />
            <div className="absolute -bottom-1 -left-1 w-4 h-4 border-b-2 border-l-2 border-orange-500" />
            <div className="absolute -bottom-1 -right-1 w-4 h-4 border-b-2 border-r-2 border-orange-500" />

            {/* Scanning line animation */}
            {!isPaused && (
              <div className="absolute inset-x-2 h-0.5 bg-orange-500 animate-scan-line" />
            )}
          </div>
        </div>
      )}

      {/* Paused overlay */}
      {isPaused && isReady && (
        <div className="absolute inset-0 bg-black/50 flex items-center justify-center">
          <div className="text-center">
            <Pause className="w-12 h-12 text-white mx-auto mb-2" />
            <p className="text-white font-medium">Scanner Paused</p>
          </div>
        </div>
      )}

      {/* Controls */}
      <div className="absolute bottom-4 left-0 right-0 flex justify-center gap-4">
        <button
          onClick={togglePause}
          className="p-3 bg-zinc-800/80 backdrop-blur rounded-full hover:bg-zinc-700/80 transition-colors"
          title={isPaused ? 'Resume' : 'Pause'}
        >
          {isPaused ? (
            <Play className="w-5 h-5 text-white" />
          ) : (
            <Pause className="w-5 h-5 text-white" />
          )}
        </button>

        {torchAvailable && (
          <button
            onClick={toggleTorch}
            className={`p-3 rounded-full transition-colors ${
              torchOn
                ? 'bg-yellow-500/80 hover:bg-yellow-400/80'
                : 'bg-zinc-800/80 backdrop-blur hover:bg-zinc-700/80'
            }`}
            title={torchOn ? 'Turn off flashlight' : 'Turn on flashlight'}
          >
            {torchOn ? (
              <LightbulbOff className="w-5 h-5 text-zinc-900" />
            ) : (
              <Lightbulb className="w-5 h-5 text-white" />
            )}
          </button>
        )}
      </div>

      {/* Loading state */}
      {!isReady && (
        <div className="absolute inset-0 bg-zinc-900 flex items-center justify-center">
          <div className="text-center">
            <Camera className="w-12 h-12 text-orange-500 mx-auto mb-2 animate-pulse" />
            <p className="text-zinc-400">Starting camera...</p>
          </div>
        </div>
      )}

      {/* CSS for scan line animation */}
      <style>{`
        @keyframes scanLine {
          0% { top: 0; }
          50% { top: calc(100% - 2px); }
          100% { top: 0; }
        }
        .animate-scan-line {
          animation: scanLine 2s ease-in-out infinite;
        }
      `}</style>
    </div>
  );
}
