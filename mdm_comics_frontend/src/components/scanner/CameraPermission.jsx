/**
 * CameraPermission - Handles camera permission request and denied state
 * Phase 4: MDM Admin Console Inventory System v1.3.0
 *
 * NASTY-015 FIX: Graceful permission denied UI with clear instructions.
 */
import React, { useState, useEffect } from 'react';
import { Camera, RefreshCw, AlertTriangle, Settings } from 'lucide-react';

export default function CameraPermission({ onGranted, onDenied, children }) {
  const [status, setStatus] = useState('checking'); // checking, granted, denied, error
  const [errorMessage, setErrorMessage] = useState('');

  useEffect(() => {
    checkCameraPermission();
  }, []);

  const checkCameraPermission = async () => {
    setStatus('checking');

    try {
      // First try the Permissions API
      if (navigator.permissions && navigator.permissions.query) {
        try {
          const result = await navigator.permissions.query({ name: 'camera' });

          if (result.state === 'denied') {
            setStatus('denied');
            onDenied?.();
            return;
          }

          if (result.state === 'granted') {
            setStatus('granted');
            onGranted?.();
            return;
          }

          // If 'prompt', we need to try getUserMedia to trigger the permission dialog
        } catch (permErr) {
          // Permissions API not supported for camera, fall through to getUserMedia
          // LOW-001: Gate console.log behind DEV mode
          if (import.meta.env.DEV) console.log('[CameraPermission] Permissions API not available, trying getUserMedia');
        }
      }

      // Try getUserMedia directly
      const stream = await navigator.mediaDevices.getUserMedia({
        video: { facingMode: 'environment' }
      });

      // Permission granted, stop the test stream
      stream.getTracks().forEach(track => track.stop());

      setStatus('granted');
      onGranted?.();
    } catch (error) {
      // LOW-001: Gate console.error behind DEV mode
      if (import.meta.env.DEV) console.error('[CameraPermission] Error:', error);

      if (error.name === 'NotAllowedError' || error.name === 'PermissionDeniedError') {
        setStatus('denied');
        onDenied?.();
      } else if (error.name === 'NotFoundError' || error.name === 'DevicesNotFoundError') {
        setStatus('error');
        setErrorMessage('No camera found on this device.');
      } else if (error.name === 'NotReadableError' || error.name === 'TrackStartError') {
        setStatus('error');
        setErrorMessage('Camera is in use by another application.');
      } else {
        setStatus('error');
        setErrorMessage(error.message || 'Could not access camera.');
      }
    }
  };

  const handleRetry = () => {
    checkCameraPermission();
  };

  // Checking state
  if (status === 'checking') {
    return (
      <div className="flex flex-col items-center justify-center min-h-[300px] p-6">
        <div className="w-16 h-16 bg-zinc-800 rounded-full flex items-center justify-center mb-4 animate-pulse">
          <Camera className="w-8 h-8 text-orange-500" />
        </div>
        <p className="text-white font-medium">Checking camera access...</p>
      </div>
    );
  }

  // Permission granted - render children
  if (status === 'granted') {
    return children;
  }

  // Permission denied
  if (status === 'denied') {
    return (
      <div className="flex flex-col items-center justify-center min-h-[400px] p-6 text-center">
        <div className="w-20 h-20 bg-red-500/20 rounded-full flex items-center justify-center mb-6">
          <Camera className="w-10 h-10 text-red-400" />
        </div>

        <h2 className="text-xl font-bold text-white mb-2">Camera Access Required</h2>
        <p className="text-zinc-400 mb-6 max-w-sm">
          To scan barcodes, please enable camera access in your browser settings.
        </p>

        <div className="bg-zinc-800/50 rounded-xl p-4 mb-6 text-left max-w-sm">
          <h3 className="text-sm font-semibold text-zinc-300 mb-3 flex items-center gap-2">
            <Settings className="w-4 h-4" />
            How to enable camera:
          </h3>
          <ol className="text-sm text-zinc-400 space-y-2">
            <li className="flex gap-2">
              <span className="text-orange-400 font-bold">1.</span>
              Tap the lock icon (or site info) in your browser's address bar
            </li>
            <li className="flex gap-2">
              <span className="text-orange-400 font-bold">2.</span>
              Find "Camera" in the permissions list
            </li>
            <li className="flex gap-2">
              <span className="text-orange-400 font-bold">3.</span>
              Change it to "Allow"
            </li>
            <li className="flex gap-2">
              <span className="text-orange-400 font-bold">4.</span>
              Refresh this page
            </li>
          </ol>
        </div>

        <button
          onClick={handleRetry}
          className="flex items-center gap-2 px-6 py-3 bg-orange-500 text-white rounded-xl font-semibold hover:bg-orange-600 transition-colors"
        >
          <RefreshCw className="w-4 h-4" />
          Try Again
        </button>
      </div>
    );
  }

  // Error state
  return (
    <div className="flex flex-col items-center justify-center min-h-[400px] p-6 text-center">
      <div className="w-20 h-20 bg-yellow-500/20 rounded-full flex items-center justify-center mb-6">
        <AlertTriangle className="w-10 h-10 text-yellow-400" />
      </div>

      <h2 className="text-xl font-bold text-white mb-2">Camera Error</h2>
      <p className="text-zinc-400 mb-6 max-w-sm">{errorMessage}</p>

      <button
        onClick={handleRetry}
        className="flex items-center gap-2 px-6 py-3 bg-zinc-700 text-white rounded-xl font-semibold hover:bg-zinc-600 transition-colors"
      >
        <RefreshCw className="w-4 h-4" />
        Retry
      </button>
    </div>
  );
}
