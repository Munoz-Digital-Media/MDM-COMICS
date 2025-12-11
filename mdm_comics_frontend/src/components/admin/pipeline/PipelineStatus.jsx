/**
 * PipelineStatus - GCD Import Progress Tracker
 * Displays real-time progress of the GCD data import pipeline
 *
 * Features:
 * - Option 1: Live Import Stats (rate, ETA, current batch)
 * - Option 5: Data Quality Summary (metadata completeness, cover images, etc.)
 */
import React, { useState, useEffect, useCallback, useRef } from 'react';
import {
  Database, RefreshCw, Loader2, CheckCircle, XCircle,
  AlertTriangle, Play, Pause, RotateCcw, Clock, Zap,
  Image, FileText, BookOpen, Building2, BarChart3
} from 'lucide-react';
import { adminAPI } from '../../../services/adminApi';

function formatNumber(num) {
  if (num === null || num === undefined) return '0';
  return num.toLocaleString();
}

function formatPercent(num) {
  if (num === null || num === undefined) return '0.00';
  return num.toFixed(2);
}

function formatDuration(seconds) {
  if (!seconds || seconds <= 0) return '--';
  const hours = Math.floor(seconds / 3600);
  const minutes = Math.floor((seconds % 3600) / 60);
  const secs = Math.floor(seconds % 60);
  if (hours > 0) {
    return `${hours}h ${minutes}m`;
  }
  if (minutes > 0) {
    return `${minutes}m ${secs}s`;
  }
  return `${secs}s`;
}

function formatRate(rate) {
  if (!rate || rate <= 0) return '--';
  if (rate >= 1000) {
    return `${(rate / 1000).toFixed(1)}k/s`;
  }
  return `${Math.round(rate)}/s`;
}

export default function PipelineStatus({ compact = false }) {
  const [status, setStatus] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [autoRefresh, setAutoRefresh] = useState(true);
  const [actionLoading, setActionLoading] = useState(false);

  // Option 1: Rate tracking state with exponential moving average (EMA)
  const [importRate, setImportRate] = useState(0);
  const lastCountRef = useRef({ count: 0, timestamp: Date.now() });
  const EMA_ALPHA = 0.3; // Smoothing factor (0.3 = 30% new, 70% old)

  const fetchStatus = useCallback(async () => {
    try {
      const data = await adminAPI.getGCDStatus();

      // Calculate import rate with EMA smoothing (Option 1)
      const now = Date.now();
      const prevCount = lastCountRef.current.count;
      const prevTime = lastCountRef.current.timestamp;
      const timeDiff = (now - prevTime) / 1000; // seconds

      if (timeDiff > 0 && prevCount > 0) {
        const countDiff = data.imported_count - prevCount;
        if (countDiff > 0) {
          // Calculate instantaneous rate
          const instantRate = countDiff / timeDiff;
          // Apply exponential moving average for smoother display
          setImportRate(prevRate =>
            prevRate > 0
              ? EMA_ALPHA * instantRate + (1 - EMA_ALPHA) * prevRate
              : instantRate
          );
        }
        // If count hasn't changed, gradually decay the rate toward 0
        else if (countDiff === 0) {
          setImportRate(prevRate => prevRate * 0.9); // Decay by 10%
        }
      }

      lastCountRef.current = { count: data.imported_count, timestamp: now };

      setStatus(data);
      setError(null);
    } catch (err) {
      console.error('Pipeline status fetch error:', err);
      setError(err.message);
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    fetchStatus();
  }, [fetchStatus]);

  // Auto-refresh every 5 seconds when import is running
  useEffect(() => {
    if (!autoRefresh || !status?.checkpoint?.is_running) return;

    const interval = setInterval(fetchStatus, 5000);
    return () => clearInterval(interval);
  }, [autoRefresh, status?.checkpoint?.is_running, fetchStatus]);

  const handleTriggerImport = async () => {
    setActionLoading(true);
    try {
      await adminAPI.triggerGCDImport({ max_records: 0, batch_size: 5000 });
      await fetchStatus();
    } catch (err) {
      setError(err.message);
    } finally {
      setActionLoading(false);
    }
  };

  const handleResetCheckpoint = async () => {
    setActionLoading(true);
    try {
      await adminAPI.resetGCDCheckpoint();
      await fetchStatus();
    } catch (err) {
      setError(err.message);
    } finally {
      setActionLoading(false);
    }
  };

  if (loading) {
    return (
      <div className="bg-zinc-900 border border-zinc-800 rounded-xl p-5">
        <div className="flex items-center gap-2 text-zinc-500">
          <Loader2 className="w-4 h-4 animate-spin" />
          <span className="text-sm">Loading pipeline status...</span>
        </div>
      </div>
    );
  }

  if (error && !status) {
    return (
      <div className="bg-zinc-900 border border-zinc-800 rounded-xl p-5">
        <div className="flex items-center justify-between">
          <div className="flex items-center gap-2 text-red-400">
            <AlertTriangle className="w-4 h-4" />
            <span className="text-sm">{error}</span>
          </div>
          <button
            onClick={fetchStatus}
            className="p-2 hover:bg-zinc-800 rounded-lg transition-colors"
          >
            <RefreshCw className="w-4 h-4 text-zinc-400" />
          </button>
        </div>
      </div>
    );
  }

  const checkpoint = status?.checkpoint || {};
  const settings = status?.settings || {};
  const dataQuality = status?.data_quality || null;
  const isRunning = checkpoint.is_running;
  const totalInDump = settings.dump_total_count || 0;
  const importedCount = status?.imported_count || 0;
  const progress = totalInDump > 0 ? (importedCount / totalInDump) * 100 : 0;
  const remaining = totalInDump - importedCount;

  // Option 1: Calculate ETA
  const eta = importRate > 0 ? remaining / importRate : null;

  // Compact view for dashboard
  if (compact) {
    return (
      <div className="bg-zinc-900 border border-zinc-800 rounded-xl p-5">
        <div className="flex items-center justify-between mb-3">
          <div className="flex items-center gap-2">
            <Database className="w-4 h-4 text-blue-400" />
            <h3 className="text-sm font-semibold text-zinc-400">GCD Import</h3>
          </div>
          <div className="flex items-center gap-2">
            {isRunning ? (
              <span className="flex items-center gap-1 text-xs px-2 py-1 bg-blue-500/20 text-blue-400 rounded-full">
                <Loader2 className="w-3 h-3 animate-spin" />
                Running
              </span>
            ) : progress >= 100 ? (
              <span className="flex items-center gap-1 text-xs px-2 py-1 bg-green-500/20 text-green-400 rounded-full">
                <CheckCircle className="w-3 h-3" />
                Complete
              </span>
            ) : (
              <span className="flex items-center gap-1 text-xs px-2 py-1 bg-zinc-700 text-zinc-400 rounded-full">
                <Pause className="w-3 h-3" />
                Paused
              </span>
            )}
          </div>
        </div>

        {/* Progress bar */}
        <div className="mb-3">
          <div className="h-2 bg-zinc-800 rounded-full overflow-hidden">
            <div
              className={`h-full transition-all duration-500 ${
                isRunning ? 'bg-blue-500' : progress >= 100 ? 'bg-green-500' : 'bg-orange-500'
              }`}
              style={{ width: `${Math.min(progress, 100)}%` }}
            />
          </div>
        </div>

        <div className="flex items-center justify-between text-xs">
          <span className="text-zinc-500">
            {formatNumber(importedCount)} / {formatNumber(totalInDump)}
          </span>
          <span className={`font-medium ${isRunning ? 'text-blue-400' : 'text-zinc-400'}`}>
            {formatPercent(progress)}%
          </span>
        </div>

        {/* Compact rate/ETA when running */}
        {isRunning && importRate > 0 && (
          <div className="mt-2 flex items-center gap-3 text-xs text-zinc-500">
            <span className="flex items-center gap-1">
              <Zap className="w-3 h-3 text-yellow-400" />
              {formatRate(importRate)}
            </span>
            {eta && (
              <span className="flex items-center gap-1">
                <Clock className="w-3 h-3 text-blue-400" />
                ETA: {formatDuration(eta)}
              </span>
            )}
          </div>
        )}

        {checkpoint.total_errors > 0 && (
          <div className="mt-2 flex items-center gap-1 text-xs text-red-400">
            <XCircle className="w-3 h-3" />
            {checkpoint.total_errors} errors
          </div>
        )}
      </div>
    );
  }

  // Full view
  return (
    <div className="bg-zinc-900 border border-zinc-800 rounded-xl p-5 space-y-4">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-3">
          <div className="p-2 bg-blue-500/20 rounded-lg border border-blue-500/30">
            <Database className="w-5 h-5 text-blue-400" />
          </div>
          <div>
            <h3 className="font-semibold text-white">GCD Import Pipeline</h3>
            <p className="text-xs text-zinc-500">Grand Comics Database - 2.5M+ records</p>
          </div>
        </div>
        <div className="flex items-center gap-2">
          <button
            onClick={() => setAutoRefresh(!autoRefresh)}
            className={`p-2 rounded-lg transition-colors ${
              autoRefresh ? 'bg-blue-500/20 text-blue-400' : 'bg-zinc-800 text-zinc-500'
            }`}
            title={autoRefresh ? 'Auto-refresh ON' : 'Auto-refresh OFF'}
          >
            <RefreshCw className={`w-4 h-4 ${autoRefresh && isRunning ? 'animate-spin' : ''}`} />
          </button>
        </div>
      </div>

      {/* Status Badge */}
      <div className="flex items-center gap-3">
        {isRunning ? (
          <span className="flex items-center gap-2 px-3 py-1.5 bg-blue-500/20 text-blue-400 rounded-lg text-sm">
            <Loader2 className="w-4 h-4 animate-spin" />
            Import Running
          </span>
        ) : progress >= 100 ? (
          <span className="flex items-center gap-2 px-3 py-1.5 bg-green-500/20 text-green-400 rounded-lg text-sm">
            <CheckCircle className="w-4 h-4" />
            Import Complete
          </span>
        ) : (
          <span className="flex items-center gap-2 px-3 py-1.5 bg-zinc-800 text-zinc-400 rounded-lg text-sm">
            <Pause className="w-4 h-4" />
            Paused
          </span>
        )}

        {!isRunning && progress < 100 && (
          <button
            onClick={handleTriggerImport}
            disabled={actionLoading}
            className="flex items-center gap-2 px-3 py-1.5 bg-green-500/20 text-green-400 rounded-lg text-sm hover:bg-green-500/30 transition-colors disabled:opacity-50"
          >
            {actionLoading ? (
              <Loader2 className="w-4 h-4 animate-spin" />
            ) : (
              <Play className="w-4 h-4" />
            )}
            Resume Import
          </button>
        )}

        {checkpoint.last_error && (
          <button
            onClick={handleResetCheckpoint}
            disabled={actionLoading}
            className="flex items-center gap-2 px-3 py-1.5 bg-orange-500/20 text-orange-400 rounded-lg text-sm hover:bg-orange-500/30 transition-colors disabled:opacity-50"
          >
            <RotateCcw className="w-4 h-4" />
            Reset
          </button>
        )}
      </div>

      {/* Progress */}
      <div>
        <div className="flex items-center justify-between mb-2">
          <span className="text-sm text-zinc-400">Progress</span>
          <span className="text-sm font-medium text-white">{formatPercent(progress)}%</span>
        </div>
        <div className="h-3 bg-zinc-800 rounded-full overflow-hidden">
          <div
            className={`h-full transition-all duration-500 ${
              isRunning ? 'bg-blue-500' : progress >= 100 ? 'bg-green-500' : 'bg-orange-500'
            }`}
            style={{ width: `${Math.min(progress, 100)}%` }}
          />
        </div>
      </div>

      {/* Stats Grid */}
      <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
        <div className="bg-zinc-800/50 rounded-lg p-3">
          <p className="text-xs text-zinc-500 mb-1">Imported</p>
          <p className="text-lg font-bold text-white">{formatNumber(importedCount)}</p>
        </div>
        <div className="bg-zinc-800/50 rounded-lg p-3">
          <p className="text-xs text-zinc-500 mb-1">Remaining</p>
          <p className="text-lg font-bold text-white">{formatNumber(remaining)}</p>
        </div>
        <div className="bg-zinc-800/50 rounded-lg p-3">
          <p className="text-xs text-zinc-500 mb-1">Processed</p>
          <p className="text-lg font-bold text-white">{formatNumber(checkpoint.total_processed)}</p>
        </div>
        <div className="bg-zinc-800/50 rounded-lg p-3">
          <p className="text-xs text-zinc-500 mb-1">Errors</p>
          <p className={`text-lg font-bold ${checkpoint.total_errors > 0 ? 'text-red-400' : 'text-white'}`}>
            {formatNumber(checkpoint.total_errors)}
          </p>
        </div>
      </div>

      {/* Option 1: Live Import Stats (when running) */}
      {isRunning && (
        <div className="bg-blue-500/10 border border-blue-500/20 rounded-lg p-4">
          <div className="flex items-center gap-2 mb-3">
            <Zap className="w-4 h-4 text-yellow-400" />
            <h4 className="text-sm font-medium text-blue-400">Live Import Stats</h4>
          </div>
          <div className="grid grid-cols-2 md:grid-cols-3 gap-4">
            <div>
              <p className="text-xs text-zinc-500 mb-1">Import Rate</p>
              <p className="text-lg font-bold text-white">
                {importRate > 0 ? formatRate(importRate) : 'Calculating...'}
              </p>
            </div>
            <div>
              <p className="text-xs text-zinc-500 mb-1">Est. Time Remaining</p>
              <p className="text-lg font-bold text-white">
                {eta ? formatDuration(eta) : 'Calculating...'}
              </p>
            </div>
            <div>
              <p className="text-xs text-zinc-500 mb-1">Batch Size</p>
              <p className="text-lg font-bold text-white">{formatNumber(settings.batch_size || 5000)}</p>
            </div>
          </div>
          {checkpoint.last_run_started && (
            <p className="text-xs text-zinc-500 mt-3">
              Started: {new Date(checkpoint.last_run_started).toLocaleString()}
            </p>
          )}
        </div>
      )}

      {/* Option 5: Data Quality Summary (when not running or showing alongside) */}
      {dataQuality && !isRunning && (
        <div className="bg-purple-500/10 border border-purple-500/20 rounded-lg p-4">
          <div className="flex items-center gap-2 mb-3">
            <BarChart3 className="w-4 h-4 text-purple-400" />
            <h4 className="text-sm font-medium text-purple-400">Data Quality Summary</h4>
          </div>
          <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
            <div className="flex items-start gap-2">
              <Image className="w-4 h-4 text-green-400 mt-0.5" />
              <div>
                <p className="text-xs text-zinc-500">Cover Images</p>
                <p className="text-sm font-semibold text-white">
                  {dataQuality.pct_with_cover}%
                </p>
                <p className="text-xs text-zinc-600">
                  {formatNumber(dataQuality.with_cover_image)} records
                </p>
              </div>
            </div>
            <div className="flex items-start gap-2">
              <FileText className="w-4 h-4 text-blue-400 mt-0.5" />
              <div>
                <p className="text-xs text-zinc-500">Descriptions</p>
                <p className="text-sm font-semibold text-white">
                  {dataQuality.pct_with_description}%
                </p>
                <p className="text-xs text-zinc-600">
                  {formatNumber(dataQuality.with_description)} records
                </p>
              </div>
            </div>
            <div className="flex items-start gap-2">
              <BookOpen className="w-4 h-4 text-orange-400 mt-0.5" />
              <div>
                <p className="text-xs text-zinc-500">Unique Series</p>
                <p className="text-sm font-semibold text-white">
                  {formatNumber(dataQuality.unique_series)}
                </p>
              </div>
            </div>
            <div className="flex items-start gap-2">
              <Building2 className="w-4 h-4 text-yellow-400 mt-0.5" />
              <div>
                <p className="text-xs text-zinc-500">Publishers</p>
                <p className="text-sm font-semibold text-white">
                  {formatNumber(dataQuality.unique_publishers)}
                </p>
              </div>
            </div>
          </div>

          {/* Additional quality metrics */}
          <div className="mt-3 pt-3 border-t border-purple-500/20 grid grid-cols-2 md:grid-cols-3 gap-3 text-xs">
            <div className="flex justify-between">
              <span className="text-zinc-500">With ISBN:</span>
              <span className="text-zinc-400">{dataQuality.pct_with_isbn}% ({formatNumber(dataQuality.with_isbn)})</span>
            </div>
            <div className="flex justify-between">
              <span className="text-zinc-500">With Cover Date:</span>
              <span className="text-zinc-400">{formatNumber(dataQuality.with_cover_date)}</span>
            </div>
            <div className="flex justify-between">
              <span className="text-zinc-500">With Pricing:</span>
              <span className="text-zinc-400">{dataQuality.pct_with_pricing}% ({formatNumber(dataQuality.with_pricing)})</span>
            </div>
          </div>
        </div>
      )}

      {/* Error Display */}
      {checkpoint.last_error && (
        <div className="bg-red-500/10 border border-red-500/20 rounded-lg p-3">
          <div className="flex items-start gap-2">
            <XCircle className="w-4 h-4 text-red-400 mt-0.5 shrink-0" />
            <div>
              <p className="text-sm font-medium text-red-400 mb-1">Last Error</p>
              <p className="text-xs text-red-400/80 font-mono break-all">
                {checkpoint.last_error.substring(0, 200)}
                {checkpoint.last_error.length > 200 && '...'}
              </p>
            </div>
          </div>
        </div>
      )}

      {/* GCD Dump Info */}
      <div className="pt-3 border-t border-zinc-800">
        <div className="flex items-center justify-between text-xs text-zinc-500">
          <span>GCD Dump: {settings.dump_exists ? 'Available' : 'Not Found'}</span>
          <span>Total Records: {formatNumber(totalInDump)}</span>
        </div>
      </div>
    </div>
  );
}
