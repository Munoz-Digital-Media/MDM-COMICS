/**
 * PipelineStatus - GCD Import Progress Tracker
 * Displays real-time progress of the GCD data import pipeline
 */
import React, { useState, useEffect, useCallback } from 'react';
import {
  Database, RefreshCw, Loader2, CheckCircle, XCircle,
  AlertTriangle, Play, Pause, RotateCcw
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

export default function PipelineStatus({ compact = false }) {
  const [status, setStatus] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [autoRefresh, setAutoRefresh] = useState(true);
  const [actionLoading, setActionLoading] = useState(false);

  const fetchStatus = useCallback(async () => {
    try {
      const data = await adminAPI.getGCDStatus();
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
  const isRunning = checkpoint.is_running;
  const totalInDump = settings.dump_total_count || 0;
  const importedCount = status?.imported_count || 0;
  const progress = totalInDump > 0 ? (importedCount / totalInDump) * 100 : 0;
  const remaining = totalInDump - importedCount;

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
