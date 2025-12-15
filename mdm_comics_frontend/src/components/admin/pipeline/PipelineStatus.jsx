/**
 * PipelineStatus - Multi-Pipeline Progress Tracker v1.12.0
 * Displays real-time progress of ALL data pipelines:
 * - GCD Import (Grand Comics Database)
 * - PriceCharting Matching (UPC/ISBN -> PriceCharting ID)
 *
 * Features:
 * - Live Import Stats (rate, ETA, current batch)
 * - Data Quality Summary (metadata completeness, cover images, etc.)
 * - Pipeline controls (start, pause, reset)
 */
import React, { useState, useEffect, useCallback, useRef } from 'react';
import {
  Database, RefreshCw, Loader2, CheckCircle, XCircle,
  AlertTriangle, Play, Pause, RotateCcw, Clock, Zap,
  Image, FileText, BookOpen, Building2, BarChart3,
  DollarSign, Link2, TrendingUp, Layers, Square
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
  const [gcdStatus, setGcdStatus] = useState(null);
  const [pcStatus, setPcStatus] = useState(null);
  const [mseStatus, setMseStatus] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [autoRefresh, setAutoRefresh] = useState(true);
  const [actionLoading, setActionLoading] = useState(false);
  const [activeTab, setActiveTab] = useState('gcd'); // 'gcd', 'pricecharting', or 'mse'

  // Option 1: Rate tracking state with exponential moving average (EMA)
  const [importRate, setImportRate] = useState(0);
  const lastCountRef = useRef({ count: 0, timestamp: Date.now() });
  const EMA_ALPHA = 0.3; // Smoothing factor (0.3 = 30% new, 70% old)

  const fetchStatus = useCallback(async () => {
    try {
      // Fetch all pipeline statuses in parallel
      const [gcdData, pcData, mseData] = await Promise.all([
        adminAPI.getGCDStatus().catch(e => null),
        adminAPI.getPriceChartingStatus().catch(e => null),
        adminAPI.getSequentialEnrichmentStatus().catch(e => null),
      ]);

      const data = gcdData; // For backwards compatibility with rate calculation

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

      lastCountRef.current = { count: data?.imported_count || 0, timestamp: now };

      setGcdStatus(gcdData);
      setPcStatus(pcData);
      setMseStatus(mseData);
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

  // Auto-refresh every 5 seconds when ANY pipeline is running
  useEffect(() => {
    const anyRunning = gcdStatus?.checkpoint?.is_running || pcStatus?.checkpoint?.is_running || mseStatus?.checkpoint?.is_running;
    if (!autoRefresh || !anyRunning) return;

    const interval = setInterval(fetchStatus, 5000);
    return () => clearInterval(interval);
  }, [autoRefresh, gcdStatus?.checkpoint?.is_running, pcStatus?.checkpoint?.is_running, mseStatus?.checkpoint?.is_running, fetchStatus]);

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

  const handleTriggerPriceCharting = async () => {
    setActionLoading(true);
    try {
      await adminAPI.triggerPriceChartingMatch({ batch_size: 500, max_records: 0 });
      await fetchStatus();
    } catch (err) {
      setError(err.message);
    } finally {
      setActionLoading(false);
    }
  };

  const handleTriggerMSE = async () => {
    setActionLoading(true);
    try {
      await adminAPI.triggerSequentialEnrichment({ batch_size: 100, max_records: 0 });
      await fetchStatus();
    } catch (err) {
      setError(err.message);
    } finally {
      setActionLoading(false);
    }
  };

  // v1.20.0: Job control handlers
  const handlePauseMSE = async () => {
    setActionLoading(true);
    try {
      await adminAPI.pauseJob('sequential_enrichment');
      await fetchStatus();
    } catch (err) {
      setError(err.message);
    } finally {
      setActionLoading(false);
    }
  };

  const handleStopMSE = async () => {
    setActionLoading(true);
    try {
      await adminAPI.stopJob('sequential_enrichment');
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

  if (error && !gcdStatus && !pcStatus) {
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

  // GCD Status extraction
  const gcdCheckpoint = gcdStatus?.checkpoint || {};
  const gcdSettings = gcdStatus?.settings || {};
  const dataQuality = gcdStatus?.data_quality || null;
  const gcdIsRunning = gcdCheckpoint.is_running;
  const totalInDump = gcdSettings.dump_total_count || 0;
  const importedCount = gcdStatus?.imported_count || 0;
  const gcdProgress = totalInDump > 0 ? (importedCount / totalInDump) * 100 : 0;
  const gcdRemaining = totalInDump - importedCount;
  const gcdEta = importRate > 0 ? gcdRemaining / importRate : null;

  // PriceCharting Status extraction
  const pcCheckpoint = pcStatus?.checkpoint || {};
  const pcStats = pcStatus?.matching_stats || {};
  const pcIsRunning = pcCheckpoint.is_running;
  const pcComicsMatched = pcStats.comics_matched || 0;
  const pcComicsTotal = pcStats.comics_total || 0;
  const pcFunkosMatched = pcStats.funkos_matched || 0;
  const pcFunkosTotal = pcStats.funkos_total || 0;
  const pcProgress = pcComicsTotal > 0 ? (pcComicsMatched / pcComicsTotal) * 100 : 0;
  const pcStateData = pcCheckpoint.state_data || {};
  const pcPhase = pcStateData.phase || 'unknown';
  const pcComicLastId = pcStateData.comic_last_id || 0;

  // Calculate time spent for PriceCharting
  const pcStarted = pcCheckpoint.last_run_started ? new Date(pcCheckpoint.last_run_started) : null;
  const pcTimeSpent = pcStarted && pcIsRunning ? Math.floor((Date.now() - pcStarted.getTime()) / 1000) : null;

  // Estimate remaining time based on progress rate
  const pcEstRate = pcTimeSpent && pcComicLastId > 0 ? pcComicLastId / pcTimeSpent : 0;
  const pcRemainingRecords = pcComicsTotal - pcComicLastId;
  const pcEta = pcEstRate > 0 ? pcRemainingRecords / pcEstRate : null;

  // Sequential Enrichment (MSE) Status extraction
  const mseCheckpoint = mseStatus?.checkpoint || {};
  const mseCoverage = mseStatus?.coverage || {};
  const mseSources = mseStatus?.sources || [];
  const mseAlgorithm = mseStatus?.algorithm || 'sequential_exhaustive';
  const mseIsRunning = mseCheckpoint.is_running;
  const mseControlSignal = mseCheckpoint.control_signal || 'run';  // v1.20.0
  const mseIsPaused = mseControlSignal === 'pause' && !mseIsRunning;
  const mseIsStopped = mseControlSignal === 'stop' && !mseIsRunning;
  const mseProcessed = mseCheckpoint.total_processed || 0;
  const mseUpdated = mseCheckpoint.total_updated || 0;
  const mseErrors = mseCheckpoint.total_errors || 0;
  const mseTotalComics = mseCoverage.total_comics || 0;
  const mseWithMetron = mseCoverage.with_metron || 0;
  const mseWithComicvine = mseCoverage.with_comicvine || 0;
  const mseWithPricecharting = mseCoverage.with_pricecharting || 0;
  const mseWithDescription = mseCoverage.with_description || 0;
  const mseWithUpc = mseCoverage.with_upc || 0;
  const mseWithIsbn = mseCoverage.with_isbn || 0;
  const mseWithImage = mseCoverage.with_image || 0;
  const mseWithMarketMetrics = mseCoverage.with_market_metrics || 0;
  // Calculate percentages locally for better precision display
  const calcPct = (count) => mseTotalComics > 0 ? (count / mseTotalComics * 100) : 0;

  // Check which jobs are running
  const anyRunning = gcdIsRunning || pcIsRunning || mseIsRunning;

  // Compact view for dashboard - shows BOTH pipelines
  if (compact) {
    return (
      <div className="bg-zinc-900 border border-zinc-800 rounded-xl p-5 space-y-4">
        {/* GCD Import Section */}
        <div>
          <div className="flex items-center justify-between mb-2">
            <div className="flex items-center gap-2">
              <Database className="w-4 h-4 text-blue-400" />
              <h3 className="text-sm font-semibold text-zinc-400">GCD Import</h3>
            </div>
            <div className="flex items-center gap-2">
              {gcdIsRunning ? (
                <span className="flex items-center gap-1 text-xs px-2 py-1 bg-blue-500/20 text-blue-400 rounded-full">
                  <Loader2 className="w-3 h-3 animate-spin" />
                  Running
                </span>
              ) : gcdProgress >= 100 ? (
                <span className="flex items-center gap-1 text-xs px-2 py-1 bg-green-500/20 text-green-400 rounded-full">
                  <CheckCircle className="w-3 h-3" />
                  Complete
                </span>
              ) : (
                <span className="flex items-center gap-1 text-xs px-2 py-1 bg-zinc-700 text-zinc-400 rounded-full">
                  <Pause className="w-3 h-3" />
                  Idle
                </span>
              )}
            </div>
          </div>
          <div className="h-2 bg-zinc-800 rounded-full overflow-hidden mb-1">
            <div
              className={`h-full transition-all duration-500 ${
                gcdIsRunning ? 'bg-blue-500' : gcdProgress >= 100 ? 'bg-green-500' : 'bg-orange-500'
              }`}
              style={{ width: `${Math.min(gcdProgress, 100)}%` }}
            />
          </div>
          <div className="flex items-center justify-between text-xs">
            <span className="text-zinc-500">{formatNumber(importedCount)} / {formatNumber(totalInDump)}</span>
            <span className="text-zinc-400">{formatPercent(gcdProgress)}%</span>
          </div>
        </div>

        {/* PriceCharting Matching Section */}
        <div className="pt-3 border-t border-zinc-800">
          <div className="flex items-center justify-between mb-2">
            <div className="flex items-center gap-2">
              <DollarSign className="w-4 h-4 text-green-400" />
              <h3 className="text-sm font-semibold text-zinc-400">PriceCharting Match</h3>
            </div>
            <div className="flex items-center gap-2">
              {pcIsRunning ? (
                <span className="flex items-center gap-1 text-xs px-2 py-1 bg-green-500/20 text-green-400 rounded-full">
                  <Loader2 className="w-3 h-3 animate-spin" />
                  Running
                </span>
              ) : pcComicsMatched > 0 ? (
                <span className="flex items-center gap-1 text-xs px-2 py-1 bg-green-500/20 text-green-400 rounded-full">
                  <Link2 className="w-3 h-3" />
                  {formatNumber(pcComicsMatched)} matched
                </span>
              ) : (
                <span className="flex items-center gap-1 text-xs px-2 py-1 bg-zinc-700 text-zinc-400 rounded-full">
                  <Pause className="w-3 h-3" />
                  Idle
                </span>
              )}
            </div>
          </div>

          {/* Progress info */}
          {pcIsRunning && (
            <div className="space-y-2">
              <div className="h-2 bg-zinc-800 rounded-full overflow-hidden">
                <div
                  className="h-full bg-green-500 transition-all duration-500"
                  style={{ width: `${Math.min((pcComicLastId / pcComicsTotal) * 100, 100)}%` }}
                />
              </div>
              <div className="grid grid-cols-2 gap-2 text-xs">
                <div>
                  <span className="text-zinc-500">Processing:</span>
                  <span className="text-zinc-300 ml-1">ID {formatNumber(pcComicLastId)} / {formatNumber(pcComicsTotal)}</span>
                </div>
                <div>
                  <span className="text-zinc-500">Matched:</span>
                  <span className="text-green-400 ml-1">{formatNumber(pcComicsMatched)}</span>
                </div>
              </div>
              {pcTimeSpent && (
                <div className="flex items-center gap-3 text-xs text-zinc-500">
                  <span className="flex items-center gap-1">
                    <Clock className="w-3 h-3 text-blue-400" />
                    Time: {formatDuration(pcTimeSpent)}
                  </span>
                  {pcEta && (
                    <span className="flex items-center gap-1">
                      <TrendingUp className="w-3 h-3 text-yellow-400" />
                      ETA: {formatDuration(pcEta)}
                    </span>
                  )}
                </div>
              )}
            </div>
          )}

          {!pcIsRunning && pcComicsMatched > 0 && (
            <div className="text-xs text-zinc-500">
              Comics: {formatNumber(pcComicsMatched)} matched | Funkos: {formatNumber(pcFunkosMatched)} matched
            </div>
          )}
        </div>

        {/* Auto-refresh indicator */}
        <div className="flex items-center justify-between text-xs text-zinc-600 pt-2 border-t border-zinc-800">
          <span>{anyRunning ? 'Auto-refreshing...' : 'Idle'}</span>
          <button
            onClick={fetchStatus}
            className="p-1 hover:bg-zinc-800 rounded transition-colors"
          >
            <RefreshCw className={`w-3 h-3 ${anyRunning ? 'animate-spin text-blue-400' : 'text-zinc-500'}`} />
          </button>
        </div>
      </div>
    );
  }

  // Full view with tabs for GCD and PriceCharting
  return (
    <div className="bg-zinc-900 border border-zinc-800 rounded-xl p-5 space-y-4">
      {/* Header with Tabs */}
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-2">
          <button
            onClick={() => setActiveTab('gcd')}
            className={`flex items-center gap-2 px-4 py-2 rounded-lg transition-colors ${
              activeTab === 'gcd'
                ? 'bg-blue-500/20 text-blue-400 border border-blue-500/30'
                : 'bg-zinc-800 text-zinc-400 hover:bg-zinc-700'
            }`}
          >
            <Database className="w-4 h-4" />
            <span className="text-sm font-medium">GCD Import</span>
            {gcdIsRunning && <Loader2 className="w-3 h-3 animate-spin" />}
          </button>
          <button
            onClick={() => setActiveTab('pricecharting')}
            className={`flex items-center gap-2 px-4 py-2 rounded-lg transition-colors ${
              activeTab === 'pricecharting'
                ? 'bg-green-500/20 text-green-400 border border-green-500/30'
                : 'bg-zinc-800 text-zinc-400 hover:bg-zinc-700'
            }`}
          >
            <DollarSign className="w-4 h-4" />
            <span className="text-sm font-medium">PriceCharting</span>
            {pcIsRunning && <Loader2 className="w-3 h-3 animate-spin" />}
          </button>
          <button
            onClick={() => setActiveTab('mse')}
            className={`flex items-center gap-2 px-4 py-2 rounded-lg transition-colors ${
              activeTab === 'mse'
                ? 'bg-purple-500/20 text-purple-400 border border-purple-500/30'
                : 'bg-zinc-800 text-zinc-400 hover:bg-zinc-700'
            }`}
          >
            <Layers className="w-4 h-4" />
            <span className="text-sm font-medium">Sequential Enrichment</span>
            {mseIsRunning && <Loader2 className="w-3 h-3 animate-spin" />}
          </button>
        </div>
        <button
          onClick={() => setAutoRefresh(!autoRefresh)}
          className={`p-2 rounded-lg transition-colors ${
            autoRefresh ? 'bg-blue-500/20 text-blue-400' : 'bg-zinc-800 text-zinc-500'
          }`}
          title={autoRefresh ? 'Auto-refresh ON' : 'Auto-refresh OFF'}
        >
          <RefreshCw className={`w-4 h-4 ${autoRefresh && anyRunning ? 'animate-spin' : ''}`} />
        </button>
      </div>

      {/* ==================== GCD TAB ==================== */}
      {activeTab === 'gcd' && (
        <>
          {/* Status Badge */}
          <div className="flex items-center gap-3">
            {gcdIsRunning ? (
              <span className="flex items-center gap-2 px-3 py-1.5 bg-blue-500/20 text-blue-400 rounded-lg text-sm">
                <Loader2 className="w-4 h-4 animate-spin" />
                Import Running
              </span>
            ) : gcdProgress >= 100 ? (
              <span className="flex items-center gap-2 px-3 py-1.5 bg-green-500/20 text-green-400 rounded-lg text-sm">
                <CheckCircle className="w-4 h-4" />
                Import Complete
              </span>
            ) : (
              <span className="flex items-center gap-2 px-3 py-1.5 bg-zinc-800 text-zinc-400 rounded-lg text-sm">
                <Pause className="w-4 h-4" />
                Idle
              </span>
            )}

            {!gcdIsRunning && gcdProgress < 100 && (
              <button
                onClick={handleTriggerImport}
                disabled={actionLoading}
                className="flex items-center gap-2 px-3 py-1.5 bg-green-500/20 text-green-400 rounded-lg text-sm hover:bg-green-500/30 transition-colors disabled:opacity-50"
              >
                {actionLoading ? <Loader2 className="w-4 h-4 animate-spin" /> : <Play className="w-4 h-4" />}
                Resume Import
              </button>
            )}

            {gcdCheckpoint.last_error && (
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
              <span className="text-sm font-medium text-white">{formatPercent(gcdProgress)}%</span>
            </div>
            <div className="h-3 bg-zinc-800 rounded-full overflow-hidden">
              <div
                className={`h-full transition-all duration-500 ${
                  gcdIsRunning ? 'bg-blue-500' : gcdProgress >= 100 ? 'bg-green-500' : 'bg-orange-500'
                }`}
                style={{ width: `${Math.min(gcdProgress, 100)}%` }}
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
              <p className="text-lg font-bold text-white">{formatNumber(gcdRemaining)}</p>
            </div>
            <div className="bg-zinc-800/50 rounded-lg p-3">
              <p className="text-xs text-zinc-500 mb-1">Processed</p>
              <p className="text-lg font-bold text-white">{formatNumber(gcdCheckpoint.total_processed)}</p>
            </div>
            <div className="bg-zinc-800/50 rounded-lg p-3">
              <p className="text-xs text-zinc-500 mb-1">Errors</p>
              <p className={`text-lg font-bold ${gcdCheckpoint.total_errors > 0 ? 'text-red-400' : 'text-white'}`}>
                {formatNumber(gcdCheckpoint.total_errors)}
              </p>
            </div>
          </div>

          {/* Live Import Stats (when running) */}
          {gcdIsRunning && (
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
                    {gcdEta ? formatDuration(gcdEta) : 'Calculating...'}
                  </p>
                </div>
                <div>
                  <p className="text-xs text-zinc-500 mb-1">Batch Size</p>
                  <p className="text-lg font-bold text-white">{formatNumber(gcdSettings.batch_size || 5000)}</p>
                </div>
              </div>
              {gcdCheckpoint.last_run_started && (
                <p className="text-xs text-zinc-500 mt-3">
                  Started: {new Date(gcdCheckpoint.last_run_started).toLocaleString()}
                </p>
              )}
            </div>
          )}

          {/* Data Quality Summary */}
          {dataQuality && !gcdIsRunning && (
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
          {gcdCheckpoint.last_error && (
            <div className="bg-red-500/10 border border-red-500/20 rounded-lg p-3">
              <div className="flex items-start gap-2">
                <XCircle className="w-4 h-4 text-red-400 mt-0.5 shrink-0" />
                <div>
                  <p className="text-sm font-medium text-red-400 mb-1">Last Error</p>
                  <p className="text-xs text-red-400/80 font-mono break-all">
                    {gcdCheckpoint.last_error.substring(0, 200)}
                    {gcdCheckpoint.last_error.length > 200 && '...'}
                  </p>
                </div>
              </div>
            </div>
          )}

          {/* GCD Dump Info */}
          <div className="pt-3 border-t border-zinc-800">
            <div className="flex items-center justify-between text-xs text-zinc-500">
              <span>GCD Dump: {gcdSettings.dump_exists ? 'Available' : 'Not Found'}</span>
              <span>Total Records: {formatNumber(totalInDump)}</span>
            </div>
          </div>
        </>
      )}

      {/* ==================== PRICECHARTING TAB ==================== */}
      {activeTab === 'pricecharting' && (
        <>
          {/* Status Badge */}
          <div className="flex items-center gap-3">
            {pcIsRunning ? (
              <span className="flex items-center gap-2 px-3 py-1.5 bg-green-500/20 text-green-400 rounded-lg text-sm">
                <Loader2 className="w-4 h-4 animate-spin" />
                Matching Running
              </span>
            ) : pcComicsMatched > 0 ? (
              <span className="flex items-center gap-2 px-3 py-1.5 bg-green-500/20 text-green-400 rounded-lg text-sm">
                <Link2 className="w-4 h-4" />
                {formatNumber(pcComicsMatched)} Comics Matched
              </span>
            ) : (
              <span className="flex items-center gap-2 px-3 py-1.5 bg-zinc-800 text-zinc-400 rounded-lg text-sm">
                <Pause className="w-4 h-4" />
                Idle
              </span>
            )}

            {!pcIsRunning && (
              <button
                onClick={handleTriggerPriceCharting}
                disabled={actionLoading}
                className="flex items-center gap-2 px-3 py-1.5 bg-green-500/20 text-green-400 rounded-lg text-sm hover:bg-green-500/30 transition-colors disabled:opacity-50"
              >
                {actionLoading ? <Loader2 className="w-4 h-4 animate-spin" /> : <Play className="w-4 h-4" />}
                {pcComicsMatched > 0 ? 'Continue Matching' : 'Start Matching'}
              </button>
            )}
          </div>

          {/* Progress Bar */}
          <div>
            <div className="flex items-center justify-between mb-2">
              <span className="text-sm text-zinc-400">Matching Progress</span>
              <span className="text-sm font-medium text-white">
                {pcComicsTotal > 0 ? formatPercent((pcComicLastId / pcComicsTotal) * 100) : '0.00'}%
              </span>
            </div>
            <div className="h-3 bg-zinc-800 rounded-full overflow-hidden">
              <div
                className={`h-full transition-all duration-500 ${pcIsRunning ? 'bg-green-500' : 'bg-green-600'}`}
                style={{ width: `${pcComicsTotal > 0 ? Math.min((pcComicLastId / pcComicsTotal) * 100, 100) : 0}%` }}
              />
            </div>
          </div>

          {/* Stats Grid */}
          <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
            <div className="bg-zinc-800/50 rounded-lg p-3">
              <p className="text-xs text-zinc-500 mb-1">Comics Matched</p>
              <p className="text-lg font-bold text-green-400">{formatNumber(pcComicsMatched)}</p>
            </div>
            <div className="bg-zinc-800/50 rounded-lg p-3">
              <p className="text-xs text-zinc-500 mb-1">Total Comics</p>
              <p className="text-lg font-bold text-white">{formatNumber(pcComicsTotal)}</p>
            </div>
            <div className="bg-zinc-800/50 rounded-lg p-3">
              <p className="text-xs text-zinc-500 mb-1">Funkos Matched</p>
              <p className="text-lg font-bold text-green-400">{formatNumber(pcFunkosMatched)}</p>
            </div>
            <div className="bg-zinc-800/50 rounded-lg p-3">
              <p className="text-xs text-zinc-500 mb-1">Current Position</p>
              <p className="text-lg font-bold text-white">ID {formatNumber(pcComicLastId)}</p>
            </div>
          </div>

          {/* Live Matching Stats (when running) */}
          {pcIsRunning && (
            <div className="bg-green-500/10 border border-green-500/20 rounded-lg p-4">
              <div className="flex items-center gap-2 mb-3">
                <Zap className="w-4 h-4 text-yellow-400" />
                <h4 className="text-sm font-medium text-green-400">Live Matching Stats</h4>
              </div>
              <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
                <div>
                  <p className="text-xs text-zinc-500 mb-1 flex items-center gap-1">
                    <Clock className="w-3 h-3 text-blue-400" />
                    Time Spent
                  </p>
                  <p className="text-lg font-bold text-white">
                    {pcTimeSpent ? formatDuration(pcTimeSpent) : '--'}
                  </p>
                </div>
                <div>
                  <p className="text-xs text-zinc-500 mb-1 flex items-center gap-1">
                    <TrendingUp className="w-3 h-3 text-yellow-400" />
                    Est. Remaining
                  </p>
                  <p className="text-lg font-bold text-white">
                    {pcEta ? formatDuration(pcEta) : 'Calculating...'}
                  </p>
                </div>
                <div>
                  <p className="text-xs text-zinc-500 mb-1">Match Rate</p>
                  <p className="text-lg font-bold text-white">
                    {pcEstRate > 0 ? formatRate(pcEstRate) : '--'}
                  </p>
                </div>
                <div>
                  <p className="text-xs text-zinc-500 mb-1">Current Phase</p>
                  <p className="text-lg font-bold text-white capitalize">{pcPhase}</p>
                </div>
              </div>
              {pcStarted && (
                <p className="text-xs text-zinc-500 mt-3">
                  Started: {pcStarted.toLocaleString()}
                </p>
              )}
            </div>
          )}

          {/* Match Summary (when not running and has matches) */}
          {!pcIsRunning && pcComicsMatched > 0 && (
            <div className="bg-green-500/10 border border-green-500/20 rounded-lg p-4">
              <div className="flex items-center gap-2 mb-3">
                <BarChart3 className="w-4 h-4 text-green-400" />
                <h4 className="text-sm font-medium text-green-400">Match Summary</h4>
              </div>
              <div className="grid grid-cols-2 gap-4 text-sm">
                <div className="flex justify-between">
                  <span className="text-zinc-500">Comics Match Rate:</span>
                  <span className="text-green-400 font-medium">
                    {pcComicsTotal > 0 ? formatPercent((pcComicsMatched / pcComicsTotal) * 100) : '0'}%
                  </span>
                </div>
                <div className="flex justify-between">
                  <span className="text-zinc-500">Funkos Match Rate:</span>
                  <span className="text-green-400 font-medium">
                    {pcFunkosTotal > 0 ? formatPercent((pcFunkosMatched / pcFunkosTotal) * 100) : '0'}%
                  </span>
                </div>
                <div className="flex justify-between">
                  <span className="text-zinc-500">Total Processed:</span>
                  <span className="text-zinc-300">{formatNumber(pcCheckpoint.total_processed || 0)}</span>
                </div>
                <div className="flex justify-between">
                  <span className="text-zinc-500">Errors:</span>
                  <span className={pcCheckpoint.total_errors > 0 ? 'text-red-400' : 'text-zinc-300'}>
                    {formatNumber(pcCheckpoint.total_errors || 0)}
                  </span>
                </div>
              </div>
            </div>
          )}

          {/* Error Display */}
          {pcCheckpoint.last_error && (
            <div className="bg-red-500/10 border border-red-500/20 rounded-lg p-3">
              <div className="flex items-start gap-2">
                <XCircle className="w-4 h-4 text-red-400 mt-0.5 shrink-0" />
                <div>
                  <p className="text-sm font-medium text-red-400 mb-1">Last Error</p>
                  <p className="text-xs text-red-400/80 font-mono break-all">
                    {pcCheckpoint.last_error.substring(0, 200)}
                    {pcCheckpoint.last_error.length > 200 && '...'}
                  </p>
                </div>
              </div>
            </div>
          )}

          {/* API Info */}
          <div className="pt-3 border-t border-zinc-800">
            <div className="flex items-center justify-between text-xs text-zinc-500">
              <span>Matching via PriceCharting API (UPC/ISBN lookup)</span>
              <span>Batch Size: 500</span>
            </div>
          </div>
        </>
      )}

      {/* ==================== SEQUENTIAL ENRICHMENT (MSE) TAB ==================== */}
      {activeTab === 'mse' && (
        <>
          {/* Status Badge + Algorithm */}
          <div className="flex items-center justify-between flex-wrap gap-3">
            <div className="flex items-center gap-3">
              {mseIsRunning ? (
                <span className="flex items-center gap-2 px-3 py-1.5 bg-purple-500/20 text-purple-400 rounded-lg text-sm">
                  <Loader2 className="w-4 h-4 animate-spin" />
                  Sequential Enrichment Running
                </span>
              ) : mseIsPaused ? (
                <span className="flex items-center gap-2 px-3 py-1.5 bg-yellow-500/20 text-yellow-400 rounded-lg text-sm">
                  <Pause className="w-4 h-4" />
                  Paused (will auto-resume on cron)
                </span>
              ) : mseIsStopped ? (
                <span className="flex items-center gap-2 px-3 py-1.5 bg-red-500/20 text-red-400 rounded-lg text-sm">
                  <Square className="w-4 h-4" />
                  Stopped
                </span>
              ) : mseProcessed > 0 ? (
                <span className="flex items-center gap-2 px-3 py-1.5 bg-green-500/20 text-green-400 rounded-lg text-sm">
                  <CheckCircle className="w-4 h-4" />
                  {formatNumber(mseProcessed)} Processed
                </span>
              ) : (
                <span className="flex items-center gap-2 px-3 py-1.5 bg-zinc-800 text-zinc-400 rounded-lg text-sm">
                  <Pause className="w-4 h-4" />
                  Idle
                </span>
              )}

              {!mseIsRunning && (
                <button
                  onClick={handleTriggerMSE}
                  disabled={actionLoading}
                  className="flex items-center gap-2 px-3 py-1.5 bg-purple-500/20 text-purple-400 rounded-lg text-sm hover:bg-purple-500/30 transition-colors disabled:opacity-50"
                >
                  {actionLoading ? <Loader2 className="w-4 h-4 animate-spin" /> : <Play className="w-4 h-4" />}
                  {mseIsPaused || mseIsStopped ? 'Resume' : mseProcessed > 0 ? 'Continue' : 'Start'}
                </button>
              )}

              {/* v1.20.0: Pause/Stop controls when running */}
              {mseIsRunning && (
                <>
                  <button
                    onClick={handlePauseMSE}
                    disabled={actionLoading}
                    className="flex items-center gap-2 px-3 py-1.5 bg-yellow-500/20 text-yellow-400 rounded-lg text-sm hover:bg-yellow-500/30 transition-colors disabled:opacity-50"
                    title="Pause - saves checkpoint, cron will auto-resume"
                  >
                    {actionLoading ? <Loader2 className="w-4 h-4 animate-spin" /> : <Pause className="w-4 h-4" />}
                    Pause
                  </button>
                  <button
                    onClick={handleStopMSE}
                    disabled={actionLoading}
                    className="flex items-center gap-2 px-3 py-1.5 bg-red-500/20 text-red-400 rounded-lg text-sm hover:bg-red-500/30 transition-colors disabled:opacity-50"
                    title="Stop - saves checkpoint, releases lock"
                  >
                    {actionLoading ? <Loader2 className="w-4 h-4 animate-spin" /> : <Square className="w-4 h-4" />}
                    Stop
                  </button>
                </>
              )}
            </div>
            <span className="text-xs text-zinc-500 font-mono">{mseAlgorithm}</span>
          </div>

          {/* Identifier Coverage - UPC & ISBN (most important for matching) */}
          <div className="bg-zinc-800/30 rounded-lg p-4">
            <h4 className="text-sm font-medium text-zinc-300 mb-3">Identifier Coverage</h4>
            <div className="space-y-3">
              <div>
                <div className="flex items-center justify-between mb-1">
                  <span className="text-sm text-zinc-400">UPC Barcodes</span>
                  <span className="text-sm">
                    <span className="font-medium text-orange-400">{formatNumber(mseWithUpc)}</span>
                    <span className="text-zinc-500"> / {formatNumber(mseTotalComics)}</span>
                    <span className="text-zinc-500 ml-2">({calcPct(mseWithUpc).toFixed(1)}%)</span>
                  </span>
                </div>
                <div className="h-2 bg-zinc-800 rounded-full overflow-hidden">
                  <div className="h-full bg-orange-500" style={{ width: `${Math.min(calcPct(mseWithUpc), 100)}%` }} />
                </div>
              </div>
              <div>
                <div className="flex items-center justify-between mb-1">
                  <span className="text-sm text-zinc-400">ISBN Codes</span>
                  <span className="text-sm">
                    <span className="font-medium text-cyan-400">{formatNumber(mseWithIsbn)}</span>
                    <span className="text-zinc-500"> / {formatNumber(mseTotalComics)}</span>
                    <span className="text-zinc-500 ml-2">({calcPct(mseWithIsbn).toFixed(1)}%)</span>
                  </span>
                </div>
                <div className="h-2 bg-zinc-800 rounded-full overflow-hidden">
                  <div className="h-full bg-cyan-500" style={{ width: `${Math.min(calcPct(mseWithIsbn), 100)}%` }} />
                </div>
              </div>
            </div>
          </div>

          {/* Content Coverage - Description & Image */}
          <div className="bg-zinc-800/30 rounded-lg p-4">
            <h4 className="text-sm font-medium text-zinc-300 mb-3">Content Coverage</h4>
            <div className="space-y-3">
              <div>
                <div className="flex items-center justify-between mb-1">
                  <span className="text-sm text-zinc-400">Description</span>
                  <span className="text-sm">
                    <span className="font-medium text-purple-400">{formatNumber(mseWithDescription)}</span>
                    <span className="text-zinc-500"> / {formatNumber(mseTotalComics)}</span>
                    <span className="text-zinc-500 ml-2">({calcPct(mseWithDescription).toFixed(1)}%)</span>
                  </span>
                </div>
                <div className="h-2 bg-zinc-800 rounded-full overflow-hidden">
                  <div className="h-full bg-purple-500" style={{ width: `${Math.min(calcPct(mseWithDescription), 100)}%` }} />
                </div>
              </div>
              <div>
                <div className="flex items-center justify-between mb-1">
                  <span className="text-sm text-zinc-400">Cover Image</span>
                  <span className="text-sm">
                    <span className="font-medium text-pink-400">{formatNumber(mseWithImage)}</span>
                    <span className="text-zinc-500"> / {formatNumber(mseTotalComics)}</span>
                    <span className="text-zinc-500 ml-2">({calcPct(mseWithImage).toFixed(1)}%)</span>
                  </span>
                </div>
                <div className="h-2 bg-zinc-800 rounded-full overflow-hidden">
                  <div className="h-full bg-pink-500" style={{ width: `${Math.min(calcPct(mseWithImage), 100)}%` }} />
                </div>
              </div>
            </div>
          </div>

          {/* Source ID Coverage */}
          <div className="bg-zinc-800/30 rounded-lg p-4">
            <h4 className="text-sm font-medium text-zinc-300 mb-3">Source ID Coverage</h4>
            <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
              <div className="flex items-center justify-between p-2 bg-zinc-800/50 rounded">
                <span className="text-sm text-zinc-400">Metron</span>
                <span className="text-sm font-medium text-blue-400">{formatNumber(mseWithMetron)}</span>
              </div>
              <div className="flex items-center justify-between p-2 bg-zinc-800/50 rounded">
                <span className="text-sm text-zinc-400">ComicVine</span>
                <span className="text-sm font-medium text-green-400">{formatNumber(mseWithComicvine)}</span>
              </div>
              <div className="flex items-center justify-between p-2 bg-zinc-800/50 rounded">
                <span className="text-sm text-zinc-400">PriceCharting</span>
                <span className="text-sm font-medium text-yellow-400">{formatNumber(mseWithPricecharting)}</span>
              </div>
              <div className="flex items-center justify-between p-2 bg-zinc-800/50 rounded">
                <span className="text-sm text-zinc-400">Description</span>
                <span className="text-sm font-medium text-purple-400">{formatNumber(mseWithDescription)}</span>
              </div>
            </div>
          </div>

          {/* Active Sources (dynamic from backend) */}
          {mseSources.length > 0 && (
            <div className="bg-purple-500/10 border border-purple-500/20 rounded-lg p-4">
              <div className="flex items-center gap-2 mb-3">
                <Layers className="w-4 h-4 text-purple-400" />
                <h4 className="text-sm font-medium text-purple-400">Active Enrichment Sources</h4>
              </div>
              <div className="flex flex-wrap gap-2">
                {mseSources.map(source => {
                  // Format source names with proper case and acronyms
                  const formatSource = (s) => {
                    const nameMap = {
                      'metron': 'Metron',
                      'comicvine': 'ComicVine',
                      'pricecharting': 'PriceCharting',
                      'comicbookrealm': 'CBR',
                      'marvel_fandom': 'Marvel Fandom',
                      'mycomicshop': 'MyComicShop',
                      'dc_fandom': 'DC Fandom',
                      'image_fandom': 'Image Fandom',
                      'idw_fandom': 'IDW Fandom',
                      'darkhorse_fandom': 'Dark Horse Fandom',
                      'dynamite_fandom': 'Dynamite Fandom',
                    };
                    return nameMap[s] || s.replace(/_/g, ' ').replace(/\b\w/g, c => c.toUpperCase());
                  };
                  return (
                    <span
                      key={source}
                      className="px-2 py-1 bg-purple-500/20 text-purple-300 rounded text-xs font-medium"
                    >
                      {formatSource(source)}
                    </span>
                  );
                })}
              </div>
            </div>
          )}

          {/* Enrichment Stats */}
          <div className="grid grid-cols-3 gap-3">
            <div className="bg-zinc-800/50 rounded-lg p-3 text-center">
              <p className="text-2xl font-bold text-white">{formatNumber(mseProcessed)}</p>
              <p className="text-xs text-zinc-500">Processed</p>
            </div>
            <div className="bg-zinc-800/50 rounded-lg p-3 text-center">
              <p className="text-2xl font-bold text-purple-400">{formatNumber(mseUpdated || 0)}</p>
              <p className="text-xs text-zinc-500">Enriched</p>
            </div>
            <div className="bg-zinc-800/50 rounded-lg p-3 text-center">
              <p className={`text-2xl font-bold ${mseErrors > 0 ? 'text-red-400' : 'text-zinc-400'}`}>
                {formatNumber(mseErrors)}
              </p>
              <p className="text-xs text-zinc-500">Errors</p>
            </div>
          </div>

          {/* Error Display */}
          {mseCheckpoint.last_error && (
            <div className="bg-red-500/10 border border-red-500/20 rounded-lg p-3">
              <div className="flex items-start gap-2">
                <XCircle className="w-4 h-4 text-red-400 mt-0.5 shrink-0" />
                <div>
                  <p className="text-sm font-medium text-red-400 mb-1">Last Error</p>
                  <p className="text-xs text-red-400/80 font-mono break-all">
                    {mseCheckpoint.last_error.substring(0, 200)}
                    {mseCheckpoint.last_error.length > 200 && '...'}
                  </p>
                </div>
              </div>
            </div>
          )}

          {/* Footer */}
          <div className="pt-3 border-t border-zinc-800">
            <div className="flex items-center justify-between text-xs text-zinc-500">
              <span>Total Comics: {formatNumber(mseTotalComics)}</span>
              <span>Batch Size: 100</span>
            </div>
          </div>
        </>
      )}
    </div>
  );
}
