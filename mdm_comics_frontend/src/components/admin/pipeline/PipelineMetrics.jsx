/**
 * PipelineMetrics - Real-time Pipeline Instrumentation Dashboard v1.24.0
 *
 * Displays pipeline health metrics from the instrumentation system:
 * - Running batches with stall detection status
 * - API performance by source
 * - Pipeline throughput summary
 * - Recent stalls and self-heals
 */
import React, { useState, useEffect, useCallback } from 'react';
import {
  Activity, AlertTriangle, CheckCircle, Clock, RefreshCw,
  Loader2, Zap, Server, XCircle, TrendingUp, Heart,
  Timer, Database, BarChart3
} from 'lucide-react';
import { adminAPI } from '../../../services/adminApi';

function formatNumber(num) {
  if (num === null || num === undefined) return '0';
  return num.toLocaleString();
}

function formatDuration(ms) {
  if (!ms || ms <= 0) return '--';
  const seconds = Math.floor(ms / 1000);
  const minutes = Math.floor(seconds / 60);
  const hours = Math.floor(minutes / 60);

  if (hours > 0) return `${hours}h ${minutes % 60}m`;
  if (minutes > 0) return `${minutes}m ${seconds % 60}s`;
  return `${seconds}s`;
}

function formatMs(ms) {
  if (!ms || ms <= 0) return '--';
  if (ms >= 1000) return `${(ms / 1000).toFixed(1)}s`;
  return `${Math.round(ms)}ms`;
}

function timeAgo(dateString) {
  if (!dateString) return '--';
  const date = new Date(dateString);
  const seconds = Math.floor((Date.now() - date.getTime()) / 1000);

  if (seconds < 60) return `${seconds}s ago`;
  if (seconds < 3600) return `${Math.floor(seconds / 60)}m ago`;
  if (seconds < 86400) return `${Math.floor(seconds / 3600)}h ago`;
  return `${Math.floor(seconds / 86400)}d ago`;
}

function StatusBadge({ status }) {
  const config = {
    running: { bg: 'bg-blue-500/20', text: 'text-blue-400', icon: Loader2, animate: true },
    completed: { bg: 'bg-green-500/20', text: 'text-green-400', icon: CheckCircle },
    failed: { bg: 'bg-red-500/20', text: 'text-red-400', icon: XCircle },
    stalled: { bg: 'bg-yellow-500/20', text: 'text-yellow-400', icon: AlertTriangle },
    self_healed: { bg: 'bg-purple-500/20', text: 'text-purple-400', icon: Heart },
  };

  const { bg, text, icon: Icon, animate } = config[status] || config.running;

  return (
    <span className={`flex items-center gap-1 px-2 py-1 rounded-full text-xs ${bg} ${text}`}>
      <Icon className={`w-3 h-3 ${animate ? 'animate-spin' : ''}`} />
      {status}
    </span>
  );
}

export default function PipelineMetrics({ compact = false }) {
  const [metrics, setMetrics] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [autoRefresh, setAutoRefresh] = useState(true);

  const fetchMetrics = useCallback(async () => {
    try {
      const data = await adminAPI.getPipelineMetrics();
      setMetrics(data);
      setError(null);
    } catch (err) {
      console.error('Pipeline metrics fetch error:', err);
      setError(err.message);
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    fetchMetrics();
  }, [fetchMetrics]);

  // Auto-refresh every 10 seconds
  useEffect(() => {
    if (!autoRefresh) return;
    const interval = setInterval(fetchMetrics, 10000);
    return () => clearInterval(interval);
  }, [autoRefresh, fetchMetrics]);

  if (loading) {
    return (
      <div className="bg-zinc-900 border border-zinc-800 rounded-xl p-5">
        <div className="flex items-center gap-2 text-zinc-500">
          <Loader2 className="w-4 h-4 animate-spin" />
          <span className="text-sm">Loading pipeline metrics...</span>
        </div>
      </div>
    );
  }

  if (error && !metrics) {
    return (
      <div className="bg-zinc-900 border border-zinc-800 rounded-xl p-5">
        <div className="flex items-center justify-between">
          <div className="flex items-center gap-2 text-yellow-400">
            <AlertTriangle className="w-4 h-4" />
            <span className="text-sm">Metrics unavailable (tables may be empty)</span>
          </div>
          <button onClick={fetchMetrics} className="p-2 hover:bg-zinc-800 rounded-lg">
            <RefreshCw className="w-4 h-4 text-zinc-400" />
          </button>
        </div>
      </div>
    );
  }

  const runningBatches = metrics?.running_batches || [];
  const recentStalls = metrics?.recent_stalls || [];
  const recentSelfHeals = metrics?.recent_self_heals || [];
  const apiPerformance = metrics?.api_performance || [];
  const batchStats = metrics?.batch_stats || {};
  const pipelineSummary = metrics?.pipeline_summary || [];
  const thresholds = metrics?.thresholds || {};

  // Calculate overall health
  const hasRunningBatches = runningBatches.length > 0;
  const hasRecentStalls = recentStalls.length > 0;
  const hasFailures = (batchStats.failed?.count || 0) > 0;

  // Compact view for dashboard integration
  if (compact) {
    return (
      <div className="space-y-3">
        {/* Running Batches */}
        <div className="flex items-center justify-between">
          <span className="text-sm text-zinc-400">Running Batches</span>
          <span className={`text-xs px-2 py-1 rounded-full ${
            hasRunningBatches
              ? 'bg-blue-500/20 text-blue-400'
              : 'bg-zinc-700/50 text-zinc-400'
          }`}>
            {runningBatches.length} Active
          </span>
        </div>

        {/* Completed (24h) */}
        <div className="flex items-center justify-between">
          <span className="text-sm text-zinc-400">Completed (24h)</span>
          <span className="text-xs px-2 py-1 rounded-full bg-green-500/20 text-green-400">
            {batchStats.completed?.count || 0} Batches
          </span>
        </div>

        {/* Failed/Stalled */}
        <div className="flex items-center justify-between">
          <span className="text-sm text-zinc-400">Failed/Stalled</span>
          <span className={`text-xs px-2 py-1 rounded-full ${
            hasFailures || hasRecentStalls
              ? 'bg-red-500/20 text-red-400'
              : 'bg-green-500/20 text-green-400'
          }`}>
            {(batchStats.failed?.count || 0) + (batchStats.stalled?.count || 0)} Issues
          </span>
        </div>

        {/* Self-Healed */}
        {(batchStats.self_healed?.count || 0) > 0 && (
          <div className="flex items-center justify-between">
            <span className="text-sm text-zinc-400">Self-Healed</span>
            <span className="text-xs px-2 py-1 rounded-full bg-purple-500/20 text-purple-400">
              {batchStats.self_healed?.count || 0} Recovered
            </span>
          </div>
        )}

        {/* API Health Summary */}
        {apiPerformance.length > 0 && (
          <div className="flex items-center justify-between">
            <span className="text-sm text-zinc-400">API Success Rate</span>
            <span className={`text-xs px-2 py-1 rounded-full ${
              apiPerformance.every(a => a.success_rate >= 95)
                ? 'bg-green-500/20 text-green-400'
                : apiPerformance.some(a => a.success_rate < 80)
                  ? 'bg-red-500/20 text-red-400'
                  : 'bg-yellow-500/20 text-yellow-400'
            }`}>
              {apiPerformance.length > 0
                ? `${Math.round(apiPerformance.reduce((a, b) => a + b.success_rate, 0) / apiPerformance.length)}% avg`
                : 'N/A'}
            </span>
          </div>
        )}

        {/* Auto-refresh toggle */}
        <div className="flex items-center justify-between pt-2 border-t border-zinc-800">
          <span className="text-xs text-zinc-600">
            {autoRefresh ? 'Auto-refreshing...' : 'Paused'}
          </span>
          <button
            onClick={fetchMetrics}
            className="p-1 hover:bg-zinc-800 rounded"
          >
            <RefreshCw className={`w-3 h-3 ${autoRefresh ? 'animate-spin text-blue-400' : 'text-zinc-500'}`} />
          </button>
        </div>
      </div>
    );
  }

  // Full view
  return (
    <div className="bg-zinc-900 border border-zinc-800 rounded-xl p-5 space-y-5">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-2">
          <Activity className="w-5 h-5 text-orange-400" />
          <h3 className="text-lg font-semibold text-white">Pipeline Metrics</h3>
          <span className="text-xs text-zinc-500">v1.24.0</span>
        </div>
        <div className="flex items-center gap-2">
          <button
            onClick={() => setAutoRefresh(!autoRefresh)}
            className={`p-2 rounded-lg transition-colors ${
              autoRefresh ? 'bg-blue-500/20 text-blue-400' : 'bg-zinc-800 text-zinc-500'
            }`}
          >
            <RefreshCw className={`w-4 h-4 ${autoRefresh ? 'animate-spin' : ''}`} />
          </button>
        </div>
      </div>

      {/* Batch Stats Summary */}
      <div className="grid grid-cols-2 md:grid-cols-5 gap-3">
        <div className="bg-blue-500/10 border border-blue-500/20 rounded-lg p-3 text-center">
          <p className="text-2xl font-bold text-blue-400">{runningBatches.length}</p>
          <p className="text-xs text-zinc-500">Running</p>
        </div>
        <div className="bg-green-500/10 border border-green-500/20 rounded-lg p-3 text-center">
          <p className="text-2xl font-bold text-green-400">{batchStats.completed?.count || 0}</p>
          <p className="text-xs text-zinc-500">Completed</p>
        </div>
        <div className="bg-red-500/10 border border-red-500/20 rounded-lg p-3 text-center">
          <p className="text-2xl font-bold text-red-400">{batchStats.failed?.count || 0}</p>
          <p className="text-xs text-zinc-500">Failed</p>
        </div>
        <div className="bg-yellow-500/10 border border-yellow-500/20 rounded-lg p-3 text-center">
          <p className="text-2xl font-bold text-yellow-400">{batchStats.stalled?.count || 0}</p>
          <p className="text-xs text-zinc-500">Stalled</p>
        </div>
        <div className="bg-purple-500/10 border border-purple-500/20 rounded-lg p-3 text-center">
          <p className="text-2xl font-bold text-purple-400">{batchStats.self_healed?.count || 0}</p>
          <p className="text-xs text-zinc-500">Self-Healed</p>
        </div>
      </div>

      {/* Running Batches */}
      {runningBatches.length > 0 && (
        <div className="bg-blue-500/5 border border-blue-500/20 rounded-lg p-4">
          <div className="flex items-center gap-2 mb-3">
            <Loader2 className="w-4 h-4 text-blue-400 animate-spin" />
            <h4 className="text-sm font-medium text-blue-400">Running Batches</h4>
          </div>
          <div className="space-y-2">
            {runningBatches.map(batch => {
              const stallThreshold = thresholds[batch.pipeline_type]?.threshold_ms || 480000;
              const isApproachingStall = batch.ms_since_heartbeat > stallThreshold * 0.7;

              return (
                <div
                  key={batch.batch_id}
                  className={`flex items-center justify-between p-2 rounded ${
                    isApproachingStall ? 'bg-yellow-500/10' : 'bg-zinc-800/50'
                  }`}
                >
                  <div className="flex items-center gap-3">
                    <div className={`w-2 h-2 rounded-full ${
                      isApproachingStall ? 'bg-yellow-500 animate-pulse' : 'bg-blue-500'
                    }`} />
                    <div>
                      <p className="text-sm text-white font-medium">{batch.pipeline_type}</p>
                      <p className="text-xs text-zinc-500">
                        {batch.records_processed}/{batch.records_in_batch} records
                      </p>
                    </div>
                  </div>
                  <div className="text-right">
                    <p className={`text-xs ${isApproachingStall ? 'text-yellow-400' : 'text-zinc-400'}`}>
                      Last heartbeat: {formatMs(batch.ms_since_heartbeat)} ago
                    </p>
                    <p className="text-xs text-zinc-600">
                      Threshold: {formatMs(stallThreshold)}
                    </p>
                  </div>
                </div>
              );
            })}
          </div>
        </div>
      )}

      {/* Pipeline Summary (24h) */}
      {pipelineSummary.length > 0 && (
        <div>
          <div className="flex items-center gap-2 mb-3">
            <BarChart3 className="w-4 h-4 text-zinc-400" />
            <h4 className="text-sm font-medium text-zinc-300">Pipeline Throughput (24h)</h4>
          </div>
          <div className="grid grid-cols-1 md:grid-cols-2 gap-2">
            {pipelineSummary.map(pipeline => (
              <div
                key={pipeline.pipeline_type}
                className="flex items-center justify-between p-3 bg-zinc-800/50 rounded-lg"
              >
                <div>
                  <p className="text-sm text-white">{pipeline.pipeline_type}</p>
                  <p className="text-xs text-zinc-500">
                    {pipeline.batch_count} batches | avg {formatMs(pipeline.avg_duration_ms)}
                  </p>
                </div>
                <div className="text-right">
                  <p className="text-sm font-medium text-green-400">
                    {formatNumber(pipeline.total_enriched)} enriched
                  </p>
                  {pipeline.total_failed > 0 && (
                    <p className="text-xs text-red-400">{pipeline.total_failed} failed</p>
                  )}
                </div>
              </div>
            ))}
          </div>
        </div>
      )}

      {/* API Performance */}
      {apiPerformance.length > 0 && (
        <div>
          <div className="flex items-center gap-2 mb-3">
            <Server className="w-4 h-4 text-zinc-400" />
            <h4 className="text-sm font-medium text-zinc-300">API Performance (24h)</h4>
          </div>
          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-2">
            {apiPerformance.map(api => (
              <div
                key={api.api_source}
                className="p-3 bg-zinc-800/50 rounded-lg"
              >
                <div className="flex items-center justify-between mb-2">
                  <span className="text-sm text-white font-medium">{api.api_source}</span>
                  <span className={`text-xs px-2 py-0.5 rounded-full ${
                    api.success_rate >= 95 ? 'bg-green-500/20 text-green-400' :
                    api.success_rate >= 80 ? 'bg-yellow-500/20 text-yellow-400' :
                    'bg-red-500/20 text-red-400'
                  }`}>
                    {api.success_rate}%
                  </span>
                </div>
                <div className="grid grid-cols-2 gap-2 text-xs">
                  <div>
                    <span className="text-zinc-500">Calls:</span>
                    <span className="text-zinc-300 ml-1">{formatNumber(api.total_calls)}</span>
                  </div>
                  <div>
                    <span className="text-zinc-500">Avg:</span>
                    <span className="text-zinc-300 ml-1">{formatMs(api.avg_response_ms)}</span>
                  </div>
                  <div>
                    <span className="text-zinc-500">P95:</span>
                    <span className="text-zinc-300 ml-1">{formatMs(api.p95_response_ms)}</span>
                  </div>
                  <div>
                    <span className="text-zinc-500">Slow:</span>
                    <span className={`ml-1 ${api.slow_calls > 0 ? 'text-yellow-400' : 'text-zinc-300'}`}>
                      {api.slow_calls}
                    </span>
                  </div>
                </div>
              </div>
            ))}
          </div>
        </div>
      )}

      {/* Recent Stalls & Self-Heals */}
      {(recentStalls.length > 0 || recentSelfHeals.length > 0) && (
        <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
          {recentStalls.length > 0 && (
            <div className="bg-yellow-500/5 border border-yellow-500/20 rounded-lg p-4">
              <div className="flex items-center gap-2 mb-3">
                <AlertTriangle className="w-4 h-4 text-yellow-400" />
                <h4 className="text-sm font-medium text-yellow-400">Recent Stalls</h4>
              </div>
              <div className="space-y-2">
                {recentStalls.slice(0, 5).map(stall => (
                  <div key={stall.batch_id} className="text-xs">
                    <div className="flex items-center justify-between">
                      <span className="text-zinc-300">{stall.pipeline_type}</span>
                      <StatusBadge status={stall.status} />
                    </div>
                    <p className="text-zinc-500">{timeAgo(stall.detected_at)}</p>
                  </div>
                ))}
              </div>
            </div>
          )}

          {recentSelfHeals.length > 0 && (
            <div className="bg-purple-500/5 border border-purple-500/20 rounded-lg p-4">
              <div className="flex items-center gap-2 mb-3">
                <Heart className="w-4 h-4 text-purple-400" />
                <h4 className="text-sm font-medium text-purple-400">Recent Self-Heals</h4>
              </div>
              <div className="space-y-2">
                {recentSelfHeals.slice(0, 5).map(heal => (
                  <div key={heal.batch_id} className="text-xs">
                    <div className="flex items-center justify-between">
                      <span className="text-zinc-300">{heal.pipeline_type}</span>
                      <span className="text-purple-400">{formatMs(heal.duration_ms)}</span>
                    </div>
                    <p className="text-zinc-500">{timeAgo(heal.self_healed_at)}</p>
                  </div>
                ))}
              </div>
            </div>
          )}
        </div>
      )}

      {/* Stall Thresholds */}
      {Object.keys(thresholds).length > 0 && (
        <div className="pt-3 border-t border-zinc-800">
          <div className="flex items-center gap-2 mb-2">
            <Timer className="w-4 h-4 text-zinc-500" />
            <span className="text-xs text-zinc-500">Adaptive Stall Thresholds</span>
          </div>
          <div className="flex flex-wrap gap-2">
            {Object.entries(thresholds).slice(0, 6).map(([pipeline, data]) => (
              <span
                key={pipeline}
                className={`text-xs px-2 py-1 rounded ${
                  data.is_adaptive ? 'bg-green-500/10 text-green-400' : 'bg-zinc-800 text-zinc-400'
                }`}
                title={`${data.sample_count} samples, P95: ${formatMs(data.p95_ms)}`}
              >
                {pipeline.split('_')[0]}: {data.threshold_minutes}m
                {data.is_adaptive && ' ✓'}
              </span>
            ))}
          </div>
        </div>
      )}

      {/* Empty State */}
      {!runningBatches.length && !pipelineSummary.length && !apiPerformance.length && (
        <div className="text-center py-8 text-zinc-500">
          <Database className="w-8 h-8 mx-auto mb-2 opacity-50" />
          <p className="text-sm">No pipeline activity in the last 24 hours</p>
          <p className="text-xs mt-1">Metrics will appear once pipelines start running</p>
        </div>
      )}
    </div>
  );
}
