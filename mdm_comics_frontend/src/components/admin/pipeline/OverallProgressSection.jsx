import React from 'react';
import { Database, Clock, Zap, CheckCircle2, AlertTriangle } from 'lucide-react';

/**
 * OverallProgressSection - Summary progress display for GCD ingestion
 *
 * v1.8.0: Added for granular GCD progress tracking (IMP-20251221-GCD-GRANULAR-PROGRESS)
 *
 * @param {number} totalProcessed - Total records processed across all phases
 * @param {number} totalRecords - Total records across all phases
 * @param {number} totalErrors - Total errors across all phases
 * @param {string} estimatedTimeRemaining - Human-readable ETA string
 * @param {number} recordsPerSecond - Current processing velocity
 * @param {boolean} isRunning - Whether ingestion is currently running
 */
export default function OverallProgressSection({
  totalProcessed = 0,
  totalRecords = 0,
  totalErrors = 0,
  estimatedTimeRemaining = null,
  recordsPerSecond = 0,
  isRunning = false
}) {
  // Calculate overall percentage
  const percentage = totalRecords > 0 ? Math.min(100, (totalProcessed / totalRecords) * 100) : 0;
  const percentageDisplay = percentage.toFixed(1);
  const isComplete = totalProcessed >= totalRecords && totalRecords > 0;

  // Format large numbers with K/M suffixes for compact display
  const formatCompact = (num) => {
    if (num >= 1000000) return `${(num / 1000000).toFixed(1)}M`;
    if (num >= 1000) return `${(num / 1000).toFixed(1)}K`;
    return num.toLocaleString();
  };

  return (
    <div className="bg-zinc-900 border border-zinc-800 rounded-lg p-6">
      {/* Header */}
      <div className="flex items-center justify-between mb-4">
        <div className="flex items-center gap-3">
          <Database className={`w-6 h-6 ${isRunning ? 'text-orange-500' : isComplete ? 'text-green-500' : 'text-zinc-500'}`} />
          <h3 className="text-lg font-semibold text-white">Overall Progress</h3>
        </div>

        {isComplete && (
          <div className="flex items-center gap-2 bg-green-500/20 text-green-400 px-3 py-1 rounded-full text-sm">
            <CheckCircle2 className="w-4 h-4" />
            Complete
          </div>
        )}
      </div>

      {/* Main progress bar */}
      <div className="w-full bg-zinc-800 rounded-full h-5 overflow-hidden mb-4">
        <div
          className={`h-full transition-all duration-500 ease-out ${
            isComplete ? 'bg-green-500' : isRunning ? 'bg-gradient-to-r from-orange-600 to-orange-400' : 'bg-zinc-700'
          }`}
          style={{ width: `${percentage}%` }}
        />
      </div>

      {/* Stats grid */}
      <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
        {/* Processed */}
        <div className="bg-zinc-800/50 rounded-lg p-3">
          <div className="text-xs text-zinc-500 uppercase tracking-wide mb-1">Processed</div>
          <div className="text-xl font-mono font-semibold text-white">
            {formatCompact(totalProcessed)}
          </div>
          <div className="text-xs text-zinc-500">
            of {formatCompact(totalRecords)}
          </div>
        </div>

        {/* Percentage */}
        <div className="bg-zinc-800/50 rounded-lg p-3">
          <div className="text-xs text-zinc-500 uppercase tracking-wide mb-1">Complete</div>
          <div className={`text-xl font-mono font-semibold ${
            isComplete ? 'text-green-400' : isRunning ? 'text-orange-400' : 'text-white'
          }`}>
            {percentageDisplay}%
          </div>
          <div className="text-xs text-zinc-500">
            {totalRecords - totalProcessed > 0 ? `${formatCompact(totalRecords - totalProcessed)} remaining` : 'Done'}
          </div>
        </div>

        {/* Velocity */}
        <div className="bg-zinc-800/50 rounded-lg p-3">
          <div className="text-xs text-zinc-500 uppercase tracking-wide mb-1 flex items-center gap-1">
            <Zap className="w-3 h-3" />
            Velocity
          </div>
          <div className="text-xl font-mono font-semibold text-white">
            {recordsPerSecond > 0 ? recordsPerSecond.toFixed(1) : '—'}
          </div>
          <div className="text-xs text-zinc-500">records/sec</div>
        </div>

        {/* ETA */}
        <div className="bg-zinc-800/50 rounded-lg p-3">
          <div className="text-xs text-zinc-500 uppercase tracking-wide mb-1 flex items-center gap-1">
            <Clock className="w-3 h-3" />
            ETA
          </div>
          <div className="text-xl font-mono font-semibold text-white">
            {isComplete ? '—' : (estimatedTimeRemaining || '—')}
          </div>
          <div className="text-xs text-zinc-500">
            {isComplete ? 'Completed' : isRunning ? 'remaining' : 'Not running'}
          </div>
        </div>
      </div>

      {/* Error summary if any */}
      {totalErrors > 0 && (
        <div className="mt-4 flex items-center gap-2 bg-red-500/10 border border-red-500/30 rounded-lg px-4 py-3">
          <AlertTriangle className="w-5 h-5 text-red-400" />
          <span className="text-red-400">
            {totalErrors.toLocaleString()} total errors across all phases
          </span>
        </div>
      )}
    </div>
  );
}
