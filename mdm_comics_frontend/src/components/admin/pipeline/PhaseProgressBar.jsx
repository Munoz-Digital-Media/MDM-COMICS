import React from 'react';
import { CheckCircle2, Loader2, Circle, AlertTriangle } from 'lucide-react';

/**
 * PhaseProgressBar - Individual phase progress indicator
 *
 * v1.8.0: Added for granular GCD progress tracking (IMP-20251221-GCD-GRANULAR-PROGRESS)
 *
 * @param {string} phaseName - Display name for the phase
 * @param {React.Component} icon - Lucide icon component
 * @param {number} processed - Records processed in this phase
 * @param {number} total - Total records in this phase
 * @param {number} errors - Error count for this phase
 * @param {boolean} isActive - Whether this phase is currently processing
 * @param {boolean} isComplete - Whether this phase has completed
 */
export default function PhaseProgressBar({
  phaseName,
  icon: Icon,
  processed = 0,
  total = 0,
  errors = 0,
  isActive = false,
  isComplete = false,
  startedAt = null,
  completedAt = null
}) {
  // Calculate percentage
  const percentage = total > 0 ? Math.min(100, (processed / total) * 100) : 0;
  const percentageDisplay = percentage.toFixed(1);

  // Determine status icon and colors
  const getStatusIcon = () => {
    if (isComplete) return <CheckCircle2 className="w-5 h-5 text-green-500" />;
    if (isActive) return <Loader2 className="w-5 h-5 text-orange-500 animate-spin" />;
    return <Circle className="w-5 h-5 text-zinc-600" />;
  };

  const getProgressBarColor = () => {
    if (isComplete) return 'bg-green-500';
    if (isActive) return 'bg-orange-500';
    return 'bg-zinc-700';
  };

  const getBorderColor = () => {
    if (isComplete) return 'border-green-500/30';
    if (isActive) return 'border-orange-500/30';
    return 'border-zinc-800';
  };

  // Format duration if completed
  const formatDuration = () => {
    if (!startedAt || !completedAt) return null;
    const start = new Date(startedAt);
    const end = new Date(completedAt);
    const durationMs = end - start;
    const minutes = Math.floor(durationMs / 60000);
    const seconds = Math.floor((durationMs % 60000) / 1000);
    if (minutes > 0) return `${minutes}m ${seconds}s`;
    return `${seconds}s`;
  };

  const duration = formatDuration();

  return (
    <div className={`bg-zinc-900 border ${getBorderColor()} rounded-lg p-4 transition-all`}>
      <div className="flex items-center justify-between mb-2">
        <div className="flex items-center gap-3">
          {getStatusIcon()}
          <div className="flex items-center gap-2">
            {Icon && <Icon className={`w-4 h-4 ${isActive ? 'text-orange-400' : isComplete ? 'text-green-400' : 'text-zinc-500'}`} />}
            <span className={`font-medium ${isActive ? 'text-white' : isComplete ? 'text-green-400' : 'text-zinc-400'}`}>
              {phaseName}
            </span>
          </div>
        </div>

        <div className="flex items-center gap-3">
          {/* Error badge */}
          {errors > 0 && (
            <div className="flex items-center gap-1 bg-red-500/20 text-red-400 text-xs px-2 py-1 rounded">
              <AlertTriangle className="w-3 h-3" />
              {errors.toLocaleString()} errors
            </div>
          )}

          {/* Numeric progress */}
          <span className="text-sm font-mono text-zinc-300">
            {processed.toLocaleString()} / {total.toLocaleString()}
          </span>
        </div>
      </div>

      {/* Progress bar */}
      <div className="w-full bg-zinc-800 rounded-full h-3 overflow-hidden">
        <div
          className={`${getProgressBarColor()} h-full transition-all duration-500 ease-out`}
          style={{ width: `${percentage}%` }}
        />
      </div>

      {/* Status line */}
      <div className="flex justify-between items-center mt-2 text-xs">
        <span className={`${isComplete ? 'text-green-400' : isActive ? 'text-orange-400' : 'text-zinc-500'}`}>
          {isComplete ? (
            <>Completed{duration && ` in ${duration}`}</>
          ) : isActive ? (
            <>{percentageDisplay}% complete</>
          ) : (
            'Pending'
          )}
        </span>

        {total > 0 && (
          <span className="text-zinc-500">
            {percentageDisplay}%
          </span>
        )}
      </div>
    </div>
  );
}
