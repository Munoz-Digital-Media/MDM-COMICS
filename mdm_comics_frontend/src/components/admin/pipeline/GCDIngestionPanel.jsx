import React from 'react';
import {
  Building2, Users, BookOpen, FileText,
  Database, AlertCircle, Layers, RefreshCw, Link2
} from 'lucide-react';
import PhaseProgressBar from './PhaseProgressBar';
import OverallProgressSection from './OverallProgressSection';

/**
 * GCDIngestionPanel - Main panel for GCD data ingestion progress
 *
 * v1.8.0: Refactored for granular progress tracking (IMP-20251221-GCD-GRANULAR-PROGRESS)
 *         - Now uses PhaseProgressBar for each phase
 *         - Now uses OverallProgressSection for summary
 *         - Consumes phase_totals, phase_progress, overall_progress from backend
 */
export default function GCDIngestionPanel({ status, onRefresh, onTrigger }) {
  // Phase configuration with icons
  const phaseConfig = {
    brands: { label: 'Brands', icon: Building2 },
    indicia: { label: 'Indicia Publishers', icon: FileText },
    creators: { label: 'Creators', icon: Users },
    characters: { label: 'Characters', icon: Users },
    issues: { label: 'Issues', icon: BookOpen },
    stories: { label: 'Stories', icon: Layers },
    credits: { label: 'Story Credits', icon: Link2 },
    reprints: { label: 'Reprints', icon: RefreshCw },
  };

  // Ordered phases for display
  const phaseOrder = ['brands', 'indicia', 'creators', 'characters', 'issues', 'stories', 'credits', 'reprints'];

  if (!status) return null;

  const { checkpoint, settings, phase_totals, phase_progress, overall_progress } = status;
  const isRunning = checkpoint?.is_running;
  const currentMode = checkpoint?.state_data?.mode || 'brands';

  // Calculate which phases are complete/active
  const getPhaseStatus = (phaseId) => {
    const phaseIdx = phaseOrder.indexOf(phaseId);
    const currentIdx = phaseOrder.indexOf(currentMode);

    // Get progress data for this phase
    const progress = phase_progress?.[phaseId] || { processed: 0, errors: 0 };
    const total = phase_totals?.[phaseId] || 0;

    // Determine state
    if (phaseIdx < currentIdx) {
      // Past phases are complete
      return { isComplete: true, isActive: false, ...progress, total };
    } else if (phaseIdx === currentIdx && isRunning) {
      // Current phase is active
      return { isComplete: false, isActive: true, ...progress, total };
    } else if (phaseIdx === currentIdx && !isRunning && progress.processed > 0) {
      // Current phase stopped - check if fully processed
      const isComplete = total > 0 && progress.processed >= total;
      return { isComplete, isActive: false, ...progress, total };
    } else {
      // Future phases are pending
      return { isComplete: false, isActive: false, processed: 0, errors: 0, total };
    }
  };

  return (
    <div className="space-y-6">
      {/* Overall Progress Section */}
      <OverallProgressSection
        totalProcessed={overall_progress?.total_processed || checkpoint?.total_processed || 0}
        totalRecords={overall_progress?.total_records || 0}
        totalErrors={overall_progress?.total_errors || checkpoint?.total_errors || 0}
        estimatedTimeRemaining={overall_progress?.estimated_time_remaining}
        recordsPerSecond={overall_progress?.records_per_second || 0}
        isRunning={isRunning}
      />

      {/* Phase Progress Bars */}
      <div className="space-y-3">
        <h3 className="text-sm font-medium text-zinc-400 uppercase tracking-wide">
          Phase Progress
        </h3>
        <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
          {phaseOrder.map((phaseId) => {
            const config = phaseConfig[phaseId];
            const phaseStatus = getPhaseStatus(phaseId);

            return (
              <PhaseProgressBar
                key={phaseId}
                phaseName={config.label}
                icon={config.icon}
                processed={phaseStatus.processed}
                total={phaseStatus.total}
                errors={phaseStatus.errors}
                isActive={phaseStatus.isActive}
                isComplete={phaseStatus.isComplete}
              />
            );
          })}
        </div>
      </div>

      {/* Detailed Phase Metrics */}
      <div className="bg-zinc-900/30 border border-zinc-800 rounded-lg p-6">
        <h3 className="text-lg font-medium text-white mb-4 flex items-center gap-2">
          <Database className="w-5 h-5 text-blue-400" />
          Ingestion Details
        </h3>

        <div className="space-y-4">
          <div className="flex justify-between text-sm border-b border-zinc-800 pb-2">
            <span className="text-zinc-400">Current Phase</span>
            <span className="text-white font-mono">{currentMode.toUpperCase()}</span>
          </div>
          <div className="flex justify-between text-sm border-b border-zinc-800 pb-2">
            <span className="text-zinc-400">Current Offset</span>
            <span className="text-white font-mono">{(checkpoint?.current_offset || 0).toLocaleString()}</span>
          </div>
          <div className="flex justify-between text-sm border-b border-zinc-800 pb-2">
            <span className="text-zinc-400">Dump File</span>
            <span className="text-zinc-300 font-mono text-xs truncate ml-4">{settings?.dump_path || 'N/A'}</span>
          </div>
          <div className="flex justify-between text-sm border-b border-zinc-800 pb-2">
            <span className="text-zinc-400">Batch Size</span>
            <span className="text-white font-mono">{settings?.batch_size || 'N/A'}</span>
          </div>

          {checkpoint?.last_error && (
            <div className="mt-4 p-3 bg-red-950/30 border border-red-900/50 rounded text-sm text-red-200">
              <div className="flex items-center gap-2 mb-1 text-red-400 font-medium">
                <AlertCircle className="w-4 h-4" />
                Last Error
              </div>
              <pre className="whitespace-pre-wrap font-mono text-xs opacity-80">
                {checkpoint.last_error}
              </pre>
            </div>
          )}
        </div>
      </div>
    </div>
  );
}
