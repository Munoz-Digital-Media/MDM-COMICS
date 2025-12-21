import React, { useState, useEffect } from 'react';
import { 
  Building2, Users, BookOpen, FileText, 
  Database, AlertCircle, CheckCircle2, Loader2,
  RefreshCw, Layers
} from 'lucide-react';

export default function GCDIngestionPanel({ status, onRefresh, onTrigger }) {
  const [activeStep, setActiveStep] = useState(0);
  
  // Phase mapping for the stepper
  const phases = [
    { id: 'brands', label: 'Brands', icon: Building2 },
    { id: 'indicia_publishers', label: 'Indicia', icon: FileText },
    { id: 'creators', label: 'Creators', icon: Users },
    { id: 'characters', label: 'Characters', icon: Users },
    { id: 'issues', label: 'Issues', icon: BookOpen },
    { id: 'stories', label: 'Stories', icon: Layers },
    { id: 'story_credits', label: 'Credits', icon: Users },
    { id: 'story_characters', label: 'Appearances', icon: Users },
    { id: 'reprints', label: 'Reprints', icon: RefreshCw },
  ];

  // Derive current phase from status
  const currentMode = status?.checkpoint?.state_data?.mode || 'brands';
  
  useEffect(() => {
    const index = phases.findIndex(p => p.id === currentMode);
    if (index !== -1) setActiveStep(index);
  }, [currentMode]);

  if (!status) return null;

  const { checkpoint, settings } = status;
  const isRunning = checkpoint?.is_running;
  
  // Calculate aggregate stats
  const totalProcessed = checkpoint?.total_processed || 0;
  const totalErrors = checkpoint?.total_errors || 0;
  
  // Get phase-specific stats if available (requires backend update)
  // Fallback to global counters for now if granular stats missing
  const currentOffset = checkpoint?.current_offset || 0;

  return (
    <div className="space-y-6">
      {/* Header Stats */}
      <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
        <div className="bg-zinc-900/50 border border-zinc-800 p-4 rounded-lg">
          <div className="text-zinc-400 text-sm mb-1">Status</div>
          <div className="flex items-center gap-2">
            {isRunning ? (
              <>
                <Loader2 className="w-4 h-4 text-blue-500 animate-spin" />
                <span className="text-blue-400 font-medium">Processing: {currentMode}</span>
              </>
            ) : (
              <>
                <div className="w-2 h-2 rounded-full bg-zinc-600" />
                <span className="text-zinc-400">Idle</span>
              </>
            )}
          </div>
        </div>
        
        <div className="bg-zinc-900/50 border border-zinc-800 p-4 rounded-lg">
          <div className="text-zinc-400 text-sm mb-1">Total Processed</div>
          <div className="text-2xl font-mono text-white">{totalProcessed.toLocaleString()}</div>
        </div>

        <div className="bg-zinc-900/50 border border-zinc-800 p-4 rounded-lg">
          <div className="text-zinc-400 text-sm mb-1">Current Offset</div>
          <div className="text-2xl font-mono text-emerald-400">{currentOffset.toLocaleString()}</div>
        </div>

        <div className="bg-zinc-900/50 border border-zinc-800 p-4 rounded-lg">
          <div className="text-zinc-400 text-sm mb-1">Total Errors</div>
          <div className="text-2xl font-mono text-red-400">{totalErrors.toLocaleString()}</div>
        </div>
      </div>

      {/* Progress Stepper */}
      <div className="relative">
        <div className="absolute top-1/2 left-0 w-full h-0.5 bg-zinc-800 -z-10" />
        <div className="flex justify-between">
          {phases.map((phase, index) => {
            const Icon = phase.icon;
            const isActive = index === activeStep;
            const isCompleted = index < activeStep;
            const isPending = index > activeStep;

            return (
              <div key={phase.id} className="flex flex-col items-center gap-2 bg-zinc-950 px-2">
                <div className={`
                  w-10 h-10 rounded-full flex items-center justify-center border-2 transition-all
                  ${isActive ? 'border-blue-500 bg-blue-500/10 text-blue-400' : ''}
                  ${isCompleted ? 'border-emerald-500 bg-emerald-500/10 text-emerald-400' : ''}
                  ${isPending ? 'border-zinc-800 bg-zinc-900 text-zinc-600' : ''}
                `}>
                  {isCompleted ? (
                    <CheckCircle2 className="w-5 h-5" />
                  ) : (
                    <Icon className="w-5 h-5" />
                  )}
                </div>
                <span className={`text-xs font-medium ${isActive ? 'text-white' : 'text-zinc-500'}`}>
                  {phase.label}
                </span>
              </div>
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
            <span className="text-zinc-400">Dump File</span>
            <span className="text-zinc-300 font-mono text-xs">{settings?.dump_path}</span>
          </div>
          <div className="flex justify-between text-sm border-b border-zinc-800 pb-2">
            <span className="text-zinc-400">Batch Size</span>
            <span className="text-white font-mono">{settings?.batch_size}</span>
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
