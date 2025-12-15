/**
 * CoverIngestion - Admin UI for ingesting local cover images
 * v1.21.0: Cover Ingestion Pipeline
 *
 * Workflow:
 * 1. Enter folder path
 * 2. Preview (shows what will be queued)
 * 3. Ingest (queues to Match Review for human approval)
 * 4. Review in Match Review screen
 * 5. Products created on approval
 *
 * Supports incremental ingestion - already processed files are skipped.
 */
import React, { useState, useCallback } from 'react';
import {
  FolderOpen, Search, Upload, Loader2, CheckCircle, XCircle,
  AlertTriangle, Image, Package, RefreshCw, ChevronDown, ChevronRight,
  Clock, CheckSquare, XSquare
} from 'lucide-react';
import { adminAPI } from '../../../services/adminApi';

// Match score color coding
function getScoreColor(score) {
  if (score >= 8) return 'text-green-400';
  if (score >= 5) return 'text-yellow-400';
  if (score > 0) return 'text-orange-400';
  return 'text-red-400';
}

function getDispositionBadge(disposition) {
  switch (disposition) {
    case 'auto_link':
      return <span className="px-2 py-0.5 bg-green-500/20 text-green-400 rounded text-xs">High Confidence</span>;
    case 'review':
      return <span className="px-2 py-0.5 bg-yellow-500/20 text-yellow-400 rounded text-xs">Needs Review</span>;
    case 'no_match':
      return <span className="px-2 py-0.5 bg-red-500/20 text-red-400 rounded text-xs">No Match</span>;
    default:
      return <span className="px-2 py-0.5 bg-zinc-500/20 text-zinc-400 rounded text-xs">{disposition}</span>;
  }
}

function PreviewItem({ item, expanded, onToggle }) {
  const fileName = item.file_path.split(/[/\\]/).pop();

  return (
    <div className="bg-zinc-800/50 rounded-lg border border-zinc-700">
      <button
        onClick={onToggle}
        className="w-full px-4 py-3 flex items-center justify-between hover:bg-zinc-800 transition-colors"
      >
        <div className="flex items-center gap-3">
          {expanded ? <ChevronDown className="w-4 h-4 text-zinc-400" /> : <ChevronRight className="w-4 h-4 text-zinc-400" />}
          <Image className="w-4 h-4 text-zinc-500" />
          <span className="text-white font-medium truncate max-w-md">{fileName}</span>
        </div>
        <div className="flex items-center gap-3">
          <span className={`font-mono text-sm ${getScoreColor(item.match_score)}`}>
            Score: {item.match_score}
          </span>
          {getDispositionBadge(item.disposition)}
        </div>
      </button>

      {expanded && (
        <div className="px-4 pb-4 pt-2 border-t border-zinc-700">
          <div className="grid grid-cols-2 gap-4 text-sm">
            <div>
              <p className="text-zinc-500 mb-1">Publisher</p>
              <p className="text-white">{item.publisher || '-'}</p>
            </div>
            <div>
              <p className="text-zinc-500 mb-1">Series</p>
              <p className="text-white">{item.series || '-'}</p>
            </div>
            <div>
              <p className="text-zinc-500 mb-1">Volume</p>
              <p className="text-white">{item.volume || '-'}</p>
            </div>
            <div>
              <p className="text-zinc-500 mb-1">Issue #</p>
              <p className="text-white">{item.issue_number || '-'}</p>
            </div>
            <div>
              <p className="text-zinc-500 mb-1">Variant</p>
              <p className="text-white">{item.variant_code || '-'}</p>
            </div>
            <div>
              <p className="text-zinc-500 mb-1">CGC Grade</p>
              <p className="text-white">{item.cgc_grade || '-'}</p>
            </div>
            <div>
              <p className="text-zinc-500 mb-1">Cover Type</p>
              <p className="text-white">{item.cover_type}</p>
            </div>
            <div>
              <p className="text-zinc-500 mb-1">Match Method</p>
              <p className="text-white">{item.match_method}</p>
            </div>
          </div>
          {item.product_name && (
            <div className="mt-3 p-2 bg-zinc-700/50 rounded">
              <p className="text-zinc-400 text-sm">Product Name: <span className="text-white">{item.product_name}</span></p>
            </div>
          )}
          {item.matched_issue_id && (
            <div className="mt-2 p-2 bg-green-500/10 rounded border border-green-500/30">
              <p className="text-green-400 text-sm">
                Matched to comic_issue #{item.matched_issue_id}
              </p>
            </div>
          )}
          <p className="mt-2 text-xs text-zinc-600 break-all">{item.file_path}</p>
        </div>
      )}
    </div>
  );
}

export default function CoverIngestion() {
  // Form state
  const [folderPath, setFolderPath] = useState('F:\\apps\\mdm_comics\\assets\\comic_book_covers');
  const [previewLimit, setPreviewLimit] = useState(50);
  const [ingestionLimit, setIngestionLimit] = useState(null);

  // Loading states
  const [previewing, setPreviewing] = useState(false);
  const [ingesting, setIngesting] = useState(false);
  const [loadingStats, setLoadingStats] = useState(false);

  // Results
  const [preview, setPreview] = useState(null);
  const [ingestionResult, setIngestionResult] = useState(null);
  const [stats, setStats] = useState(null);
  const [error, setError] = useState(null);

  // UI state
  const [expandedItems, setExpandedItems] = useState(new Set());

  const toggleItem = (index) => {
    setExpandedItems(prev => {
      const next = new Set(prev);
      if (next.has(index)) {
        next.delete(index);
      } else {
        next.add(index);
      }
      return next;
    });
  };

  const handlePreview = useCallback(async () => {
    if (!folderPath.trim()) {
      setError('Please enter a folder path');
      return;
    }

    setPreviewing(true);
    setError(null);
    setPreview(null);

    try {
      const result = await adminAPI.previewCoverIngestion(folderPath, previewLimit);
      setPreview(result);
    } catch (err) {
      setError(err.message);
    } finally {
      setPreviewing(false);
    }
  }, [folderPath, previewLimit]);

  const handleIngest = useCallback(async () => {
    if (!folderPath.trim()) {
      setError('Please enter a folder path');
      return;
    }

    if (!confirm(`This will queue covers from:\n${folderPath}\n\nItems will appear in Match Review for approval.\nAlready-processed files will be skipped.\n\nContinue?`)) {
      return;
    }

    setIngesting(true);
    setError(null);
    setIngestionResult(null);

    try {
      const result = await adminAPI.ingestCovers({
        folderPath,
        limit: ingestionLimit || undefined,
      });
      setIngestionResult(result);

      // Refresh stats after ingestion
      loadStats();
    } catch (err) {
      setError(err.message);
    } finally {
      setIngesting(false);
    }
  }, [folderPath, ingestionLimit]);

  const loadStats = useCallback(async () => {
    setLoadingStats(true);
    try {
      const result = await adminAPI.getCoverIngestionStats();
      setStats(result);
    } catch (err) {
      console.error('Failed to load stats:', err);
    } finally {
      setLoadingStats(false);
    }
  }, []);

  // Load stats on mount
  React.useEffect(() => {
    loadStats();
  }, [loadStats]);

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div>
          <h2 className="text-xl font-bold text-white flex items-center gap-2">
            <Image className="w-6 h-6 text-orange-400" />
            Cover Ingestion
          </h2>
          <p className="text-zinc-500 text-sm mt-1">
            Queue local cover images for Match Review approval
          </p>
        </div>
        <button
          onClick={loadStats}
          disabled={loadingStats}
          className="flex items-center gap-2 px-3 py-2 bg-zinc-800 hover:bg-zinc-700 rounded-lg text-zinc-300 transition-colors"
        >
          <RefreshCw className={`w-4 h-4 ${loadingStats ? 'animate-spin' : ''}`} />
          Refresh Stats
        </button>
      </div>

      {/* Stats Cards */}
      {stats && (
        <div className="grid grid-cols-2 md:grid-cols-4 lg:grid-cols-6 gap-4">
          <div className="bg-zinc-900 border border-zinc-800 rounded-xl p-4">
            <div className="flex items-center gap-2 mb-2">
              <Clock className="w-5 h-5 text-yellow-400" />
              <span className="text-zinc-500 text-sm">Pending Review</span>
            </div>
            <p className="text-2xl font-bold text-white">{stats.queue?.pending || 0}</p>
          </div>

          <div className="bg-zinc-900 border border-zinc-800 rounded-xl p-4">
            <div className="flex items-center gap-2 mb-2">
              <CheckSquare className="w-5 h-5 text-green-400" />
              <span className="text-zinc-500 text-sm">Approved</span>
            </div>
            <p className="text-2xl font-bold text-white">{stats.queue?.approved || 0}</p>
          </div>

          <div className="bg-zinc-900 border border-zinc-800 rounded-xl p-4">
            <div className="flex items-center gap-2 mb-2">
              <XSquare className="w-5 h-5 text-red-400" />
              <span className="text-zinc-500 text-sm">Rejected</span>
            </div>
            <p className="text-2xl font-bold text-white">{stats.queue?.rejected || 0}</p>
          </div>

          <div className="bg-zinc-900 border border-zinc-800 rounded-xl p-4">
            <div className="flex items-center gap-2 mb-2">
              <Package className="w-5 h-5 text-orange-400" />
              <span className="text-zinc-500 text-sm">Products Created</span>
            </div>
            <p className="text-2xl font-bold text-white">{stats.products_created?.total || 0}</p>
          </div>

          <div className="bg-zinc-900 border border-zinc-800 rounded-xl p-4">
            <div className="flex items-center gap-2 mb-2">
              <CheckCircle className="w-5 h-5 text-blue-400" />
              <span className="text-zinc-500 text-sm">High Confidence</span>
            </div>
            <p className="text-2xl font-bold text-green-400">{stats.queue?.pending_high_confidence || 0}</p>
          </div>

          <div className="bg-zinc-900 border border-zinc-800 rounded-xl p-4">
            <div className="flex items-center gap-2 mb-2">
              <AlertTriangle className="w-5 h-5 text-yellow-400" />
              <span className="text-zinc-500 text-sm">Low Confidence</span>
            </div>
            <p className="text-2xl font-bold text-red-400">{stats.queue?.pending_low_confidence || 0}</p>
          </div>
        </div>
      )}

      {/* Input Form */}
      <div className="bg-zinc-900 border border-zinc-800 rounded-xl p-6">
        <h3 className="text-lg font-semibold text-white mb-4 flex items-center gap-2">
          <FolderOpen className="w-5 h-5 text-orange-400" />
          Source Folder
        </h3>

        <div className="space-y-4">
          <div>
            <label className="block text-sm text-zinc-400 mb-2">Folder Path</label>
            <input
              type="text"
              value={folderPath}
              onChange={(e) => setFolderPath(e.target.value)}
              className="w-full bg-zinc-800 border border-zinc-700 rounded-lg px-4 py-2.5 text-white placeholder-zinc-500 focus:outline-none focus:border-orange-500"
              placeholder="F:\apps\mdm_comics\assets\comic_book_covers"
            />
            <p className="text-xs text-zinc-600 mt-1">
              Structure: publisher/series/[volume]/issue/filename.jpg
            </p>
          </div>

          <div className="grid grid-cols-2 gap-4">
            <div>
              <label className="block text-sm text-zinc-400 mb-2">Preview Limit</label>
              <input
                type="number"
                value={previewLimit}
                onChange={(e) => setPreviewLimit(parseInt(e.target.value) || 50)}
                min={1}
                max={500}
                className="w-full bg-zinc-800 border border-zinc-700 rounded-lg px-4 py-2 text-white focus:outline-none focus:border-orange-500"
              />
            </div>

            <div>
              <label className="block text-sm text-zinc-400 mb-2">Ingest Limit (optional)</label>
              <input
                type="number"
                value={ingestionLimit || ''}
                onChange={(e) => setIngestionLimit(e.target.value ? parseInt(e.target.value) : null)}
                min={1}
                placeholder="All files"
                className="w-full bg-zinc-800 border border-zinc-700 rounded-lg px-4 py-2 text-white placeholder-zinc-500 focus:outline-none focus:border-orange-500"
              />
            </div>
          </div>

          <div className="bg-zinc-800/50 rounded-lg p-3 border border-zinc-700">
            <p className="text-zinc-400 text-sm">
              <strong className="text-zinc-300">Workflow:</strong> Files are queued to Match Review for human approval.
              Already-processed files (in queue or approved/rejected) are automatically skipped.
              Products are only created when you approve items in Match Review.
            </p>
          </div>

          <div className="flex gap-3 pt-2">
            <button
              onClick={handlePreview}
              disabled={previewing || ingesting}
              className="flex items-center gap-2 px-5 py-2.5 bg-zinc-700 hover:bg-zinc-600 disabled:bg-zinc-800 disabled:text-zinc-500 rounded-lg text-white font-medium transition-colors"
            >
              {previewing ? (
                <Loader2 className="w-4 h-4 animate-spin" />
              ) : (
                <Search className="w-4 h-4" />
              )}
              Preview
            </button>

            <button
              onClick={handleIngest}
              disabled={previewing || ingesting}
              className="flex items-center gap-2 px-5 py-2.5 bg-orange-500 hover:bg-orange-600 disabled:bg-orange-500/50 disabled:text-orange-200 rounded-lg text-white font-medium transition-colors"
            >
              {ingesting ? (
                <Loader2 className="w-4 h-4 animate-spin" />
              ) : (
                <Upload className="w-4 h-4" />
              )}
              Queue for Review
            </button>
          </div>
        </div>
      </div>

      {/* Error Display */}
      {error && (
        <div className="bg-red-500/10 border border-red-500/30 rounded-xl p-4 flex items-start gap-3">
          <XCircle className="w-5 h-5 text-red-400 shrink-0 mt-0.5" />
          <div>
            <p className="text-red-400 font-medium">Error</p>
            <p className="text-red-300/80 text-sm mt-1">{error}</p>
          </div>
        </div>
      )}

      {/* Ingestion Result */}
      {ingestionResult && (
        <div className="bg-green-500/10 border border-green-500/30 rounded-xl p-6">
          <div className="flex items-center gap-3 mb-4">
            <CheckCircle className="w-6 h-6 text-green-400" />
            <h3 className="text-lg font-semibold text-green-400">Queued for Review</h3>
          </div>

          <div className="grid grid-cols-2 md:grid-cols-4 gap-4 text-sm">
            <div>
              <p className="text-zinc-400">Total Files</p>
              <p className="text-2xl font-bold text-white">{ingestionResult.total_files}</p>
            </div>
            <div>
              <p className="text-zinc-400">Queued</p>
              <p className="text-2xl font-bold text-green-400">{ingestionResult.queued_for_review}</p>
            </div>
            <div>
              <p className="text-zinc-400">High Confidence</p>
              <p className="text-2xl font-bold text-green-400">{ingestionResult.high_confidence}</p>
            </div>
            <div>
              <p className="text-zinc-400">Medium Confidence</p>
              <p className="text-2xl font-bold text-yellow-400">{ingestionResult.medium_confidence}</p>
            </div>
            <div>
              <p className="text-zinc-400">Low Confidence</p>
              <p className="text-2xl font-bold text-red-400">{ingestionResult.low_confidence}</p>
            </div>
            <div>
              <p className="text-zinc-400">Skipped</p>
              <p className="text-2xl font-bold text-zinc-400">{ingestionResult.skipped}</p>
            </div>
            <div>
              <p className="text-zinc-400">Errors</p>
              <p className="text-2xl font-bold text-red-400">{ingestionResult.errors}</p>
            </div>
          </div>

          <div className="mt-4 p-3 bg-zinc-800/50 rounded-lg">
            <p className="text-zinc-300 text-sm">
              Go to <strong>Match Review</strong> to approve or reject queued items.
              Filter by <code className="bg-zinc-700 px-1 rounded">cover_ingestion</code> entity type.
            </p>
          </div>

          {ingestionResult.error_details && ingestionResult.error_details.length > 0 && (
            <div className="mt-4 p-3 bg-red-500/10 rounded-lg border border-red-500/30">
              <p className="text-red-400 text-sm font-medium mb-2">Error Details:</p>
              {ingestionResult.error_details.map((err, i) => (
                <p key={i} className="text-red-300/80 text-xs break-all">{err}</p>
              ))}
            </div>
          )}
        </div>
      )}

      {/* Preview Results */}
      {preview && (
        <div className="bg-zinc-900 border border-zinc-800 rounded-xl p-6">
          <div className="flex items-center justify-between mb-4">
            <h3 className="text-lg font-semibold text-white flex items-center gap-2">
              <Search className="w-5 h-5 text-orange-400" />
              Preview Results
            </h3>
            <div className="flex gap-4 text-sm">
              <span className="text-green-400">{preview.stats.high_confidence} high confidence</span>
              <span className="text-yellow-400">{preview.stats.low_confidence} low confidence</span>
              <span className="text-red-400">{preview.stats.no_match} no match</span>
            </div>
          </div>

          {/* Publishers found */}
          <div className="mb-4 flex flex-wrap gap-2">
            {preview.stats.publishers.map((pub, i) => (
              <span key={i} className="px-2 py-1 bg-zinc-800 rounded text-xs text-zinc-400">
                {pub}
              </span>
            ))}
          </div>

          {/* Preview items */}
          <div className="space-y-2 max-h-[600px] overflow-y-auto pr-2">
            {preview.previews.map((item, index) => (
              <PreviewItem
                key={index}
                item={item}
                expanded={expandedItems.has(index)}
                onToggle={() => toggleItem(index)}
              />
            ))}
          </div>

          {preview.stats.total_files > previewLimit && (
            <p className="text-zinc-500 text-sm mt-4 text-center">
              Showing {previewLimit} of {preview.stats.total_files} files. Increase preview limit to see more.
            </p>
          )}
        </div>
      )}
    </div>
  );
}
