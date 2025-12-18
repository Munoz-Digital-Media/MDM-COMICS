/**
 * MatchReviewQueue - Admin interface for reviewing uncertain matches
 *
 * Per constitution_ui.json:
 * - WCAG 2.2 AA compliant
 * - Full keyboard navigation
 * - ARIA labels and live regions
 * - Focus management
 */

import React, { useState, useEffect, useCallback, useRef } from 'react';
import { matchReviewAPI } from '../../../../services/api';
import MatchCard from './MatchCard';
import MatchComparison from './MatchComparison';
import ManualSearchModal from './ManualSearchModal';
import MatchStats from './MatchStats';
import './matchReview.css';

const MatchReviewQueue = () => {
  // State
  const [matches, setMatches] = useState([]);
  const [stats, setStats] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [selectedMatch, setSelectedMatch] = useState(null);
  const [showManualSearch, setShowManualSearch] = useState(false);
  const [manualSearchEntity, setManualSearchEntity] = useState(null);
  const [filter, setFilter] = useState({
    status: 'pending',
    entity_type: null,
    min_score: null,
    max_score: null,
    escalated_only: false,
    limit: 50,
    offset: 0
  });
  const [total, setTotal] = useState(0);
  const [processing, setProcessing] = useState(new Set());

  // Bulk selection state
  const [selectedIds, setSelectedIds] = useState(new Set());
  const [showBulkRejectModal, setShowBulkRejectModal] = useState(false);
  const [bulkRejectReason, setBulkRejectReason] = useState('wrong_match');
  const [bulkRejectNotes, setBulkRejectNotes] = useState('');
  const [bulkProcessing, setBulkProcessing] = useState(false);

  // Refs for focus management
  const firstCardRef = useRef(null);
  const announcerRef = useRef(null);

  // Fetch queue data
  const fetchQueue = useCallback(async () => {
    try {
      setLoading(true);
      setError(null);

      const response = await matchReviewAPI.getQueue(filter);
      setMatches(response.items || []);
      setTotal(response.total || 0);
      setStats({
        pending_count: response.pending_count,
        escalated_count: response.escalated_count
      });
      // Clear selection when filter changes
      setSelectedIds(new Set());
    } catch (err) {
      setError(err.message || 'Failed to load queue');
      console.error('Queue fetch error:', err);
    } finally {
      setLoading(false);
    }
  }, [filter]);

  // Fetch stats
  const fetchStats = useCallback(async () => {
    try {
      const response = await matchReviewAPI.getStats();
      setStats(response);
    } catch (err) {
      console.error('Stats fetch error:', err);
    }
  }, []);

  // Initial load
  useEffect(() => {
    fetchQueue();
    fetchStats();
  }, [fetchQueue, fetchStats]);

  // Focus first card after load
  useEffect(() => {
    if (!loading && matches.length > 0 && firstCardRef.current) {
      firstCardRef.current.focus();
    }
  }, [loading, matches]);

  // Screen reader announcements
  const announce = useCallback((message) => {
    if (announcerRef.current) {
      announcerRef.current.textContent = message;
      setTimeout(() => {
        if (announcerRef.current) {
          announcerRef.current.textContent = '';
        }
      }, 1000);
    }
  }, []);

  // Selection handlers
  const toggleSelect = useCallback((id) => {
    setSelectedIds(prev => {
      const next = new Set(prev);
      if (next.has(id)) {
        next.delete(id);
      } else {
        next.add(id);
      }
      return next;
    });
  }, []);

  const selectAll = useCallback(() => {
    setSelectedIds(new Set(matches.map(m => m.id)));
    announce(`Selected all ${matches.length} matches`);
  }, [matches, announce]);

  const deselectAll = useCallback(() => {
    setSelectedIds(new Set());
    announce('Selection cleared');
  }, [announce]);

  // Actions
  const handleApprove = async (matchId) => {
    if (processing.has(matchId)) return;

    setProcessing(prev => new Set([...prev, matchId]));
    try {
      await matchReviewAPI.approve(matchId);
      announce('Match approved successfully');
      setMatches(prev => prev.filter(m => m.id !== matchId));
      setSelectedMatch(null);
      setSelectedIds(prev => {
        const next = new Set(prev);
        next.delete(matchId);
        return next;
      });
      fetchStats();
    } catch (err) {
      announce('Failed to approve match');
      console.error('Approve error:', err);
    } finally {
      setProcessing(prev => {
        const next = new Set(prev);
        next.delete(matchId);
        return next;
      });
    }
  };

  const handleReject = async (matchId, reason) => {
    if (processing.has(matchId)) return;

    setProcessing(prev => new Set([...prev, matchId]));
    try {
      await matchReviewAPI.reject(matchId, reason);
      announce('Match rejected');
      setMatches(prev => prev.filter(m => m.id !== matchId));
      setSelectedMatch(null);
      setSelectedIds(prev => {
        const next = new Set(prev);
        next.delete(matchId);
        return next;
      });
      fetchStats();
    } catch (err) {
      announce('Failed to reject match');
      console.error('Reject error:', err);
    } finally {
      setProcessing(prev => {
        const next = new Set(prev);
        next.delete(matchId);
        return next;
      });
    }
  };

  const handleSkip = async (matchId) => {
    await matchReviewAPI.skip(matchId);
    setSelectedMatch(null);
  };

  const handleBulkApprove = async () => {
    const eligibleIds = matches
      .filter(m => m.can_bulk_approve && !processing.has(m.id))
      .map(m => m.id);

    if (eligibleIds.length === 0) {
      announce('No matches eligible for bulk approve');
      return;
    }

    if (!window.confirm(`Approve ${eligibleIds.length} matches with score >= 8?`)) {
      return;
    }

    try {
      const response = await matchReviewAPI.bulkApprove(eligibleIds);
      announce(`Approved ${response.approved_count} matches`);
      fetchQueue();
      fetchStats();
    } catch (err) {
      announce('Bulk approve failed');
      console.error('Bulk approve error:', err);
    }
  };

  // Bulk reject selected matches
  const handleBulkReject = async () => {
    if (selectedIds.size === 0) return;

    setBulkProcessing(true);
    try {
      const response = await matchReviewAPI.bulkReject(
        Array.from(selectedIds),
        bulkRejectReason,
        bulkRejectNotes || null
      );
      announce(`Rejected ${response.rejected_count} matches`);
      setShowBulkRejectModal(false);
      setBulkRejectNotes('');
      fetchQueue();
      fetchStats();
    } catch (err) {
      announce('Bulk reject failed');
      console.error('Bulk reject error:', err);
    } finally {
      setBulkProcessing(false);
    }
  };

  const handleManualLink = async (entityType, entityId, pricechartingId) => {
    try {
      await matchReviewAPI.manualLink(entityType, entityId, pricechartingId);
      announce('Manual link created');
      setShowManualSearch(false);
      setManualSearchEntity(null);
      fetchQueue();
    } catch (err) {
      announce('Manual link failed');
      console.error('Manual link error:', err);
    }
  };

  // Keyboard navigation
  const handleKeyDown = useCallback((e, match, index) => {
    const cards = document.querySelectorAll('.match-card');

    switch (e.key) {
      case 'Enter':
      case ' ':
        e.preventDefault();
        setSelectedMatch(match);
        break;
      case 'ArrowDown':
        e.preventDefault();
        if (index < cards.length - 1) {
          cards[index + 1].focus();
        }
        break;
      case 'ArrowUp':
        e.preventDefault();
        if (index > 0) {
          cards[index - 1].focus();
        }
        break;
      case 'a':
        if (e.ctrlKey || e.metaKey) {
          e.preventDefault();
          handleApprove(match.id);
        }
        break;
      case 'r':
        if (e.ctrlKey || e.metaKey) {
          e.preventDefault();
          handleReject(match.id, 'other');
        }
        break;
      default:
        break;
    }
  }, [handleApprove, handleReject]);

  // Render
  return (
    <div className="match-review-queue" role="main" aria-label="Match Review Queue">
      {/* Screen reader announcer */}
      <div
        ref={announcerRef}
        role="status"
        aria-live="assertive"
        className="sr-only"
      />

      {/* Header */}
      <header className="queue-header">
        <h1>Match Review Queue</h1>
        {stats && (
          <MatchStats
            pending={stats.pending_count}
            escalated={stats.escalated_count}
            approvedToday={stats.approved_today}
            rejectedToday={stats.rejected_today}
            thresholdExceeded={stats.threshold_exceeded}
          />
        )}
      </header>

      {/* Filters */}
      <div className="queue-filters" role="search" aria-label="Filter matches">
        <select
          value={filter.status}
          onChange={(e) => setFilter(prev => ({ ...prev, status: e.target.value, offset: 0 }))}
          aria-label="Filter by status"
        >
          <option value="pending">Pending Review</option>
          <option value="approved">Approved</option>
          <option value="rejected">Rejected</option>
          <option value="all">All</option>
        </select>

        <select
          value={filter.entity_type || ''}
          onChange={(e) => setFilter(prev => ({ ...prev, entity_type: e.target.value || null, offset: 0 }))}
          aria-label="Filter by type"
        >
          <option value="">All Types</option>
          <option value="comic">Comics</option>
          <option value="funko">Funkos</option>
        </select>

        <label className="checkbox-label">
          <input
            type="checkbox"
            checked={filter.escalated_only}
            onChange={(e) => setFilter(prev => ({ ...prev, escalated_only: e.target.checked, offset: 0 }))}
          />
          Escalated Only
        </label>

        <button
          onClick={handleBulkApprove}
          className="btn-bulk-approve"
          disabled={!matches.some(m => m.can_bulk_approve)}
          aria-label="Bulk approve matches with score 8 or higher"
        >
          Bulk Approve (8+)
        </button>
      </div>

      {/* Bulk Actions Bar */}
      {selectedIds.size > 0 && (
        <div className="bulk-actions-bar" role="toolbar" aria-label="Bulk actions">
          <span className="selection-count">
            {selectedIds.size} item{selectedIds.size !== 1 ? 's' : ''} selected
          </span>
          <div className="bulk-actions-buttons">
            <button
              onClick={() => setShowBulkRejectModal(true)}
              className="btn-bulk-reject"
              aria-label={`Reject ${selectedIds.size} selected matches`}
            >
              Reject Selected
            </button>
            <button
              onClick={deselectAll}
              className="btn-deselect"
              aria-label="Clear selection"
            >
              Clear Selection
            </button>
          </div>
        </div>
      )}

      {/* Error state */}
      {error && (
        <div role="alert" className="queue-error">
          {error}
          <button onClick={fetchQueue}>Retry</button>
        </div>
      )}

      {/* Loading state */}
      {loading && (
        <div role="status" aria-label="Loading matches" className="queue-loading">
          Loading matches...
        </div>
      )}

      {/* Empty state */}
      {!loading && matches.length === 0 && (
        <div role="status" className="queue-empty">
          No matches pending review
        </div>
      )}

      {/* Match list */}
      {!loading && matches.length > 0 && (
        <>
          {/* Select All header */}
          <div className="match-list-header">
            <label className="select-all-label">
              <input
                type="checkbox"
                checked={selectedIds.size === matches.length && matches.length > 0}
                onChange={(e) => e.target.checked ? selectAll() : deselectAll()}
                aria-label="Select all matches"
              />
              Select All
            </label>
          </div>

          <div
            role="list"
            aria-label={`${matches.length} matches pending review`}
            className="match-list"
          >
            {matches.map((match, index) => (
              <MatchCard
                key={match.id}
                match={match}
                ref={index === 0 ? firstCardRef : null}
                onSelect={() => setSelectedMatch(match)}
                onApprove={() => handleApprove(match.id)}
                onReject={(reason) => handleReject(match.id, reason)}
                onManualSearch={() => {
                  setManualSearchEntity({ type: match.entity.type, id: match.entity.id });
                  setShowManualSearch(true);
                }}
                onKeyDown={(e) => handleKeyDown(e, match, index)}
                isProcessing={processing.has(match.id)}
                isSelected={selectedIds.has(match.id)}
                onToggleSelect={() => toggleSelect(match.id)}
                tabIndex={0}
              />
            ))}
          </div>

          {/* Pagination */}
          <div className="queue-pagination" role="navigation" aria-label="Pagination">
            <button
              onClick={() => setFilter(prev => ({ ...prev, offset: Math.max(0, prev.offset - prev.limit) }))}
              disabled={filter.offset === 0}
              aria-label="Previous page"
            >
              Previous
            </button>
            <span>
              Showing {filter.offset + 1} - {Math.min(filter.offset + filter.limit, total)} of {total}
            </span>
            <button
              onClick={() => setFilter(prev => ({ ...prev, offset: prev.offset + prev.limit }))}
              disabled={filter.offset + filter.limit >= total}
              aria-label="Next page"
            >
              Next
            </button>
          </div>
        </>
      )}

      {/* Detail modal */}
      {selectedMatch && (
        <MatchComparison
          match={selectedMatch}
          onClose={() => setSelectedMatch(null)}
          onApprove={() => handleApprove(selectedMatch.id)}
          onReject={(reason) => handleReject(selectedMatch.id, reason)}
          onSkip={() => handleSkip(selectedMatch.id)}
          isProcessing={processing.has(selectedMatch.id)}
        />
      )}

      {/* Manual search modal */}
      {showManualSearch && manualSearchEntity && (
        <ManualSearchModal
          entityType={manualSearchEntity.type}
          entityId={manualSearchEntity.id}
          onLink={(pcId) => handleManualLink(manualSearchEntity.type, manualSearchEntity.id, pcId)}
          onClose={() => {
            setShowManualSearch(false);
            setManualSearchEntity(null);
          }}
        />
      )}

      {/* Bulk Reject Modal */}
      {showBulkRejectModal && (
        <div className="modal-overlay" role="dialog" aria-modal="true" aria-labelledby="bulk-reject-title">
          <div className="modal-content bulk-reject-modal">
            <h2 id="bulk-reject-title">Reject {selectedIds.size} Match{selectedIds.size !== 1 ? 'es' : ''}</h2>

            <div className="form-group">
              <label htmlFor="bulk-reject-reason">Rejection Reason</label>
              <select
                id="bulk-reject-reason"
                value={bulkRejectReason}
                onChange={(e) => setBulkRejectReason(e.target.value)}
              >
                <option value="wrong_match">Wrong Match</option>
                <option value="poor_data">Poor Data Quality</option>
                <option value="duplicate">Duplicate</option>
                <option value="not_found">Not Found on PriceCharting</option>
                <option value="other">Other</option>
              </select>
            </div>

            <div className="form-group">
              <label htmlFor="bulk-reject-notes">Notes (Optional)</label>
              <textarea
                id="bulk-reject-notes"
                value={bulkRejectNotes}
                onChange={(e) => setBulkRejectNotes(e.target.value)}
                placeholder="Add any additional notes..."
                rows={3}
              />
            </div>

            <div className="modal-actions">
              <button
                onClick={() => setShowBulkRejectModal(false)}
                className="btn-cancel"
                disabled={bulkProcessing}
              >
                Cancel
              </button>
              <button
                onClick={handleBulkReject}
                className="btn-reject"
                disabled={bulkProcessing}
              >
                {bulkProcessing ? 'Rejecting...' : `Reject ${selectedIds.size} Match${selectedIds.size !== 1 ? 'es' : ''}`}
              </button>
            </div>
          </div>
        </div>
      )}

      {/* Keyboard shortcuts help */}
      <footer className="queue-shortcuts" aria-label="Keyboard shortcuts">
        <span><kbd>Enter</kbd> Open details</span>
        <span><kbd>Ctrl+A</kbd> Approve</span>
        <span><kbd>Ctrl+R</kbd> Reject</span>
        <span><kbd>Arrow keys</kbd> Navigate</span>
      </footer>
    </div>
  );
};

export default MatchReviewQueue;
