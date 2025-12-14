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
import { api } from '../../../services/api';
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

  // Refs for focus management
  const firstCardRef = useRef(null);
  const announcerRef = useRef(null);

  // Fetch queue data
  const fetchQueue = useCallback(async () => {
    try {
      setLoading(true);
      setError(null);

      const response = await api.post('/api/admin/match-queue', filter);
      setMatches(response.data.items || []);
      setTotal(response.data.total || 0);
      setStats({
        pending_count: response.data.pending_count,
        escalated_count: response.data.escalated_count
      });
    } catch (err) {
      setError(err.response?.data?.detail || 'Failed to load queue');
      console.error('Queue fetch error:', err);
    } finally {
      setLoading(false);
    }
  }, [filter]);

  // Fetch stats
  const fetchStats = useCallback(async () => {
    try {
      const response = await api.get('/api/admin/match-queue/stats');
      setStats(response.data);
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

  // Actions
  const handleApprove = async (matchId) => {
    if (processing.has(matchId)) return;

    setProcessing(prev => new Set([...prev, matchId]));
    try {
      await api.post(`/api/admin/match-queue/${matchId}/approve`, {});
      announce('Match approved successfully');
      setMatches(prev => prev.filter(m => m.id !== matchId));
      setSelectedMatch(null);
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
      await api.post(`/api/admin/match-queue/${matchId}/reject`, { reason });
      announce('Match rejected');
      setMatches(prev => prev.filter(m => m.id !== matchId));
      setSelectedMatch(null);
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
    await api.post(`/api/admin/match-queue/${matchId}/skip`, {});
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
      const response = await api.post('/api/admin/match-queue/bulk-approve', {
        match_ids: eligibleIds
      });
      announce(`Approved ${response.data.approved_count} matches`);
      fetchQueue();
      fetchStats();
    } catch (err) {
      announce('Bulk approve failed');
      console.error('Bulk approve error:', err);
    }
  };

  const handleManualLink = async (entityType, entityId, pricechartingId) => {
    try {
      await api.post('/api/admin/match-queue/manual-link', {
        entity_type: entityType,
        entity_id: entityId,
        pricecharting_id: pricechartingId
      });
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
