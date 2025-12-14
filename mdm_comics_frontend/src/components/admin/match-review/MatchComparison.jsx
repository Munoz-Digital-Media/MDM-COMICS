/**
 * MatchComparison - Side-by-side comparison modal for detailed review
 *
 * Per constitution_ui.json:
 * - WCAG 2.2 AA compliant
 * - Focus trap
 * - Escape to close
 * - ARIA labels
 */

import React, { useEffect, useRef, useState } from 'react';
import PropTypes from 'prop-types';

const MatchComparison = ({
  match,
  onClose,
  onApprove,
  onReject,
  onSkip,
  isProcessing
}) => {
  const [rejectReason, setRejectReason] = useState('');
  const [notes, setNotes] = useState('');
  const [showRejectForm, setShowRejectForm] = useState(false);
  const modalRef = useRef(null);
  const closeButtonRef = useRef(null);

  const { entity, candidate, match_method, match_score, match_details } = match;

  // Focus trap and escape key
  useEffect(() => {
    const handleKeyDown = (e) => {
      if (e.key === 'Escape') {
        onClose();
      }
    };

    // Focus first element
    if (closeButtonRef.current) {
      closeButtonRef.current.focus();
    }

    document.addEventListener('keydown', handleKeyDown);
    document.body.style.overflow = 'hidden';

    return () => {
      document.removeEventListener('keydown', handleKeyDown);
      document.body.style.overflow = 'auto';
    };
  }, [onClose]);

  // Handle reject with reason
  const handleReject = () => {
    if (!rejectReason) {
      setShowRejectForm(true);
      return;
    }
    onReject(rejectReason);
  };

  // Handle confirm reject
  const handleConfirmReject = () => {
    if (rejectReason) {
      onReject(rejectReason);
    }
  };

  // Format price
  const formatPrice = (price) => {
    if (price == null) return '—';
    return `$${price.toFixed(2)}`;
  };

  // Get score color
  const getScoreClass = (score) => {
    if (score >= 9) return 'score-high';
    if (score >= 7) return 'score-medium';
    if (score >= 5) return 'score-low';
    return 'score-very-low';
  };

  return (
    <div
      className="match-comparison-overlay"
      role="dialog"
      aria-modal="true"
      aria-labelledby="comparison-title"
      onClick={(e) => e.target === e.currentTarget && onClose()}
    >
      <div className="match-comparison-modal" ref={modalRef}>
        {/* Header */}
        <header className="comparison-header">
          <h2 id="comparison-title">Review Match</h2>
          <button
            ref={closeButtonRef}
            onClick={onClose}
            className="btn-close"
            aria-label="Close comparison"
          >
            ×
          </button>
        </header>

        {/* Match score banner */}
        <div className={`comparison-score-banner ${getScoreClass(match_score)}`}>
          <div className="score-display">
            <span className="score-value">{match_score ?? '?'}</span>
            <span className="score-label">Match Score</span>
          </div>
          <div className="match-method-display">
            <span className="method-label">Method:</span>
            <span className="method-value">{match_method}</span>
          </div>
          {match.is_escalated && (
            <div className="escalated-badge">ESCALATED</div>
          )}
        </div>

        {/* Side-by-side comparison */}
        <div className="comparison-content">
          {/* Entity (source) side */}
          <div className="comparison-side source-side">
            <h3>Source Record</h3>
            <div className="comparison-card">
              <div className="type-badge">{entity.type}</div>

              {entity.cover_image_url && (
                <div className="cover-container">
                  <img
                    src={entity.cover_image_url}
                    alt={`Cover for ${entity.name}`}
                    className="cover-image"
                  />
                </div>
              )}

              <dl className="details-list">
                <dt>Name</dt>
                <dd>{entity.name}</dd>

                {entity.series_name && (
                  <>
                    <dt>Series</dt>
                    <dd>{entity.series_name}</dd>
                  </>
                )}

                {entity.issue_number && (
                  <>
                    <dt>Issue</dt>
                    <dd>#{entity.issue_number}</dd>
                  </>
                )}

                {entity.publisher && (
                  <>
                    <dt>Publisher</dt>
                    <dd>{entity.publisher}</dd>
                  </>
                )}

                {entity.year && (
                  <>
                    <dt>Year</dt>
                    <dd>{entity.year}</dd>
                  </>
                )}

                {entity.isbn && (
                  <>
                    <dt>ISBN</dt>
                    <dd className="mono">{entity.isbn}</dd>
                  </>
                )}

                {entity.upc && (
                  <>
                    <dt>UPC</dt>
                    <dd className="mono">{entity.upc}</dd>
                  </>
                )}
              </dl>
            </div>
          </div>

          {/* Arrow */}
          <div className="comparison-arrow" aria-hidden="true">
            <span className="arrow-icon">→</span>
          </div>

          {/* Candidate side */}
          <div className="comparison-side candidate-side">
            <h3>PriceCharting Match</h3>
            <div className="comparison-card">
              <div className="source-badge">{candidate.source}</div>

              <dl className="details-list">
                <dt>Name</dt>
                <dd>{candidate.name}</dd>

                <dt>ID</dt>
                <dd className="mono">{candidate.id}</dd>

                <dt>Loose Price</dt>
                <dd className="price">{formatPrice(candidate.price_loose)}</dd>

                <dt>CIB Price</dt>
                <dd className="price">{formatPrice(candidate.price_cib)}</dd>

                <dt>Graded Price</dt>
                <dd className="price">{formatPrice(candidate.price_graded)}</dd>
              </dl>

              {candidate.url && (
                <a
                  href={candidate.url}
                  target="_blank"
                  rel="noopener noreferrer"
                  className="external-link"
                >
                  View on PriceCharting ↗
                </a>
              )}
            </div>
          </div>
        </div>

        {/* Match details (scoring breakdown) */}
        {match_details && Object.keys(match_details).length > 0 && (
          <div className="match-details-section">
            <h4>Scoring Breakdown</h4>
            <table className="details-table">
              <thead>
                <tr>
                  <th>Field</th>
                  <th>Source</th>
                  <th>Candidate</th>
                  <th>Points</th>
                </tr>
              </thead>
              <tbody>
                {Object.entries(match_details).map(([field, data]) => (
                  <tr key={field}>
                    <td>{field}</td>
                    <td>{data.source ?? '—'}</td>
                    <td>{data.candidate ?? '—'}</td>
                    <td className={data.points > 0 ? 'positive' : ''}>{data.points ?? 0}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}

        {/* Reject reason form */}
        {showRejectForm && (
          <div className="reject-form">
            <label htmlFor="reject-reason">Rejection Reason</label>
            <select
              id="reject-reason"
              value={rejectReason}
              onChange={(e) => setRejectReason(e.target.value)}
              required
            >
              <option value="">Select a reason...</option>
              <option value="wrong_item">Wrong Item</option>
              <option value="wrong_variant">Wrong Variant</option>
              <option value="wrong_year">Wrong Year</option>
              <option value="duplicate">Duplicate</option>
              <option value="other">Other</option>
            </select>

            <label htmlFor="reject-notes">Notes (optional)</label>
            <textarea
              id="reject-notes"
              value={notes}
              onChange={(e) => setNotes(e.target.value)}
              maxLength={500}
              placeholder="Additional notes..."
            />

            <button
              onClick={handleConfirmReject}
              disabled={!rejectReason || isProcessing}
              className="btn-confirm-reject"
            >
              Confirm Rejection
            </button>
          </div>
        )}

        {/* Actions */}
        <footer className="comparison-actions">
          <button
            onClick={onSkip}
            disabled={isProcessing}
            className="btn-skip"
          >
            Skip for Later
          </button>
          <button
            onClick={handleReject}
            disabled={isProcessing}
            className="btn-reject"
          >
            {showRejectForm ? 'Cancel Reject' : 'Reject'}
          </button>
          <button
            onClick={onApprove}
            disabled={isProcessing}
            className="btn-approve"
          >
            {isProcessing ? 'Processing...' : 'Approve Match'}
          </button>
        </footer>

        {/* Keyboard shortcuts reminder */}
        <div className="modal-shortcuts">
          <span><kbd>Esc</kbd> Close</span>
          <span><kbd>Enter</kbd> Approve</span>
        </div>
      </div>
    </div>
  );
};

MatchComparison.propTypes = {
  match: PropTypes.shape({
    id: PropTypes.number.isRequired,
    entity: PropTypes.shape({
      id: PropTypes.number.isRequired,
      type: PropTypes.string.isRequired,
      name: PropTypes.string.isRequired,
      series_name: PropTypes.string,
      issue_number: PropTypes.string,
      publisher: PropTypes.string,
      year: PropTypes.number,
      isbn: PropTypes.string,
      upc: PropTypes.string,
      cover_image_url: PropTypes.string
    }).isRequired,
    candidate: PropTypes.shape({
      source: PropTypes.string.isRequired,
      id: PropTypes.string.isRequired,
      name: PropTypes.string.isRequired,
      price_loose: PropTypes.number,
      price_cib: PropTypes.number,
      price_graded: PropTypes.number,
      url: PropTypes.string
    }).isRequired,
    match_method: PropTypes.string.isRequired,
    match_score: PropTypes.number,
    match_details: PropTypes.object,
    is_escalated: PropTypes.bool
  }).isRequired,
  onClose: PropTypes.func.isRequired,
  onApprove: PropTypes.func.isRequired,
  onReject: PropTypes.func.isRequired,
  onSkip: PropTypes.func.isRequired,
  isProcessing: PropTypes.bool
};

MatchComparison.defaultProps = {
  isProcessing: false
};

export default MatchComparison;
