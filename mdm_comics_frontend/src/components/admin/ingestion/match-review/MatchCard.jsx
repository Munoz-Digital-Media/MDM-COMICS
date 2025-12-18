/**
 * MatchCard - Individual match item in the review queue
 *
 * Per constitution_ui.json:
 * - WCAG 2.2 AA compliant
 * - Full keyboard navigation
 * - ARIA labels
 */

import React, { forwardRef } from 'react';
import PropTypes from 'prop-types';

const MatchCard = forwardRef(({
  match,
  onSelect,
  onApprove,
  onReject,
  onManualSearch,
  onKeyDown,
  isProcessing,
  tabIndex,
  isSelected,
  onToggleSelect
}, ref) => {
  const { entity, candidate, match_method, match_score, is_escalated, can_bulk_approve } = match;

  // Score color coding
  const getScoreClass = (score) => {
    if (score >= 9) return 'score-high';
    if (score >= 7) return 'score-medium';
    if (score >= 5) return 'score-low';
    return 'score-very-low';
  };

  // Format price
  const formatPrice = (price) => {
    if (price == null) return '—';
    return `$${price.toFixed(2)}`;
  };

  // Handle checkbox click
  const handleCheckboxClick = (e) => {
    e.stopPropagation();
    if (onToggleSelect) {
      onToggleSelect();
    }
  };

  // Handle approve click
  const handleApprove = (e) => {
    e.stopPropagation();
    if (!isProcessing) {
      onApprove();
    }
  };

  // Handle reject click
  const handleReject = (e) => {
    e.stopPropagation();
    if (!isProcessing) {
      onReject('other');
    }
  };

  // Handle manual search click
  const handleManualSearch = (e) => {
    e.stopPropagation();
    onManualSearch();
  };

  return (
    <div
      ref={ref}
      role="listitem"
      className={`match-card ${is_escalated ? 'escalated' : ''} ${isProcessing ? 'processing' : ''} ${isSelected ? 'selected' : ''}`}
      onClick={onSelect}
      onKeyDown={onKeyDown}
      tabIndex={tabIndex}
      aria-label={`Match: ${entity.name} to ${candidate.name}, score ${match_score}`}
    >
      {/* Escalation indicator */}
      {is_escalated && (
        <div className="escalation-badge" aria-label="Escalated - requires urgent review">
          ESCALATED
        </div>
      )}

      {/* Checkbox for bulk selection */}
      {onToggleSelect && (
        <div className="card-checkbox-container">
          <input
            type="checkbox"
            checked={isSelected}
            onChange={handleCheckboxClick}
            onClick={handleCheckboxClick}
            aria-label={`Select match ${match.id}`}
            className="card-checkbox"
          />
        </div>
      )}

      {/* Entity info (left side) */}
      <div className="card-entity">
        <div className="entity-badges">
          <span className="entity-type-badge">{entity.type}</span>
          <span className={`score-badge ${getScoreClass(match_score)}`}>
            {match_score ?? '?'}
          </span>
        </div>
        {entity.cover_image_url && (
          <img
            src={entity.cover_image_url}
            alt={`Cover for ${entity.name}`}
            className="entity-cover"
            loading="lazy"
          />
        )}
        <div className="entity-details">
          <h3 className="entity-name">{entity.name}</h3>
          {entity.series_name && (
            <p className="entity-series">{entity.series_name}</p>
          )}
          <div className="entity-meta">
            {entity.issue_number && <span>#{entity.issue_number}</span>}
            {entity.publisher && <span>{entity.publisher}</span>}
            {entity.year && <span>{entity.year}</span>}
          </div>
          {entity.isbn && (
            <p className="entity-isbn">ISBN: {entity.isbn}</p>
          )}
          {entity.upc && (
            <p className="entity-upc">UPC: {entity.upc}</p>
          )}
        </div>
      </div>

      {/* Match arrow */}
      <div className="match-arrow" aria-hidden="true">
        <span className="match-method">{match_method}</span>
        →
      </div>

      {/* Candidate info (right side) */}
      <div className="card-candidate">
        <div className="candidate-source-badge">{candidate.source}</div>
        <div className="candidate-details">
          <h3 className="candidate-name">{candidate.name}</h3>
          <div className="candidate-prices">
            <div className="price-item">
              <span className="price-label">Loose</span>
              <span className="price-value">{formatPrice(candidate.price_loose)}</span>
            </div>
            <div className="price-item">
              <span className="price-label">CIB</span>
              <span className="price-value">{formatPrice(candidate.price_cib)}</span>
            </div>
            <div className="price-item">
              <span className="price-label">Graded</span>
              <span className="price-value">{formatPrice(candidate.price_graded)}</span>
            </div>
          </div>
          {candidate.url && (
            <a
              href={candidate.url}
              target="_blank"
              rel="noopener noreferrer"
              className="candidate-link"
              onClick={(e) => e.stopPropagation()}
            >
              View on PriceCharting
            </a>
          )}
        </div>
      </div>

      {/* Actions */}
      <div className="card-actions">
        <button
          onClick={handleApprove}
          disabled={isProcessing}
          className={`btn-approve ${can_bulk_approve ? 'bulk-eligible' : ''}`}
          aria-label={`Approve match for ${entity.name}`}
        >
          {isProcessing ? '...' : '✓'}
        </button>
        <button
          onClick={handleReject}
          disabled={isProcessing}
          className="btn-reject"
          aria-label={`Reject match for ${entity.name}`}
        >
          {isProcessing ? '...' : '✗'}
        </button>
        <button
          onClick={handleManualSearch}
          className="btn-search"
          aria-label={`Search manually for ${entity.name}`}
        >
          🔍
        </button>
      </div>
    </div>
  );
});

MatchCard.displayName = 'MatchCard';

MatchCard.propTypes = {
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
    is_escalated: PropTypes.bool,
    can_bulk_approve: PropTypes.bool
  }).isRequired,
  onSelect: PropTypes.func.isRequired,
  onApprove: PropTypes.func.isRequired,
  onReject: PropTypes.func.isRequired,
  onManualSearch: PropTypes.func.isRequired,
  onKeyDown: PropTypes.func.isRequired,
  isProcessing: PropTypes.bool,
  tabIndex: PropTypes.number,
  isSelected: PropTypes.bool,
  onToggleSelect: PropTypes.func
};

MatchCard.defaultProps = {
  isProcessing: false,
  tabIndex: 0,
  isSelected: false,
  onToggleSelect: null
};

export default MatchCard;
