/**
 * ManualSearchModal - Search PriceCharting for manual linking
 *
 * Per constitution_ui.json:
 * - WCAG 2.2 AA compliant
 * - Focus trap
 * - Keyboard navigation
 * - ARIA labels
 */

import React, { useState, useEffect, useRef, useCallback } from 'react';
import PropTypes from 'prop-types';
import { matchReviewAPI } from '../../../services/api';

const ManualSearchModal = ({
  entityType,
  entityId,
  onLink,
  onClose
}) => {
  const [query, setQuery] = useState('');
  const [results, setResults] = useState([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);
  const [selectedId, setSelectedId] = useState(null);
  const [focusedIndex, setFocusedIndex] = useState(-1);

  const inputRef = useRef(null);
  const resultsRef = useRef(null);

  // Focus search input on mount
  useEffect(() => {
    if (inputRef.current) {
      inputRef.current.focus();
    }

    const handleKeyDown = (e) => {
      if (e.key === 'Escape') {
        onClose();
      }
    };

    document.addEventListener('keydown', handleKeyDown);
    document.body.style.overflow = 'hidden';

    return () => {
      document.removeEventListener('keydown', handleKeyDown);
      document.body.style.overflow = 'auto';
    };
  }, [onClose]);

  // Debounced search
  const searchPriceCharting = useCallback(async (searchQuery) => {
    if (searchQuery.length < 2) {
      setResults([]);
      return;
    }

    setLoading(true);
    setError(null);

    try {
      const response = await matchReviewAPI.search(searchQuery, entityType);
      setResults(response.results || []);
    } catch (err) {
      setError(err.message || 'Search failed');
      setResults([]);
    } finally {
      setLoading(false);
    }
  }, [entityType]);

  // Debounce search
  useEffect(() => {
    const timer = setTimeout(() => {
      if (query.trim()) {
        searchPriceCharting(query.trim());
      }
    }, 300);

    return () => clearTimeout(timer);
  }, [query, searchPriceCharting]);

  // Keyboard navigation in results
  const handleKeyDown = (e) => {
    if (results.length === 0) return;

    switch (e.key) {
      case 'ArrowDown':
        e.preventDefault();
        setFocusedIndex(prev =>
          prev < results.length - 1 ? prev + 1 : prev
        );
        break;
      case 'ArrowUp':
        e.preventDefault();
        setFocusedIndex(prev => (prev > 0 ? prev - 1 : prev));
        break;
      case 'Enter':
        e.preventDefault();
        if (focusedIndex >= 0 && results[focusedIndex]) {
          handleSelect(results[focusedIndex].id);
        }
        break;
      default:
        break;
    }
  };

  // Select a result
  const handleSelect = (id) => {
    setSelectedId(id);
  };

  // Confirm link
  const handleConfirmLink = () => {
    if (selectedId) {
      onLink(selectedId);
    }
  };

  // Format price
  const formatPrice = (price) => {
    if (price == null) return '—';
    return `$${price.toFixed(2)}`;
  };

  return (
    <div
      className="manual-search-overlay"
      role="dialog"
      aria-modal="true"
      aria-labelledby="search-title"
      onClick={(e) => e.target === e.currentTarget && onClose()}
    >
      <div className="manual-search-modal">
        {/* Header */}
        <header className="search-header">
          <h2 id="search-title">Search PriceCharting</h2>
          <button
            onClick={onClose}
            className="btn-close"
            aria-label="Close search"
          >
            ×
          </button>
        </header>

        {/* Search input */}
        <div className="search-input-container">
          <label htmlFor="search-query" className="sr-only">
            Search for {entityType}
          </label>
          <input
            ref={inputRef}
            id="search-query"
            type="text"
            value={query}
            onChange={(e) => setQuery(e.target.value)}
            onKeyDown={handleKeyDown}
            placeholder={`Search for ${entityType}...`}
            className="search-input"
            aria-describedby="search-help"
          />
          <span id="search-help" className="search-help">
            Enter at least 2 characters to search
          </span>
        </div>

        {/* Loading state */}
        {loading && (
          <div className="search-loading" role="status">
            Searching...
          </div>
        )}

        {/* Error state */}
        {error && (
          <div className="search-error" role="alert">
            {error}
          </div>
        )}

        {/* Results */}
        {!loading && results.length > 0 && (
          <div
            ref={resultsRef}
            className="search-results"
            role="listbox"
            aria-label="Search results"
          >
            {results.map((result, index) => (
              <div
                key={result.id}
                role="option"
                aria-selected={selectedId === result.id}
                className={`search-result ${selectedId === result.id ? 'selected' : ''} ${focusedIndex === index ? 'focused' : ''}`}
                onClick={() => handleSelect(result.id)}
                tabIndex={0}
                onKeyDown={(e) => {
                  if (e.key === 'Enter' || e.key === ' ') {
                    e.preventDefault();
                    handleSelect(result.id);
                  }
                }}
              >
                <div className="result-main">
                  <h4 className="result-name">{result.name}</h4>
                  <span className="result-console">{result.console}</span>
                </div>
                <div className="result-prices">
                  <span className="result-price">
                    <span className="price-label">Loose:</span>
                    {formatPrice(result.price_loose)}
                  </span>
                  <span className="result-price">
                    <span className="price-label">CIB:</span>
                    {formatPrice(result.price_cib)}
                  </span>
                  <span className="result-price">
                    <span className="price-label">Graded:</span>
                    {formatPrice(result.price_graded)}
                  </span>
                </div>
                <div className="result-id">ID: {result.id}</div>
              </div>
            ))}
          </div>
        )}

        {/* No results */}
        {!loading && query.length >= 2 && results.length === 0 && !error && (
          <div className="search-no-results">
            No results found for "{query}"
          </div>
        )}

        {/* Actions */}
        <footer className="search-actions">
          <button
            onClick={onClose}
            className="btn-cancel"
          >
            Cancel
          </button>
          <button
            onClick={handleConfirmLink}
            disabled={!selectedId}
            className="btn-link"
          >
            Link to Selected
          </button>
        </footer>

        {/* Selection info */}
        {selectedId && (
          <div className="selection-info" role="status">
            Selected: {results.find(r => r.id === selectedId)?.name || selectedId}
          </div>
        )}
      </div>
    </div>
  );
};

ManualSearchModal.propTypes = {
  entityType: PropTypes.oneOf(['comic', 'funko']).isRequired,
  entityId: PropTypes.number.isRequired,
  onLink: PropTypes.func.isRequired,
  onClose: PropTypes.func.isRequired
};

export default ManualSearchModal;
