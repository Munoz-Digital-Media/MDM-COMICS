/**
 * MatchComparison - Review modal for cover uploads
 *
 * Layout:
 * - Header: Title (from filename) + Match Score badge
 * - Left: Cover image prominently displayed
 * - Right: Editable fields for pricing data
 * - Bottom: Action buttons
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
  const [showRejectForm, setShowRejectForm] = useState(false);
  const [coverError, setCoverError] = useState(false);
  const modalRef = useRef(null);
  const closeButtonRef = useRef(null);

  // Editable fields
  const [editedData, setEditedData] = useState({
    upc: '',
    isbn: '',
    price_loose: '',
    price_graded: '',
  });

  const { entity, candidate, match_method, match_score } = match;

  // Use direct S3 URL from entity, with backend endpoint as fallback
  const coverUrl = entity.cover_image_url || `/api/admin/match-queue/cover/${match.id}`;

  // Reset cover error state when match changes
  useEffect(() => {
    setCoverError(false);
  }, [match.id]);

  // Initialize editable fields from entity/candidate data
  useEffect(() => {
    setEditedData({
      upc: entity.upc || '',
      isbn: entity.isbn || '',
      price_loose: candidate.price_loose ? candidate.price_loose.toFixed(2) : '',
      price_graded: candidate.price_graded ? candidate.price_graded.toFixed(2) : '',
    });
  }, [entity, candidate]);

  // Focus trap and escape key
  useEffect(() => {
    const handleKeyDown = (e) => {
      if (e.key === 'Escape') {
        onClose();
      } else if (e.key === 'Enter' && !showRejectForm) {
        e.preventDefault();
        onApprove();
      }
    };

    if (closeButtonRef.current) {
      closeButtonRef.current.focus();
    }

    document.addEventListener('keydown', handleKeyDown);
    document.body.style.overflow = 'hidden';

    return () => {
      document.removeEventListener('keydown', handleKeyDown);
      document.body.style.overflow = 'auto';
    };
  }, [onClose, onApprove, showRejectForm]);

  const handleFieldChange = (field, value) => {
    setEditedData(prev => ({ ...prev, [field]: value }));
  };

  const handleReject = () => {
    if (!rejectReason) {
      setShowRejectForm(true);
      return;
    }
    onReject(rejectReason);
  };

  const handleConfirmReject = () => {
    if (rejectReason) {
      onReject(rejectReason);
    }
  };

  // Get score color
  const getScoreColor = (score) => {
    if (score >= 8) return 'bg-green-500';
    if (score >= 5) return 'bg-yellow-500';
    return 'bg-red-500';
  };

  return (
    <div
      className="fixed inset-0 bg-black/80 flex items-center justify-center z-50 p-4"
      role="dialog"
      aria-modal="true"
      aria-labelledby="modal-title"
      onClick={(e) => e.target === e.currentTarget && onClose()}
    >
      <div
        className="bg-zinc-900 border border-zinc-700 rounded-xl max-w-4xl w-full max-h-[90vh] overflow-hidden flex flex-col"
        ref={modalRef}
      >
        {/* Header: Title + Score */}
        <header className="flex items-center justify-between px-6 py-4 border-b border-zinc-700">
          <div className="flex items-center gap-4">
            <h2 id="modal-title" className="text-xl font-bold text-white">
              {entity.name}
            </h2>
            <span className="text-zinc-500 text-sm">
              {match_method}
            </span>
          </div>
          <div className="flex items-center gap-4">
            <div className={`px-3 py-1 rounded-full ${getScoreColor(match_score)} text-white font-bold`}>
              {match_score ?? '?'} / 10
            </div>
            <button
              ref={closeButtonRef}
              onClick={onClose}
              className="text-zinc-400 hover:text-white text-2xl leading-none"
              aria-label="Close"
            >
              ×
            </button>
          </div>
        </header>

        {/* Main content: Cover left, Fields right */}
        <div className="flex-1 overflow-y-auto p-6">
          <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
            {/* Left: Cover Image */}
            <div className="flex flex-col">
              <h3 className="text-sm font-medium text-zinc-400 mb-3 uppercase tracking-wide">
                Cover Image
              </h3>
              <div className="bg-zinc-800 rounded-lg overflow-hidden flex-1 flex items-center justify-center min-h-[400px]">
                {!coverError ? (
                  <img
                    src={coverUrl}
                    alt={`Cover for ${entity.name}`}
                    className="max-w-full max-h-[500px] object-contain"
                    onError={() => setCoverError(true)}
                  />
                ) : (
                  <div className="text-zinc-600 text-center p-8">
                    <svg className="w-16 h-16 mx-auto mb-3 opacity-50" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                      <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.5} d="M4 16l4.586-4.586a2 2 0 012.828 0L16 16m-2-2l1.586-1.586a2 2 0 012.828 0L20 14m-6-6h.01M6 20h12a2 2 0 002-2V6a2 2 0 00-2-2H6a2 2 0 00-2 2v12a2 2 0 002 2z" />
                    </svg>
                    <p>No cover image available</p>
                  </div>
                )}
              </div>

              {/* Metadata below image */}
              {entity.publisher && (
                <div className="mt-4 grid grid-cols-2 gap-2 text-sm">
                  <div>
                    <span className="text-zinc-500">Publisher:</span>
                    <span className="text-white ml-2">{entity.publisher}</span>
                  </div>
                  {entity.series_name && (
                    <div>
                      <span className="text-zinc-500">Series:</span>
                      <span className="text-white ml-2">{entity.series_name}</span>
                    </div>
                  )}
                  {entity.issue_number && (
                    <div>
                      <span className="text-zinc-500">Issue:</span>
                      <span className="text-white ml-2">#{entity.issue_number}</span>
                    </div>
                  )}
                  {entity.year && (
                    <div>
                      <span className="text-zinc-500">Year:</span>
                      <span className="text-white ml-2">{entity.year}</span>
                    </div>
                  )}
                </div>
              )}
            </div>

            {/* Right: Editable Fields */}
            <div className="flex flex-col">
              <h3 className="text-sm font-medium text-zinc-400 mb-3 uppercase tracking-wide">
                Product Data
              </h3>

              <div className="space-y-4">
                {/* UPC */}
                <div>
                  <label htmlFor="upc" className="block text-sm text-zinc-400 mb-1">
                    UPC
                  </label>
                  <input
                    id="upc"
                    type="text"
                    value={editedData.upc}
                    onChange={(e) => handleFieldChange('upc', e.target.value)}
                    placeholder="Enter UPC barcode"
                    className="w-full bg-zinc-800 border border-zinc-700 rounded-lg px-4 py-2.5 text-white placeholder-zinc-500 focus:outline-none focus:border-orange-500 font-mono"
                  />
                </div>

                {/* ISBN */}
                <div>
                  <label htmlFor="isbn" className="block text-sm text-zinc-400 mb-1">
                    ISBN
                  </label>
                  <input
                    id="isbn"
                    type="text"
                    value={editedData.isbn}
                    onChange={(e) => handleFieldChange('isbn', e.target.value)}
                    placeholder="Enter ISBN"
                    className="w-full bg-zinc-800 border border-zinc-700 rounded-lg px-4 py-2.5 text-white placeholder-zinc-500 focus:outline-none focus:border-orange-500 font-mono"
                  />
                </div>

                {/* Divider */}
                <hr className="border-zinc-700 my-2" />

                {/* Pricing Section */}
                <h4 className="text-sm font-medium text-zinc-400 uppercase tracking-wide">
                  Pricing
                </h4>

                {/* Loose Price */}
                <div>
                  <label htmlFor="price_loose" className="block text-sm text-zinc-400 mb-1">
                    Raw / Loose Price
                  </label>
                  <div className="relative">
                    <span className="absolute left-3 top-1/2 -translate-y-1/2 text-zinc-500">$</span>
                    <input
                      id="price_loose"
                      type="text"
                      value={editedData.price_loose}
                      onChange={(e) => handleFieldChange('price_loose', e.target.value)}
                      placeholder="0.00"
                      className="w-full bg-zinc-800 border border-zinc-700 rounded-lg pl-8 pr-4 py-2.5 text-white placeholder-zinc-500 focus:outline-none focus:border-orange-500 font-mono"
                    />
                  </div>
                </div>

                {/* Graded Price */}
                <div>
                  <label htmlFor="price_graded" className="block text-sm text-zinc-400 mb-1">
                    9.8 / Near Mint Price
                  </label>
                  <div className="relative">
                    <span className="absolute left-3 top-1/2 -translate-y-1/2 text-zinc-500">$</span>
                    <input
                      id="price_graded"
                      type="text"
                      value={editedData.price_graded}
                      onChange={(e) => handleFieldChange('price_graded', e.target.value)}
                      placeholder="0.00"
                      className="w-full bg-zinc-800 border border-zinc-700 rounded-lg pl-8 pr-4 py-2.5 text-white placeholder-zinc-500 focus:outline-none focus:border-orange-500 font-mono"
                    />
                  </div>
                  <p className="text-xs text-zinc-600 mt-1">Auto-sourced from PriceCharting when available</p>
                </div>

                {/* External link if available */}
                {candidate.url && (
                  <a
                    href={candidate.url}
                    target="_blank"
                    rel="noopener noreferrer"
                    className="inline-flex items-center gap-2 text-orange-400 hover:text-orange-300 text-sm mt-2"
                  >
                    View on PriceCharting
                    <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                      <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M10 6H6a2 2 0 00-2 2v10a2 2 0 002 2h10a2 2 0 002-2v-4M14 4h6m0 0v6m0-6L10 14" />
                    </svg>
                  </a>
                )}
              </div>
            </div>
          </div>
        </div>

        {/* Reject reason form */}
        {showRejectForm && (
          <div className="px-6 py-4 border-t border-zinc-700 bg-zinc-800/50">
            <div className="flex items-center gap-4">
              <select
                value={rejectReason}
                onChange={(e) => setRejectReason(e.target.value)}
                className="flex-1 bg-zinc-800 border border-zinc-700 rounded-lg px-4 py-2 text-white focus:outline-none focus:border-orange-500"
              >
                <option value="">Select rejection reason...</option>
                <option value="wrong_item">Wrong Item</option>
                <option value="wrong_variant">Wrong Variant</option>
                <option value="wrong_year">Wrong Year</option>
                <option value="duplicate">Duplicate</option>
                <option value="other">Other</option>
              </select>
              <button
                onClick={handleConfirmReject}
                disabled={!rejectReason || isProcessing}
                className="px-4 py-2 bg-red-600 hover:bg-red-700 disabled:bg-red-600/50 text-white rounded-lg font-medium"
              >
                Confirm Reject
              </button>
              <button
                onClick={() => setShowRejectForm(false)}
                className="px-4 py-2 bg-zinc-700 hover:bg-zinc-600 text-white rounded-lg"
              >
                Cancel
              </button>
            </div>
          </div>
        )}

        {/* Action buttons */}
        <footer className="flex items-center justify-between px-6 py-4 border-t border-zinc-700 bg-zinc-800/30">
          <div className="text-xs text-zinc-500">
            <kbd className="px-1.5 py-0.5 bg-zinc-700 rounded text-zinc-400">Esc</kbd> Close
            <span className="mx-3">|</span>
            <kbd className="px-1.5 py-0.5 bg-zinc-700 rounded text-zinc-400">Enter</kbd> Approve
          </div>

          <div className="flex items-center gap-3">
            <button
              onClick={onSkip}
              disabled={isProcessing}
              className="px-5 py-2.5 bg-zinc-700 hover:bg-zinc-600 disabled:bg-zinc-700/50 text-white rounded-lg font-medium transition-colors"
            >
              Skip for Later
            </button>
            <button
              onClick={handleReject}
              disabled={isProcessing}
              className="px-5 py-2.5 bg-red-600 hover:bg-red-700 disabled:bg-red-600/50 text-white rounded-lg font-medium transition-colors"
            >
              Reject
            </button>
            <button
              onClick={onApprove}
              disabled={isProcessing}
              className="px-5 py-2.5 bg-orange-500 hover:bg-orange-600 disabled:bg-orange-500/50 text-white rounded-lg font-medium transition-colors"
            >
              {isProcessing ? 'Processing...' : 'Approve Match'}
            </button>
          </div>
        </footer>
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
      price_graded: PropTypes.number,
      url: PropTypes.string
    }).isRequired,
    match_method: PropTypes.string.isRequired,
    match_score: PropTypes.number,
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
