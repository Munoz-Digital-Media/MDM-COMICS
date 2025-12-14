/**
 * MatchStats - Queue statistics display
 *
 * Per constitution_ui.json:
 * - WCAG 2.2 AA compliant
 * - ARIA labels
 */

import React from 'react';
import PropTypes from 'prop-types';

const MatchStats = ({
  pending,
  escalated,
  approvedToday,
  rejectedToday,
  thresholdExceeded
}) => {
  return (
    <div
      className="match-stats"
      role="region"
      aria-label="Queue statistics"
    >
      {/* Pending count */}
      <div className={`stat-item ${thresholdExceeded ? 'warning' : ''}`}>
        <span className="stat-value">{pending ?? 0}</span>
        <span className="stat-label">Pending</span>
        {thresholdExceeded && (
          <span className="stat-warning" role="alert" aria-label="Threshold exceeded">
            ⚠️
          </span>
        )}
      </div>

      {/* Escalated count */}
      <div className={`stat-item ${escalated > 0 ? 'urgent' : ''}`}>
        <span className="stat-value">{escalated ?? 0}</span>
        <span className="stat-label">Escalated</span>
      </div>

      {/* Today's activity */}
      <div className="stat-item approved">
        <span className="stat-value">{approvedToday ?? 0}</span>
        <span className="stat-label">Approved Today</span>
      </div>

      <div className="stat-item rejected">
        <span className="stat-value">{rejectedToday ?? 0}</span>
        <span className="stat-label">Rejected Today</span>
      </div>
    </div>
  );
};

MatchStats.propTypes = {
  pending: PropTypes.number,
  escalated: PropTypes.number,
  approvedToday: PropTypes.number,
  rejectedToday: PropTypes.number,
  thresholdExceeded: PropTypes.bool
};

MatchStats.defaultProps = {
  pending: 0,
  escalated: 0,
  approvedToday: 0,
  rejectedToday: 0,
  thresholdExceeded: false
};

export default MatchStats;
