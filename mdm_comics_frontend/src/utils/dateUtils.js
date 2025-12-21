/**
 * Date utilities for convention event parsing
 * Handles formats like: "Jan 10-11, 2026", "Jan 31-Feb 1, 2026"
 */

const MONTH_MAP = {
  'Jan': 0, 'Feb': 1, 'Mar': 2, 'Apr': 3, 'May': 4, 'Jun': 5,
  'Jul': 6, 'Aug': 7, 'Sep': 8, 'Oct': 9, 'Nov': 10, 'Dec': 11
};

/**
 * Parse a convention date_text string into start and end Date objects
 * @param {string} dateText - e.g., "Jan 10-11, 2026" or "Jan 31-Feb 1, 2026"
 * @returns {{ startDate: Date, endDate: Date } | null}
 */
export function parseDateText(dateText) {
  if (!dateText || typeof dateText !== 'string') return null;

  try {
    // Extract year from end of string
    const yearMatch = dateText.match(/,?\s*(\d{4})$/);
    if (!yearMatch) return null;
    const year = parseInt(yearMatch[1], 10);

    // Remove year and trim
    const datePart = dateText.replace(/,?\s*\d{4}$/, '').trim();

    // Split by hyphen to get start and end
    const parts = datePart.split('-').map(p => p.trim());
    if (parts.length !== 2) return null;

    // Parse start: "Jan 10" or "Jan 31"
    const startMatch = parts[0].match(/^([A-Za-z]{3})\s*(\d{1,2})$/);
    if (!startMatch) return null;
    const startMonth = MONTH_MAP[startMatch[1]];
    const startDay = parseInt(startMatch[2], 10);
    if (startMonth === undefined) return null;

    // Parse end: "11" (same month) or "Feb 1" (different month)
    let endMonth, endDay;
    const endMatch = parts[1].match(/^([A-Za-z]{3})?\s*(\d{1,2})$/);
    if (!endMatch) return null;

    if (endMatch[1]) {
      // Different month: "Feb 1"
      endMonth = MONTH_MAP[endMatch[1]];
      if (endMonth === undefined) return null;
    } else {
      // Same month: just day number
      endMonth = startMonth;
    }
    endDay = parseInt(endMatch[2], 10);

    const startDate = new Date(year, startMonth, startDay);
    const endDate = new Date(year, endMonth, endDay, 23, 59, 59); // End of day

    return { startDate, endDate };
  } catch (e) {
    console.warn('Failed to parse date:', dateText, e);
    return null;
  }
}

/**
 * Check if an event is in the future (end date >= today)
 * @param {Date} endDate
 * @returns {boolean}
 */
export function isFutureEvent(endDate) {
  if (!endDate || !(endDate instanceof Date) || isNaN(endDate.getTime())) {
    return true; // Fail-safe: show if unparseable
  }
  // Compare at midnight to avoid timezone bugs
  const today = new Date();
  today.setHours(0, 0, 0, 0);
  return endDate >= today;
}

/**
 * Sort events by start date ascending (earliest first)
 * @param {Array} events - Array of event objects with parsed dates
 * @returns {Array} Sorted array
 */
export function sortEventsByDate(events) {
  return [...events].sort((a, b) => {
    const aDate = a.parsedDates?.startDate?.getTime() || Infinity;
    const bDate = b.parsedDates?.startDate?.getTime() || Infinity;
    return aDate - bDate;
  });
}

/**
 * Format a date range for display (short form)
 * @param {string} dateText - Original date text
 * @returns {string} Shortened display version
 */
export function formatDateShort(dateText) {
  if (!dateText) return '';
  // Remove year for compact display: "Jan 10-11, 2026" -> "Jan 10-11"
  return dateText.replace(/,?\s*\d{4}$/, '').trim();
}
