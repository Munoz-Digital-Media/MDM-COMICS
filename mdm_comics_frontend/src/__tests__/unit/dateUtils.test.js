/**
 * Unit tests for dateUtils - Convention date parsing utilities
 */
import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import {
  parseDateText,
  isFutureEvent,
  sortEventsByDate,
  formatDateShort
} from '../../utils/dateUtils';

describe('dateUtils', () => {
  describe('parseDateText', () => {
    it('parses same-month date range correctly', () => {
      const result = parseDateText('Jan 10-11, 2026');
      expect(result).not.toBeNull();
      expect(result.startDate.getFullYear()).toBe(2026);
      expect(result.startDate.getMonth()).toBe(0); // January
      expect(result.startDate.getDate()).toBe(10);
      expect(result.endDate.getDate()).toBe(11);
    });

    it('parses cross-month date range correctly', () => {
      const result = parseDateText('Jan 31-Feb 1, 2026');
      expect(result).not.toBeNull();
      expect(result.startDate.getMonth()).toBe(0); // January
      expect(result.startDate.getDate()).toBe(31);
      expect(result.endDate.getMonth()).toBe(1); // February
      expect(result.endDate.getDate()).toBe(1);
    });

    it('parses Feb-Mar cross-month range correctly', () => {
      const result = parseDateText('Feb 28-Mar 1, 2026');
      expect(result).not.toBeNull();
      expect(result.startDate.getMonth()).toBe(1); // February
      expect(result.startDate.getDate()).toBe(28);
      expect(result.endDate.getMonth()).toBe(2); // March
      expect(result.endDate.getDate()).toBe(1);
    });

    it('parses December dates correctly', () => {
      const result = parseDateText('Dec 27-28, 2026');
      expect(result).not.toBeNull();
      expect(result.startDate.getMonth()).toBe(11); // December
      expect(result.startDate.getDate()).toBe(27);
      expect(result.endDate.getDate()).toBe(28);
    });

    it('parses single-digit dates correctly', () => {
      const result = parseDateText('Jan 1-2, 2026');
      expect(result).not.toBeNull();
      expect(result.startDate.getDate()).toBe(1);
      expect(result.endDate.getDate()).toBe(2);
    });

    it('returns null for invalid input', () => {
      expect(parseDateText(null)).toBeNull();
      expect(parseDateText(undefined)).toBeNull();
      expect(parseDateText('')).toBeNull();
      expect(parseDateText('not a date')).toBeNull();
      expect(parseDateText(123)).toBeNull();
    });

    it('returns null for missing year', () => {
      expect(parseDateText('Jan 10-11')).toBeNull();
    });

    it('returns null for malformed date range', () => {
      expect(parseDateText('Jan 10, 2026')).toBeNull(); // Missing range
      expect(parseDateText('10-11, 2026')).toBeNull(); // Missing month
    });

    it('sets end date to end of day (23:59:59)', () => {
      const result = parseDateText('Jan 10-11, 2026');
      expect(result.endDate.getHours()).toBe(23);
      expect(result.endDate.getMinutes()).toBe(59);
      expect(result.endDate.getSeconds()).toBe(59);
    });
  });

  describe('isFutureEvent', () => {
    beforeEach(() => {
      // Mock current date to Jan 15, 2026
      vi.useFakeTimers();
      vi.setSystemTime(new Date(2026, 0, 15, 12, 0, 0));
    });

    afterEach(() => {
      vi.useRealTimers();
    });

    it('returns true for future events', () => {
      const futureDate = new Date(2026, 0, 20); // Jan 20, 2026
      expect(isFutureEvent(futureDate)).toBe(true);
    });

    it('returns true for events ending today', () => {
      const todayEnd = new Date(2026, 0, 15, 23, 59, 59); // Jan 15, 2026 end of day
      expect(isFutureEvent(todayEnd)).toBe(true);
    });

    it('returns false for past events', () => {
      const pastDate = new Date(2026, 0, 10); // Jan 10, 2026
      expect(isFutureEvent(pastDate)).toBe(false);
    });

    it('returns true for null (fail-safe)', () => {
      expect(isFutureEvent(null)).toBe(true);
    });

    it('returns true for undefined (fail-safe)', () => {
      expect(isFutureEvent(undefined)).toBe(true);
    });

    it('returns true for invalid Date object (fail-safe)', () => {
      expect(isFutureEvent(new Date('invalid'))).toBe(true);
    });

    it('returns true for non-Date objects (fail-safe)', () => {
      expect(isFutureEvent('2026-01-20')).toBe(true);
      expect(isFutureEvent(12345)).toBe(true);
    });
  });

  describe('sortEventsByDate', () => {
    it('sorts events by start date ascending', () => {
      const events = [
        { id: 'c', parsedDates: { startDate: new Date(2026, 2, 1) } }, // March
        { id: 'a', parsedDates: { startDate: new Date(2026, 0, 10) } }, // Jan
        { id: 'b', parsedDates: { startDate: new Date(2026, 1, 15) } }, // Feb
      ];

      const sorted = sortEventsByDate(events);
      expect(sorted[0].id).toBe('a'); // Jan first
      expect(sorted[1].id).toBe('b'); // Feb second
      expect(sorted[2].id).toBe('c'); // March last
    });

    it('handles events with same date', () => {
      const events = [
        { id: 'a', parsedDates: { startDate: new Date(2026, 0, 17) } },
        { id: 'b', parsedDates: { startDate: new Date(2026, 0, 17) } },
      ];

      const sorted = sortEventsByDate(events);
      expect(sorted.length).toBe(2);
    });

    it('puts events without parsed dates at end', () => {
      const events = [
        { id: 'b', parsedDates: null },
        { id: 'a', parsedDates: { startDate: new Date(2026, 0, 10) } },
      ];

      const sorted = sortEventsByDate(events);
      expect(sorted[0].id).toBe('a'); // Valid date first
      expect(sorted[1].id).toBe('b'); // Null dates last
    });

    it('does not mutate original array', () => {
      const events = [
        { id: 'b', parsedDates: { startDate: new Date(2026, 1, 1) } },
        { id: 'a', parsedDates: { startDate: new Date(2026, 0, 1) } },
      ];

      const sorted = sortEventsByDate(events);
      expect(events[0].id).toBe('b'); // Original unchanged
      expect(sorted[0].id).toBe('a'); // Sorted copy
    });

    it('handles empty array', () => {
      expect(sortEventsByDate([])).toEqual([]);
    });
  });

  describe('formatDateShort', () => {
    it('removes year from date string', () => {
      expect(formatDateShort('Jan 10-11, 2026')).toBe('Jan 10-11');
    });

    it('handles dates without comma before year', () => {
      expect(formatDateShort('Jan 10-11 2026')).toBe('Jan 10-11');
    });

    it('handles cross-month ranges', () => {
      expect(formatDateShort('Jan 31-Feb 1, 2026')).toBe('Jan 31-Feb 1');
    });

    it('returns empty string for null/undefined', () => {
      expect(formatDateShort(null)).toBe('');
      expect(formatDateShort(undefined)).toBe('');
    });

    it('returns empty string for empty input', () => {
      expect(formatDateShort('')).toBe('');
    });
  });
});
