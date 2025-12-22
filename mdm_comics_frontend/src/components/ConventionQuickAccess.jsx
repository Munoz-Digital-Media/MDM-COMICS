/**
 * ConventionQuickAccess - Tab-style convention buttons with expandable details
 * v1.1.0
 *
 * Features:
 * - Horizontal button row matching admin pipeline pattern (GCD Import, PriceCharting, etc.)
 * - Future events only (past events filtered out)
 * - Date-sorted ASC (nearest to TODAY shown first)
 * - Capped at 5 buttons with "View All" link
 * - Expandable detail card on click
 * - Title links to convention website
 */
import React, { useState, useMemo, useCallback, useId } from 'react';
import { Calendar, ExternalLink, MapPin, ChevronDown, ChevronUp, ChevronRight } from 'lucide-react';
import { DEFAULT_CONVENTIONS } from '../config/conventions.config';
import { parseDateText, isFutureEvent, sortEventsByDate, formatDateShort } from '../utils/dateUtils';

const MAX_TOTAL_ITEMS = 5; // Total items including "+more" button (4 events + 1 more)

export default function ConventionQuickAccess({ onViewAll }) {
  const [expandedEventId, setExpandedEventId] = useState(null);
  const uniqueId = useId();

  // Process events: flatten, parse dates, filter future, sort ASC
  const processedEvents = useMemo(() => {
    const allEvents = [];

    DEFAULT_CONVENTIONS.forEach((convention) => {
      (convention.events || []).forEach((event, idx) => {
        const parsedDates = parseDateText(event.date_text);
        const eventId = `${convention.slug}-${idx}`;

        allEvents.push({
          id: eventId,
          conventionName: convention.name,
          conventionSlug: convention.slug,
          city: event.name,
          dateText: event.date_text,
          eventUrl: event.event_url || convention.baseUrl,
          parsedDates,
        });
      });
    });

    // Filter to future events only
    const futureEvents = allEvents.filter((evt) => {
      if (!evt.parsedDates) return true; // Include unparseable (fail-safe)
      return isFutureEvent(evt.parsedDates.endDate);
    });

    // Sort by date ASC (earliest first = leftmost)
    return sortEventsByDate(futureEvents);
  }, []);

  const handleButtonClick = useCallback((eventId) => {
    setExpandedEventId((prev) => (prev === eventId ? null : eventId));
  }, []);

  const expandedEvent = useMemo(() => {
    if (!expandedEventId) return null;
    return processedEvents.find((e) => e.id === expandedEventId);
  }, [expandedEventId, processedEvents]);

  // Calculate how many event buttons to show
  // If we have more events than MAX_TOTAL_ITEMS, reserve 1 slot for "+more" button
  const hasMoreEvents = processedEvents.length > MAX_TOTAL_ITEMS;
  const maxEventButtons = hasMoreEvents ? MAX_TOTAL_ITEMS - 1 : MAX_TOTAL_ITEMS;

  const visibleEvents = useMemo(() => {
    return processedEvents.slice(0, maxEventButtons);
  }, [processedEvents, maxEventButtons]);

  const totalEventCount = processedEvents.length;
  const hiddenEventCount = totalEventCount - visibleEvents.length;

  // Don't render if no future events
  if (processedEvents.length === 0) {
    return null;
  }

  return (
    <section className="max-w-7xl mx-auto px-4 pt-6 pb-4">
      {/* Section Label with View All link */}
      <div className="flex items-center justify-between mb-3">
        <p className="text-xs uppercase tracking-[0.15em] text-zinc-500">
          Upcoming Conventions
          {totalEventCount > 0 && (
            <span className="ml-2 text-zinc-600">({totalEventCount})</span>
          )}
        </p>
        {onViewAll && (
          <button
            onClick={onViewAll}
            className="text-xs text-orange-400 hover:text-orange-300 transition-colors flex items-center gap-1"
          >
            View All
            <ChevronRight className="w-3 h-3" />
          </button>
        )}
      </div>

      {/* Button Row - No scroll, max 5 items */}
      <div
        className="flex items-center gap-2 flex-wrap"
        role="tablist"
        aria-label="Convention events"
      >
        {visibleEvents.map((event) => {
          const isExpanded = expandedEventId === event.id;
          const cardId = `${uniqueId}-card-${event.id}`;

          return (
            <button
              key={event.id}
              onClick={() => handleButtonClick(event.id)}
              className={`flex items-center gap-2 px-3 py-2 rounded-lg transition-colors flex-shrink-0
                ${isExpanded
                  ? 'bg-orange-500/20 text-orange-400 border border-orange-500/30'
                  : 'bg-zinc-800 text-zinc-400 hover:bg-zinc-700 border border-zinc-700 hover:border-zinc-600'
                }`}
              role="tab"
              aria-selected={isExpanded}
              aria-expanded={isExpanded}
              aria-controls={isExpanded ? cardId : undefined}
            >
              <Calendar className="w-4 h-4 flex-shrink-0" />
              <div className="flex flex-col items-start leading-tight">
                <span className="text-xs text-zinc-500">{event.conventionName}</span>
                <span className="text-sm font-medium">{event.city}</span>
              </div>
              <span className="text-xs px-2 py-0.5 bg-zinc-900/80 rounded-full whitespace-nowrap">
                {formatDateShort(event.dateText)}
              </span>
              {isExpanded ? (
                <ChevronUp className="w-3 h-3 ml-1" />
              ) : (
                <ChevronDown className="w-3 h-3 ml-1" />
              )}
            </button>
          );
        })}

        {/* More events indicator - only if there are hidden events */}
        {hiddenEventCount > 0 && onViewAll && (
          <button
            onClick={onViewAll}
            className="flex items-center gap-2 px-4 py-2 rounded-lg transition-colors whitespace-nowrap flex-shrink-0 bg-zinc-800/50 text-orange-400 hover:bg-zinc-700 border border-zinc-700 hover:border-orange-500/30"
          >
            <span className="text-sm font-medium">+{hiddenEventCount} more</span>
            <ChevronRight className="w-4 h-4" />
          </button>
        )}
      </div>

      {/* Expanded Detail Card */}
      {expandedEvent && (
        <div
          id={`${uniqueId}-card-${expandedEvent.id}`}
          role="tabpanel"
          aria-labelledby={expandedEvent.id}
          className="mt-3 bg-zinc-900/90 border border-orange-500/20 rounded-xl p-4 shadow-lg animate-fadeIn"
        >
          <div className="flex items-start justify-between gap-4">
            <div className="flex-1 min-w-0">
              {/* Clickable Title */}
              <a
                href={expandedEvent.eventUrl}
                target="_blank"
                rel="noopener noreferrer"
                className="inline-flex items-center gap-2 text-lg font-semibold text-white hover:text-orange-400 transition-colors group"
                aria-label={`Visit ${expandedEvent.conventionName} website (opens in new tab)`}
              >
                <span>{expandedEvent.conventionName}</span>
                <ExternalLink className="w-4 h-4 opacity-50 group-hover:opacity-100 transition-opacity" />
              </a>

              {/* City & Date */}
              <div className="flex flex-wrap items-center gap-3 mt-2 text-sm text-zinc-400">
                <span className="flex items-center gap-1.5">
                  <MapPin className="w-4 h-4 text-orange-400" />
                  {expandedEvent.city}
                </span>
                <span className="flex items-center gap-1.5">
                  <Calendar className="w-4 h-4 text-orange-400" />
                  {expandedEvent.dateText}
                </span>
              </div>
            </div>

            {/* CTA Button */}
            <a
              href={expandedEvent.eventUrl}
              target="_blank"
              rel="noopener noreferrer"
              className="flex-shrink-0 px-4 py-2 bg-orange-500 hover:bg-orange-600 text-white text-sm font-medium rounded-lg transition-colors"
            >
              View Details
            </a>
          </div>
        </div>
      )}
    </section>
  );
}
