/**
 * ConventionQuickAccess - Tab-style convention buttons with morphing detail cards
 * v1.3.0
 *
 * Features:
 * - Horizontal button row matching admin pipeline pattern
 * - Future events only, date-sorted ASC
 * - Capped at 4 buttons + "+N more" button (5 total)
 * - Morphing card animation: button expands into detail view
 * - Rich venue/ticket info in expanded state
 */
import React, { useState, useMemo, useCallback, useId } from 'react';
import { Calendar, MapPin, ChevronRight, Clock, Ticket, X } from 'lucide-react';
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
          // Rich data fields
          venue: event.venue || null,
          address: event.address || null,
          tableCount: event.tableCount || null,
          showHours: event.showHours || null,
          vipEntry: event.vipEntry || null,
          grading: event.grading || [],
          ticketUrl: event.ticketUrl || null,
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

  // Find the expanded event data
  const expandedEvent = expandedEventId
    ? visibleEvents.find((e) => e.id === expandedEventId)
    : null;

  return (
    <section className="max-w-7xl mx-auto px-4 pt-6 pb-4">
      {/* Section Label */}
      <div className="mb-3">
        <p className="text-xs uppercase tracking-[0.15em] text-zinc-500">
          Upcoming Conventions
          {totalEventCount > 0 && (
            <span className="ml-2 text-zinc-600">({totalEventCount})</span>
          )}
        </p>
      </div>

      {/* Button Row - inline-flex so it doesn't reflow */}
      <div className="overflow-x-auto pb-2">
        <div
          className="inline-flex items-center gap-2"
          role="tablist"
          aria-label="Convention events"
        >
          {visibleEvents.map((event) => {
            const isSelected = expandedEventId === event.id;

            return (
              <button
                key={event.id}
                type="button"
                onClick={() => handleButtonClick(event.id)}
                className={`flex items-center gap-2 px-3 py-2 rounded-lg border whitespace-nowrap transition-colors
                  ${isSelected
                    ? 'bg-orange-500/20 text-orange-400 border-orange-500/50'
                    : 'bg-zinc-800 text-zinc-400 border-zinc-700 hover:bg-zinc-700 hover:border-zinc-600'
                  }
                `}
                role="tab"
                aria-selected={isSelected}
              >
                <Calendar className="w-4 h-4 flex-shrink-0" />
                <div className="flex flex-col items-start leading-tight">
                  <span className={`text-xs ${isSelected ? 'text-orange-300/70' : 'text-zinc-500'}`}>
                    {event.conventionName}
                  </span>
                  <span className="text-sm font-medium">{event.city}</span>
                </div>
                <span className={`text-xs px-2 py-0.5 rounded-full
                  ${isSelected ? 'bg-orange-500/20 text-orange-300' : 'bg-zinc-900/80 text-zinc-400'}
                `}>
                  {formatDateShort(event.dateText)}
                </span>
              </button>
            );
          })}

          {/* More events indicator */}
          {hiddenEventCount > 0 && onViewAll && (
            <button
              type="button"
              onClick={onViewAll}
              className="flex items-center gap-2 px-4 py-2 rounded-lg whitespace-nowrap bg-zinc-800/50 text-orange-400 hover:bg-zinc-700 border border-zinc-700 hover:border-orange-500/30 transition-colors"
            >
              <span className="text-sm font-medium">+{hiddenEventCount} more</span>
              <ChevronRight className="w-4 h-4" />
            </button>
          )}
        </div>
      </div>

      {/* Detail Card - completely separate block below */}
      {expandedEvent && (
        <div className="mt-3 bg-gradient-to-br from-zinc-900 via-zinc-900 to-zinc-800 border border-orange-500/30 rounded-xl p-4 shadow-lg">
          {/* Row 1: Convention Name | Date | Time | Actions */}
          <div className="flex items-center justify-between gap-4 mb-3 flex-wrap">
            <div className="flex items-center gap-4 flex-wrap">
              <h3 className="text-lg font-semibold text-white">
                {expandedEvent.conventionName}
              </h3>
              <span className="flex items-center gap-1.5 text-sm text-zinc-400">
                <Calendar className="w-4 h-4 text-orange-400" />
                {expandedEvent.dateText}
              </span>
              {expandedEvent.showHours && (
                <span className="flex items-center gap-1.5 text-sm text-zinc-400">
                  <Clock className="w-4 h-4 text-orange-400" />
                  {expandedEvent.showHours}
                  {expandedEvent.vipEntry && (
                    <span className="text-zinc-500">(VIP: {expandedEvent.vipEntry})</span>
                  )}
                </span>
              )}
            </div>
            <div className="flex items-center gap-2">
              <a
                href={expandedEvent.ticketUrl || expandedEvent.eventUrl}
                target="_blank"
                rel="noopener noreferrer"
                className="px-4 py-2 bg-orange-500 hover:bg-orange-600 text-white text-sm font-medium rounded-lg transition-colors flex items-center gap-1.5"
              >
                <Ticket className="w-4 h-4" />
                Get Tickets
              </a>
              <button
                type="button"
                onClick={() => setExpandedEventId(null)}
                className="p-2 text-zinc-500 hover:text-white hover:bg-zinc-700 rounded-lg transition-colors"
                aria-label="Close details"
              >
                <X className="w-4 h-4" />
              </button>
            </div>
          </div>

          {/* Row 2: Location | Table Count | Grading */}
          <div className="flex flex-wrap items-center gap-x-6 gap-y-2 text-sm text-zinc-400">
            {(expandedEvent.venue || expandedEvent.address) && (
              <span className="flex items-center gap-1.5">
                <MapPin className="w-4 h-4 text-orange-400 flex-shrink-0" />
                <span>
                  {expandedEvent.venue}
                  {expandedEvent.venue && expandedEvent.address && ' · '}
                  {expandedEvent.address}
                </span>
              </span>
            )}
            {expandedEvent.tableCount && (
              <span className="text-zinc-500">
                {expandedEvent.tableCount} tables
              </span>
            )}
            {expandedEvent.grading?.length > 0 && (
              <span className="flex items-center gap-1.5">
                <span className="text-zinc-500">Grading:</span>
                <span className="text-zinc-300">{expandedEvent.grading.join(', ')}</span>
              </span>
            )}
          </div>
        </div>
      )}
    </section>
  );
}
