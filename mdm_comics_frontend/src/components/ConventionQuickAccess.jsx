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

  // Inline styles for morphing animation (CSS-in-JS for the spring effect)
  const morphStyles = `
    .convention-card {
      transition: all 0.4s cubic-bezier(0.34, 1.56, 0.64, 1);
    }
    .convention-card.collapsed {
      flex: 0 0 auto;
    }
    .convention-card.expanded {
      flex: 1 1 100%;
    }
    .convention-card .collapsed-content {
      transition: opacity 0.15s ease-out, transform 0.15s ease-out;
    }
    .convention-card.expanded .collapsed-content {
      opacity: 0;
      transform: scale(0.95);
      position: absolute;
      pointer-events: none;
    }
    .convention-card .expanded-content {
      transition: opacity 0.2s ease-in 0.1s, transform 0.3s cubic-bezier(0.34, 1.56, 0.64, 1) 0.1s;
      opacity: 0;
      transform: scale(0.98);
    }
    .convention-card.expanded .expanded-content {
      opacity: 1;
      transform: scale(1);
    }
    .convention-card:not(.expanded) .expanded-content {
      position: absolute;
      pointer-events: none;
      height: 0;
      overflow: hidden;
    }
    .other-cards-fade {
      transition: opacity 0.3s ease, transform 0.3s ease;
    }
    .other-cards-fade.faded {
      opacity: 0.4;
      transform: scale(0.95);
    }
  `;

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

  return (
    <section className="max-w-7xl mx-auto px-4 pt-6 pb-4">
      <style>{morphStyles}</style>

      {/* Section Label */}
      <div className="mb-3">
        <p className="text-xs uppercase tracking-[0.15em] text-zinc-500">
          Upcoming Conventions
          {totalEventCount > 0 && (
            <span className="ml-2 text-zinc-600">({totalEventCount})</span>
          )}
        </p>
      </div>

      {/* Morphing Card Row */}
      <div
        className="flex items-stretch gap-2 flex-wrap"
        role="tablist"
        aria-label="Convention events"
      >
        {visibleEvents.map((event) => {
          const isExpanded = expandedEventId === event.id;
          const hasExpanded = expandedEventId !== null;
          const isFaded = hasExpanded && !isExpanded;

          return (
            <div
              key={event.id}
              className={`convention-card relative rounded-xl overflow-hidden
                ${isExpanded ? 'expanded' : 'collapsed'}
                ${isExpanded
                  ? 'bg-gradient-to-br from-zinc-900 via-zinc-900 to-zinc-800 border border-orange-500/30 shadow-xl shadow-orange-500/10'
                  : 'bg-zinc-800 border border-zinc-700 hover:border-zinc-600 cursor-pointer'
                }
                ${isFaded ? 'other-cards-fade faded' : 'other-cards-fade'}
              `}
              role="tab"
              aria-selected={isExpanded}
              aria-expanded={isExpanded}
              onClick={() => !isExpanded && handleButtonClick(event.id)}
              onKeyDown={(e) => e.key === 'Enter' && !isExpanded && handleButtonClick(event.id)}
              tabIndex={isExpanded ? -1 : 0}
            >
              {/* Collapsed State (Button) */}
              <div className="collapsed-content flex items-center gap-2 px-3 py-2">
                <Calendar className="w-4 h-4 flex-shrink-0 text-zinc-400" />
                <div className="flex flex-col items-start leading-tight">
                  <span className="text-xs text-zinc-500">{event.conventionName}</span>
                  <span className="text-sm font-medium text-zinc-300">{event.city}</span>
                </div>
                <span className="text-xs px-2 py-0.5 bg-zinc-900/80 rounded-full whitespace-nowrap text-zinc-400">
                  {formatDateShort(event.dateText)}
                </span>
              </div>

              {/* Expanded State (Detail Card) */}
              <div className="expanded-content p-4">
                {/* Row 1: Convention Name | Date | Time | Actions */}
                <div className="flex items-center justify-between gap-4 mb-3">
                  <div className="flex items-center gap-4 flex-wrap min-w-0">
                    <h3 className="text-lg font-semibold text-white">
                      {event.conventionName}
                    </h3>
                    <span className="flex items-center gap-1.5 text-sm text-zinc-400">
                      <Calendar className="w-4 h-4 text-orange-400" />
                      {event.dateText}
                    </span>
                    {event.showHours && (
                      <span className="flex items-center gap-1.5 text-sm text-zinc-400">
                        <Clock className="w-4 h-4 text-orange-400" />
                        {event.showHours}
                        {event.vipEntry && (
                          <span className="text-zinc-500">(VIP: {event.vipEntry})</span>
                        )}
                      </span>
                    )}
                  </div>
                  <div className="flex items-center gap-2 flex-shrink-0">
                    <a
                      href={event.ticketUrl || event.eventUrl}
                      target="_blank"
                      rel="noopener noreferrer"
                      className="px-4 py-2 bg-orange-500 hover:bg-orange-600 text-white text-sm font-medium rounded-lg transition-colors flex items-center gap-1.5"
                      onClick={(e) => e.stopPropagation()}
                    >
                      <Ticket className="w-4 h-4" />
                      Get Tickets
                    </a>
                    <button
                      onClick={(e) => {
                        e.stopPropagation();
                        setExpandedEventId(null);
                      }}
                      className="p-2 text-zinc-500 hover:text-white hover:bg-zinc-700 rounded-lg transition-colors"
                      aria-label="Close details"
                    >
                      <X className="w-4 h-4" />
                    </button>
                  </div>
                </div>

                {/* Row 2: Location | Table Count | Grading */}
                <div className="flex flex-wrap items-center gap-x-6 gap-y-2 text-sm text-zinc-400">
                  {(event.venue || event.address) && (
                    <span className="flex items-center gap-1.5">
                      <MapPin className="w-4 h-4 text-orange-400 flex-shrink-0" />
                      <span>
                        {event.venue}
                        {event.venue && event.address && ' · '}
                        {event.address}
                      </span>
                    </span>
                  )}
                  {event.tableCount && (
                    <span className="text-zinc-500">
                      {event.tableCount} tables
                    </span>
                  )}
                  {event.grading?.length > 0 && (
                    <span className="flex items-center gap-1.5">
                      <span className="text-zinc-500">Grading:</span>
                      <span className="text-zinc-300">{event.grading.join(', ')}</span>
                    </span>
                  )}
                </div>
              </div>
            </div>
          );
        })}

        {/* More events indicator */}
        {hiddenEventCount > 0 && onViewAll && (
          <button
            onClick={onViewAll}
            className={`flex items-center gap-2 px-4 py-2 rounded-xl transition-all whitespace-nowrap flex-shrink-0 bg-zinc-800/50 text-orange-400 hover:bg-zinc-700 border border-zinc-700 hover:border-orange-500/30
              ${expandedEventId ? 'other-cards-fade faded' : 'other-cards-fade'}
            `}
          >
            <span className="text-sm font-medium">+{hiddenEventCount} more</span>
            <ChevronRight className="w-4 h-4" />
          </button>
        )}
      </div>
    </section>
  );
}
