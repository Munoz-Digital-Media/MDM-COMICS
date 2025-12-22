/**
 * ConventionsPage - Full page display of all upcoming conventions
 * v1.0.0
 *
 * Features:
 * - Card-based layout for all conventions
 * - Date-sorted ASC (nearest to TODAY shown first)
 * - Future events only
 * - External links to convention websites
 */
import React, { useMemo } from 'react';
import { Calendar, ExternalLink, MapPin, ArrowLeft } from 'lucide-react';
import { DEFAULT_CONVENTIONS } from '../config/conventions.config';
import { parseDateText, isFutureEvent, sortEventsByDate } from '../utils/dateUtils';

export default function ConventionsPage({ onBack }) {
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

    // Sort by date ASC (nearest to today first)
    return sortEventsByDate(futureEvents);
  }, []);

  return (
    <div className="min-h-screen bg-zinc-950">
      {/* Header */}
      <div className="max-w-7xl mx-auto px-4 py-8">
        <button
          onClick={onBack}
          className="flex items-center gap-2 text-zinc-400 hover:text-white transition-colors mb-6"
        >
          <ArrowLeft className="w-5 h-5" />
          <span>Back to Shop</span>
        </button>

        <h1 className="font-comic text-3xl sm:text-4xl md:text-5xl text-white mb-2">
          Upcoming <span className="text-orange-500">Conventions</span>
        </h1>
        <p className="text-zinc-400 text-lg">
          Find us at these upcoming events
        </p>
      </div>

      {/* Convention Cards Grid */}
      <div className="max-w-7xl mx-auto px-4 pb-16">
        {processedEvents.length === 0 ? (
          <div className="text-center py-16">
            <Calendar className="w-16 h-16 text-zinc-600 mx-auto mb-4" />
            <p className="text-zinc-400 text-lg">No upcoming conventions scheduled</p>
          </div>
        ) : (
          <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-6">
            {processedEvents.map((event, index) => (
              <ConventionCard
                key={event.id}
                event={event}
                isNext={index === 0}
              />
            ))}
          </div>
        )}
      </div>
    </div>
  );
}

function ConventionCard({ event, isNext }) {
  return (
    <div
      className={`relative bg-zinc-900 border rounded-xl overflow-hidden transition-all hover:border-orange-500/50 hover:shadow-lg hover:shadow-orange-500/10 ${
        isNext ? 'border-orange-500/30 ring-1 ring-orange-500/20' : 'border-zinc-800'
      }`}
    >
      {/* Next Up Badge */}
      {isNext && (
        <div className="absolute top-3 right-3 px-2 py-1 bg-orange-500 text-white text-xs font-bold rounded-full uppercase tracking-wide">
          Next Up
        </div>
      )}

      {/* Card Content */}
      <div className="p-6">
        {/* Convention Name */}
        <h3 className="text-xl font-semibold text-white mb-3 pr-20">
          {event.conventionName}
        </h3>

        {/* City */}
        <div className="flex items-center gap-2 text-zinc-400 mb-2">
          <MapPin className="w-4 h-4 text-orange-400 flex-shrink-0" />
          <span>{event.city}</span>
        </div>

        {/* Date */}
        <div className="flex items-center gap-2 text-zinc-400 mb-6">
          <Calendar className="w-4 h-4 text-orange-400 flex-shrink-0" />
          <span>{event.dateText}</span>
        </div>

        {/* CTA Button */}
        <a
          href={event.eventUrl}
          target="_blank"
          rel="noopener noreferrer"
          className="flex items-center justify-center gap-2 w-full px-4 py-3 bg-zinc-800 hover:bg-orange-500 text-white text-sm font-medium rounded-lg transition-colors group"
        >
          <span>View Event Details</span>
          <ExternalLink className="w-4 h-4 opacity-50 group-hover:opacity-100 transition-opacity" />
        </a>
      </div>
    </div>
  );
}
