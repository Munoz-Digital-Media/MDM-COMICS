import React, { useMemo } from 'react'
import { Calendar, MapPin, ExternalLink } from 'lucide-react'
import { DEFAULT_CONVENTIONS } from '../config/conventions.config'

export default function ConventionShowcase() {
  const cards = useMemo(() => {
    const list = []
    DEFAULT_CONVENTIONS.forEach((con) => {
      (con.events || []).forEach((evt) => {
        list.push({
          convention: con.name,
          slug: con.slug,
          city: evt.name,
          date: evt.date_text || evt.date_text_full,
          dateFull: evt.date_text || evt.date_text_full,
          url: evt.event_url || con.baseUrl,
        })
      })
    })
    return list
  }, [])

  if (!cards.length) return null

  return (
    <section className="bg-gradient-to-r from-orange-500/10 via-red-500/5 to-zinc-900 border-t border-b border-zinc-800 py-10 px-4">
      <div className="max-w-6xl mx-auto">
        <div className="flex items-center justify-between mb-6">
          <div>
            <p className="text-xs uppercase tracking-[0.2em] text-orange-400">Conventions</p>
            <h2 className="text-2xl font-semibold text-white">Catch us on the road</h2>
            <p className="text-sm text-zinc-400">Upcoming show calendar (auto-fed from partner pages)</p>
          </div>
        </div>
        <div className="grid sm:grid-cols-2 lg:grid-cols-3 gap-4">
          {cards.map((card, idx) => (
            <div key={`${card.slug}-${idx}`} className="bg-zinc-900/80 border border-zinc-800 rounded-xl p-4 shadow-lg hover:-translate-y-0.5 transition-transform">
              <div className="flex items-center justify-between mb-3">
                <div className="text-xs px-2 py-1 rounded-full bg-orange-500/10 text-orange-300 border border-orange-500/30">
                  {card.convention}
                </div>
                {card.url && (
                  <a href={card.url} target="_blank" rel="noreferrer" className="text-zinc-400 hover:text-orange-300">
                    <ExternalLink className="w-4 h-4" />
                  </a>
                )}
              </div>
              <div className="text-lg font-semibold text-white">{card.city}</div>
              <div className="flex items-center gap-2 text-sm text-zinc-400 mt-2">
                <Calendar className="w-4 h-4 text-orange-300" />
                <span>{card.date}</span>
              </div>
              <div className="flex items-center gap-2 text-sm text-zinc-400 mt-1">
                <MapPin className="w-4 h-4 text-orange-300" />
                <span>{card.convention}</span>
              </div>
            </div>
          ))}
        </div>
      </div>
    </section>
  )
}
