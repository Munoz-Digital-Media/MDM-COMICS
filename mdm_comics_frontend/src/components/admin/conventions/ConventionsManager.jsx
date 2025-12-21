import React, { useEffect, useMemo, useState } from 'react'
import { Plus, Save, RefreshCw, ExternalLink } from 'lucide-react'
import { DEFAULT_CONVENTIONS } from '../../../config/conventions.config'

const STORAGE_KEY = 'mdm-conventions-config'

export default function ConventionsManager() {
  const [conventions, setConventions] = useState(() => {
    try {
      const cached = localStorage.getItem(STORAGE_KEY)
      if (cached) return JSON.parse(cached)
    } catch {
      /* ignore */
    }
    return DEFAULT_CONVENTIONS
  })
  const [draft, setDraft] = useState({
    slug: '',
    name: '',
    baseUrl: '',
    parser: 'galaxycon_shopify',
  })

  useEffect(() => {
    try {
      localStorage.setItem(STORAGE_KEY, JSON.stringify(conventions))
    } catch {
      /* ignore */
    }
  }, [conventions])

  const sorted = useMemo(
    () => [...conventions].sort((a, b) => a.name.localeCompare(b.name)),
    [conventions]
  )

  const addConvention = () => {
    if (!draft.slug || !draft.name) return
    setConventions((prev) => {
      const exists = prev.find((c) => c.slug === draft.slug)
      if (exists) {
        return prev.map((c) => (c.slug === draft.slug ? { ...c, ...draft } : c))
      }
      return [...prev, { ...draft, pages: draft.pages || {} }]
    })
    setDraft({ slug: '', name: '', baseUrl: '', parser: 'galaxycon_shopify' })
  }

  const updatePage = (slug, key, value) => {
    setConventions((prev) =>
      prev.map((c) =>
        c.slug === slug ? { ...c, pages: { ...c.pages, [key]: value } } : c
      )
    )
  }

  return (
    <div className="p-6 space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h2 className="text-xl font-semibold text-white">Conventions</h2>
          <p className="text-sm text-zinc-400">
            Configure convention sources for the ML features. Saved locally (browser) until backend API is added.
          </p>
        </div>
        <button
          onClick={() => setConventions(DEFAULT_CONVENTIONS)}
          className="inline-flex items-center gap-2 px-3 py-2 text-sm text-zinc-100 bg-zinc-800 rounded-lg hover:bg-zinc-700 transition"
        >
          <RefreshCw className="w-4 h-4" />
          Reset to defaults
        </button>
      </div>

      <div className="grid md:grid-cols-2 gap-4">
        <div className="bg-zinc-900 border border-zinc-800 rounded-lg p-4 space-y-3">
          <h3 className="text-sm font-semibold text-white flex items-center gap-2">
            <Plus className="w-4 h-4" /> Add / Update
          </h3>
          <div className="space-y-2">
            <input
              className="w-full bg-zinc-800 text-white text-sm rounded px-3 py-2 outline-none border border-zinc-700 focus:border-red-400"
              placeholder="slug (unique id)"
              value={draft.slug}
              onChange={(e) => setDraft({ ...draft, slug: e.target.value })}
            />
            <input
              className="w-full bg-zinc-800 text-white text-sm rounded px-3 py-2 outline-none border border-zinc-700 focus:border-red-400"
              placeholder="Name"
              value={draft.name}
              onChange={(e) => setDraft({ ...draft, name: e.target.value })}
            />
            <input
              className="w-full bg-zinc-800 text-white text-sm rounded px-3 py-2 outline-none border border-zinc-700 focus:border-red-400"
              placeholder="Base URL"
              value={draft.baseUrl}
              onChange={(e) => setDraft({ ...draft, baseUrl: e.target.value })}
            />
            <input
              className="w-full bg-zinc-800 text-white text-sm rounded px-3 py-2 outline-none border border-zinc-700 focus:border-red-400"
              placeholder="Parser (e.g., galaxycon_shopify)"
              value={draft.parser}
              onChange={(e) => setDraft({ ...draft, parser: e.target.value })}
            />
          </div>
          <button
            onClick={addConvention}
            className="inline-flex items-center gap-2 px-3 py-2 text-sm text-white bg-red-500 rounded-lg hover:bg-red-600 transition"
          >
            <Save className="w-4 h-4" />
            Save
          </button>
          <p className="text-xs text-zinc-500">
            Tip: Only local to your browser for now. Backend API wiring pending.
          </p>
        </div>

        <div className="bg-zinc-900 border border-zinc-800 rounded-lg p-4 space-y-3">
          <h3 className="text-sm font-semibold text-white">Pages (selected)</h3>
          <p className="text-xs text-zinc-500">
            Select an event below, then edit page URLs if needed.
          </p>
          {sorted.map((c) => (
            <div key={c.slug} className="border border-zinc-800 rounded-lg p-3 mb-3">
              <div className="flex items-center justify-between mb-2">
                <div>
                  <div className="text-sm font-semibold text-white">{c.name}</div>
                  <div className="text-xs text-zinc-500">{c.slug} · {c.parser}</div>
                </div>
                {c.baseUrl && (
                  <a
                    href={c.baseUrl}
                    target="_blank"
                    rel="noreferrer"
                    className="text-xs text-red-300 inline-flex items-center gap-1 hover:text-red-200"
                  >
                    View <ExternalLink className="w-3 h-3" />
                  </a>
                )}
              </div>
              {['guests','autographs','photoOps','groupPhotoOps','mailInAutographs'].map((key) => (
                <div key={key} className="mb-2">
                  <label className="text-xs text-zinc-400 block mb-1">{key}</label>
                  <input
                    className="w-full bg-zinc-800 text-white text-sm rounded px-3 py-2 outline-none border border-zinc-700 focus:border-red-400"
                    value={c.pages?.[key] || ''}
                    onChange={(e) => updatePage(c.slug, key, e.target.value)}
                    placeholder={`https://.../${key}`}
                  />
                </div>
              ))}
            </div>
          ))}
        </div>
      </div>
    </div>
  )
