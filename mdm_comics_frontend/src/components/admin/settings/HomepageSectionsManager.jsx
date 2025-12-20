/**
 * HomepageSectionsManager Component
 *
 * CHARLIE-07: Admin UI for managing homepage section ordering and visibility.
 *
 * Features:
 * - Reorder sections using up/down buttons
 * - Toggle section visibility
 * - Adjust max items per section
 * - Preview current configuration
 * - Save changes to backend
 */
import React, { useState, useEffect, useCallback } from "react";
import {
  ChevronUp,
  ChevronDown,
  Eye,
  EyeOff,
  Save,
  RefreshCw,
  Loader2,
  AlertCircle,
  CheckCircle,
  GripVertical,
} from "lucide-react";

// Default sections (fallback)
const DEFAULT_SECTIONS = [
  { key: "bagged-boarded", title: "Bagged & Boarded Books", emoji: "📚", visible: true, display_order: 1, max_items: 5, data_source: "products" },
  { key: "graded", title: "Graded Books", emoji: "🏆", visible: true, display_order: 2, max_items: 5, data_source: "products" },
  { key: "funko", title: "Funko POPs", emoji: "🎭", visible: true, display_order: 3, max_items: 5, data_source: "products" },
  { key: "supplies", title: "Supplies", emoji: "📦", visible: true, display_order: 4, max_items: 5, data_source: "products" },
  { key: "bundles", title: "Bundles", emoji: "🎁", visible: true, display_order: 5, max_items: 5, data_source: "bundles" },
];

export default function HomepageSectionsManager() {
  const [sections, setSections] = useState(DEFAULT_SECTIONS);
  const [loading, setLoading] = useState(true);
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState(null);
  const [success, setSuccess] = useState(null);
  const [hasChanges, setHasChanges] = useState(false);

  // Fetch current configuration
  const fetchSections = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      // TODO: Replace with actual API call when backend is ready
      // const response = await fetch('/api/admin/homepage/sections');
      // const data = await response.json();
      // setSections(data.sections);

      // For now, use defaults
      setSections(DEFAULT_SECTIONS);
      setHasChanges(false);
    } catch (err) {
      setError("Failed to load sections configuration");
      console.error(err);
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    fetchSections();
  }, [fetchSections]);

  // Move section up
  const moveUp = (index) => {
    if (index === 0) return;
    const newSections = [...sections];
    [newSections[index - 1], newSections[index]] = [newSections[index], newSections[index - 1]];
    // Update display_order
    newSections.forEach((s, i) => { s.display_order = i + 1; });
    setSections(newSections);
    setHasChanges(true);
  };

  // Move section down
  const moveDown = (index) => {
    if (index === sections.length - 1) return;
    const newSections = [...sections];
    [newSections[index], newSections[index + 1]] = [newSections[index + 1], newSections[index]];
    // Update display_order
    newSections.forEach((s, i) => { s.display_order = i + 1; });
    setSections(newSections);
    setHasChanges(true);
  };

  // Toggle visibility
  const toggleVisibility = (index) => {
    const newSections = [...sections];
    newSections[index] = { ...newSections[index], visible: !newSections[index].visible };
    setSections(newSections);
    setHasChanges(true);
  };

  // Update max items
  const updateMaxItems = (index, value) => {
    const maxItems = Math.max(1, Math.min(10, parseInt(value) || 5));
    const newSections = [...sections];
    newSections[index] = { ...newSections[index], max_items: maxItems };
    setSections(newSections);
    setHasChanges(true);
  };

  // Save changes
  const saveChanges = async () => {
    setSaving(true);
    setError(null);
    setSuccess(null);
    try {
      // TODO: Replace with actual API call when backend is ready
      // await fetch('/api/admin/homepage/sections', {
      //   method: 'PUT',
      //   headers: { 'Content-Type': 'application/json' },
      //   body: JSON.stringify({ sections }),
      // });

      // Simulate save
      await new Promise(resolve => setTimeout(resolve, 500));

      setSuccess("Homepage sections saved successfully!");
      setHasChanges(false);
      setTimeout(() => setSuccess(null), 3000);
    } catch (err) {
      setError("Failed to save changes");
      console.error(err);
    } finally {
      setSaving(false);
    }
  };

  // Reset to defaults
  const resetToDefaults = () => {
    setSections(DEFAULT_SECTIONS);
    setHasChanges(true);
  };

  if (loading) {
    return (
      <div className="flex items-center justify-center py-12">
        <Loader2 className="w-8 h-8 text-orange-500 animate-spin" />
      </div>
    );
  }

  return (
    <div className="bg-zinc-900 rounded-xl border border-zinc-800 p-6">
      <div className="flex items-center justify-between mb-6">
        <div>
          <h2 className="text-xl font-bold text-white">Homepage Sections</h2>
          <p className="text-sm text-zinc-500 mt-1">
            Configure the order and visibility of homepage sections
          </p>
        </div>
        <div className="flex items-center gap-2">
          <button
            onClick={resetToDefaults}
            className="px-3 py-2 text-sm text-zinc-400 hover:text-white transition-colors"
          >
            Reset to Defaults
          </button>
          <button
            onClick={saveChanges}
            disabled={!hasChanges || saving}
            className={`flex items-center gap-2 px-4 py-2 rounded-lg font-semibold text-sm transition-all ${
              hasChanges && !saving
                ? "bg-orange-500 text-white hover:bg-orange-600"
                : "bg-zinc-800 text-zinc-500 cursor-not-allowed"
            }`}
          >
            {saving ? (
              <Loader2 className="w-4 h-4 animate-spin" />
            ) : (
              <Save className="w-4 h-4" />
            )}
            Save Changes
          </button>
        </div>
      </div>

      {/* Status Messages */}
      {error && (
        <div className="mb-4 p-3 bg-red-500/10 border border-red-500/20 rounded-lg flex items-center gap-2">
          <AlertCircle className="w-4 h-4 text-red-400" />
          <span className="text-sm text-red-400">{error}</span>
        </div>
      )}
      {success && (
        <div className="mb-4 p-3 bg-green-500/10 border border-green-500/20 rounded-lg flex items-center gap-2">
          <CheckCircle className="w-4 h-4 text-green-400" />
          <span className="text-sm text-green-400">{success}</span>
        </div>
      )}

      {/* Sections List */}
      <div className="space-y-2">
        {sections.map((section, index) => (
          <div
            key={section.key}
            className={`flex items-center gap-4 p-4 rounded-lg border transition-colors ${
              section.visible
                ? "bg-zinc-800/50 border-zinc-700"
                : "bg-zinc-900 border-zinc-800 opacity-60"
            }`}
          >
            {/* Grip Icon (visual only for now) */}
            <GripVertical className="w-5 h-5 text-zinc-600" />

            {/* Order Buttons */}
            <div className="flex flex-col gap-1">
              <button
                onClick={() => moveUp(index)}
                disabled={index === 0}
                className={`p-1 rounded transition-colors ${
                  index === 0
                    ? "text-zinc-700 cursor-not-allowed"
                    : "text-zinc-400 hover:text-white hover:bg-zinc-700"
                }`}
                aria-label="Move up"
              >
                <ChevronUp className="w-4 h-4" />
              </button>
              <button
                onClick={() => moveDown(index)}
                disabled={index === sections.length - 1}
                className={`p-1 rounded transition-colors ${
                  index === sections.length - 1
                    ? "text-zinc-700 cursor-not-allowed"
                    : "text-zinc-400 hover:text-white hover:bg-zinc-700"
                }`}
                aria-label="Move down"
              >
                <ChevronDown className="w-4 h-4" />
              </button>
            </div>

            {/* Section Info */}
            <div className="flex-1 min-w-0">
              <div className="flex items-center gap-2">
                <span className="text-xl">{section.emoji}</span>
                <span className="font-semibold text-white">{section.title}</span>
                <span className="px-2 py-0.5 bg-zinc-700 rounded text-xs text-zinc-400">
                  {section.data_source}
                </span>
              </div>
              <p className="text-xs text-zinc-500 mt-1">
                Key: {section.key} • Order: {section.display_order}
              </p>
            </div>

            {/* Max Items */}
            <div className="flex items-center gap-2">
              <label className="text-xs text-zinc-500">Max Items:</label>
              <input
                type="number"
                min="1"
                max="10"
                value={section.max_items}
                onChange={(e) => updateMaxItems(index, e.target.value)}
                className="w-16 px-2 py-1 bg-zinc-800 border border-zinc-700 rounded text-sm text-white text-center focus:outline-none focus:border-orange-500"
              />
            </div>

            {/* Visibility Toggle */}
            <button
              onClick={() => toggleVisibility(index)}
              className={`p-2 rounded-lg transition-colors ${
                section.visible
                  ? "bg-green-500/20 text-green-400 hover:bg-green-500/30"
                  : "bg-zinc-800 text-zinc-500 hover:bg-zinc-700"
              }`}
              aria-label={section.visible ? "Hide section" : "Show section"}
            >
              {section.visible ? (
                <Eye className="w-5 h-5" />
              ) : (
                <EyeOff className="w-5 h-5" />
              )}
            </button>
          </div>
        ))}
      </div>

      {/* Preview Note */}
      <div className="mt-6 p-4 bg-zinc-800/50 rounded-lg border border-zinc-700">
        <h3 className="text-sm font-semibold text-white mb-2">Preview Order</h3>
        <div className="flex flex-wrap gap-2">
          {sections
            .filter((s) => s.visible)
            .sort((a, b) => a.display_order - b.display_order)
            .map((section) => (
              <span
                key={section.key}
                className="inline-flex items-center gap-1 px-2 py-1 bg-zinc-700 rounded text-sm"
              >
                <span>{section.emoji}</span>
                <span className="text-zinc-300">{section.title}</span>
              </span>
            ))}
        </div>
        {sections.filter((s) => !s.visible).length > 0 && (
          <p className="text-xs text-zinc-500 mt-2">
            Hidden: {sections.filter((s) => !s.visible).map((s) => s.title).join(", ")}
          </p>
        )}
      </div>
    </div>
  );
}
