import React, { useState, useCallback } from "react";
import { Search, X, Loader2, BookOpen, Calendar, Building2 } from "lucide-react";
import { comicsAPI } from "../services/api";

/**
 * Comic Database Search Component
 * Searches the Metron database for comic issues
 */
export default function ComicSearch({ onSelectComic, onClose }) {
  const [searchQuery, setSearchQuery] = useState("");
  const [issueNumber, setIssueNumber] = useState("");
  const [results, setResults] = useState([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);
  const [hasSearched, setHasSearched] = useState(false);

  const handleSearch = useCallback(async () => {
    if (!searchQuery.trim()) return;

    setLoading(true);
    setError(null);
    setHasSearched(true);

    try {
      const data = await comicsAPI.search({
        series: searchQuery,
        number: issueNumber || undefined,
      });
      setResults(data.results || []);
    } catch (err) {
      setError("Failed to search comics. Please try again.");
      console.error(err);
    } finally {
      setLoading(false);
    }
  }, [searchQuery, issueNumber]);

  const handleKeyPress = (e) => {
    if (e.key === "Enter") {
      handleSearch();
    }
  };

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center">
      {/* Overlay */}
      <div
        className="absolute inset-0 bg-black/70 backdrop-blur-sm"
        onClick={onClose}
      />

      {/* Modal */}
      <div className="relative bg-zinc-900 rounded-2xl border border-zinc-800 w-full max-w-3xl mx-4 max-h-[85vh] flex flex-col shadow-2xl">
        {/* Header */}
        <div className="flex items-center justify-between p-6 border-b border-zinc-800">
          <div>
            <h3 className="font-comic text-2xl text-white">COMIC DATABASE</h3>
            <p className="text-zinc-500 text-sm">Search the Metron comic database</p>
          </div>
          <button
            onClick={onClose}
            className="p-2 hover:bg-zinc-800 rounded-lg transition-colors"
          >
            <X className="w-5 h-5 text-zinc-400" />
          </button>
        </div>

        {/* Search Form */}
        <div className="p-6 border-b border-zinc-800">
          <div className="flex gap-3">
            <div className="flex-1">
              <label className="block text-xs text-zinc-500 mb-1">Series Name</label>
              <input
                type="text"
                value={searchQuery}
                onChange={(e) => setSearchQuery(e.target.value)}
                onKeyPress={handleKeyPress}
                placeholder="e.g., Amazing Spider-Man"
                className="w-full px-4 py-3 bg-zinc-800 border border-zinc-700 rounded-xl text-white placeholder-zinc-500 focus:outline-none focus:border-orange-500 transition-colors"
              />
            </div>
            <div className="w-32">
              <label className="block text-xs text-zinc-500 mb-1">Issue #</label>
              <input
                type="text"
                value={issueNumber}
                onChange={(e) => setIssueNumber(e.target.value)}
                onKeyPress={handleKeyPress}
                placeholder="e.g., 300"
                className="w-full px-4 py-3 bg-zinc-800 border border-zinc-700 rounded-xl text-white placeholder-zinc-500 focus:outline-none focus:border-orange-500 transition-colors"
              />
            </div>
            <div className="flex items-end">
              <button
                onClick={handleSearch}
                disabled={loading || !searchQuery.trim()}
                className="px-6 py-3 bg-orange-500 rounded-xl font-bold text-white hover:shadow-lg hover:shadow-orange-500/25 transition-all disabled:opacity-50 disabled:cursor-not-allowed flex items-center gap-2"
              >
                {loading ? (
                  <Loader2 className="w-5 h-5 animate-spin" />
                ) : (
                  <Search className="w-5 h-5" />
                )}
                Search
              </button>
            </div>
          </div>
        </div>

        {/* Results */}
        <div className="flex-1 overflow-auto p-6">
          {error && (
            <div className="text-center py-8">
              <p className="text-red-500">{error}</p>
            </div>
          )}

          {!hasSearched && !loading && (
            <div className="text-center py-12">
              <BookOpen className="w-16 h-16 text-zinc-700 mx-auto mb-4" />
              <p className="text-zinc-500">Search for comics by series name and issue number</p>
              <p className="text-zinc-600 text-sm mt-2">Powered by Metron Database</p>
            </div>
          )}

          {hasSearched && results.length === 0 && !loading && (
            <div className="text-center py-12">
              <p className="text-zinc-500">No comics found. Try a different search.</p>
            </div>
          )}

          {results.length > 0 && (
            <div className="grid grid-cols-2 md:grid-cols-3 gap-4">
              {results.map((comic) => (
                <div
                  key={comic.id}
                  onClick={() => onSelectComic(comic)}
                  className="bg-zinc-800 rounded-xl border border-zinc-700 overflow-hidden cursor-pointer hover:border-orange-500 hover:shadow-lg hover:shadow-orange-500/10 transition-all group"
                >
                  {/* Cover Image */}
                  <div className="aspect-[2/3] bg-zinc-900 relative overflow-hidden">
                    {comic.image ? (
                      <img
                        src={comic.image}
                        alt={comic.issue}
                        className="w-full h-full object-cover group-hover:scale-105 transition-transform duration-300"
                        onError={(e) => {
                          e.target.onerror = null;
                          e.target.src = `https://placehold.co/400x600/27272a/f59e0b?text=${encodeURIComponent(comic.number || '?')}`;
                        }}
                      />
                    ) : (
                      <div className="w-full h-full flex items-center justify-center">
                        <BookOpen className="w-12 h-12 text-zinc-700" />
                      </div>
                    )}
                  </div>

                  {/* Info */}
                  <div className="p-3">
                    <p className="text-xs text-orange-500 font-semibold mb-1">
                      {comic.series?.name || 'Unknown Series'}
                    </p>
                    <h4 className="text-white font-bold text-sm line-clamp-2 mb-2">
                      #{comic.number}
                    </h4>
                    <div className="flex items-center gap-2 text-xs text-zinc-500">
                      {comic.cover_date && (
                        <span className="flex items-center gap-1">
                          <Calendar className="w-3 h-3" />
                          {new Date(comic.cover_date).getFullYear()}
                        </span>
                      )}
                      {comic.series?.volume && (
                        <span className="flex items-center gap-1">
                          <Building2 className="w-3 h-3" />
                          Vol. {comic.series.volume}
                        </span>
                      )}
                    </div>
                  </div>
                </div>
              ))}
            </div>
          )}
        </div>

        {/* Footer */}
        <div className="p-4 border-t border-zinc-800 text-center">
          <p className="text-xs text-zinc-600">
            Data provided by <a href="https://metron.cloud" target="_blank" rel="noopener noreferrer" className="text-orange-500 hover:underline">Metron.cloud</a>
          </p>
        </div>
      </div>
    </div>
  );
}
