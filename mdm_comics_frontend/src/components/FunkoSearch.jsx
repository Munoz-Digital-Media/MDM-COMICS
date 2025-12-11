import React, { useState, useCallback, useEffect } from "react";
import { Search, X, Loader2, Package, Tag, ChevronLeft, ChevronRight } from "lucide-react";
import { funkosAPI } from "../services/api";

/**
 * Funko POP Database Search Component
 * Searches local Funko database (23,000+ entries)
 */
export default function FunkoSearch({ onSelectFunko, onClose }) {
  const [searchQuery, setSearchQuery] = useState("");
  const [seriesFilter, setSeriesFilter] = useState("");
  const [results, setResults] = useState([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);
  const [hasSearched, setHasSearched] = useState(false);
  const [page, setPage] = useState(1);
  const [totalPages, setTotalPages] = useState(1);
  const [total, setTotal] = useState(0);
  const [stats, setStats] = useState(null);

  // Load stats on mount
  useEffect(() => {
    funkosAPI.getStats().then(setStats).catch(() => {});
  }, []);

  const handleSearch = useCallback(async (pageNum = 1) => {
    if (!searchQuery.trim() && !seriesFilter.trim()) return;

    setLoading(true);
    setError(null);
    setHasSearched(true);

    try {
      const data = await funkosAPI.search({
        q: searchQuery || undefined,
        series: seriesFilter || undefined,
        page: pageNum,
        per_page: 20,
      });
      setResults(data.results || []);
      setTotalPages(data.pages || 1);
      setTotal(data.total || 0);
      setPage(pageNum);
    } catch (err) {
      setError("Failed to search Funkos. Please try again.");
      console.error(err);
    } finally {
      setLoading(false);
    }
  }, [searchQuery, seriesFilter]);

  const handleKeyPress = (e) => {
    if (e.key === "Enter") {
      handleSearch(1);
    }
  };

  const handlePageChange = (newPage) => {
    if (newPage >= 1 && newPage <= totalPages) {
      handleSearch(newPage);
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
      <div className="relative bg-zinc-900 rounded-2xl border border-zinc-800 w-full max-w-4xl mx-4 max-h-[85vh] flex flex-col shadow-2xl">
        {/* Header */}
        <div className="flex items-center justify-between p-6 border-b border-zinc-800">
          <div>
            <h3 className="font-comic text-2xl text-white">FUNKO DATABASE</h3>
            <p className="text-zinc-500 text-sm">
              Search {stats ? `${stats.total_funkos.toLocaleString()} Funkos` : 'the database'}
            </p>
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
              <label className="block text-xs text-zinc-500 mb-1">Search Title</label>
              <input
                type="text"
                value={searchQuery}
                onChange={(e) => setSearchQuery(e.target.value)}
                onKeyPress={handleKeyPress}
                placeholder="e.g., Spider-Man, Darth Vader"
                className="w-full px-4 py-3 bg-zinc-800 border border-zinc-700 rounded-xl text-white placeholder-zinc-500 focus:outline-none focus:border-orange-500 transition-colors"
              />
            </div>
            <div className="w-48">
              <label className="block text-xs text-zinc-500 mb-1">Series (optional)</label>
              <input
                type="text"
                value={seriesFilter}
                onChange={(e) => setSeriesFilter(e.target.value)}
                onKeyPress={handleKeyPress}
                placeholder="e.g., Pop! Marvel"
                className="w-full px-4 py-3 bg-zinc-800 border border-zinc-700 rounded-xl text-white placeholder-zinc-500 focus:outline-none focus:border-orange-500 transition-colors"
              />
            </div>
            <div className="flex items-end">
              <button
                onClick={() => handleSearch(1)}
                disabled={loading || (!searchQuery.trim() && !seriesFilter.trim())}
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
              <Package className="w-16 h-16 text-zinc-700 mx-auto mb-4" />
              <p className="text-zinc-500">Search for Funko POPs by name or series</p>
              <p className="text-zinc-600 text-sm mt-2">
                {stats ? `${stats.total_funkos.toLocaleString()} Funkos in database` : 'Loading...'}
              </p>
            </div>
          )}

          {hasSearched && results.length === 0 && !loading && (
            <div className="text-center py-12">
              <p className="text-zinc-500">No Funkos found. Try a different search.</p>
            </div>
          )}

          {results.length > 0 && (
            <>
              {/* Results count */}
              <div className="flex justify-between items-center mb-4">
                <p className="text-zinc-500 text-sm">
                  Showing {results.length} of {total.toLocaleString()} results
                </p>
              </div>

              {/* Grid */}
              <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
                {results.map((funko) => (
                  <div
                    key={funko.id}
                    onClick={() => onSelectFunko(funko)}
                    className="bg-zinc-800 rounded-xl border border-zinc-700 overflow-hidden cursor-pointer hover:border-orange-500 hover:shadow-lg hover:shadow-orange-500/10 transition-all group"
                  >
                    {/* Image */}
                    <div className="aspect-square bg-zinc-900 relative overflow-hidden">
                      {funko.image_url ? (
                        <img
                          src={funko.image_url}
                          alt={funko.title}
                          className="w-full h-full object-contain group-hover:scale-105 transition-transform duration-300"
                          onError={(e) => {
                            e.target.onerror = null;
                            e.target.src = `https://placehold.co/400x400/27272a/f97316?text=+`;
                          }}
                        />
                      ) : (
                        <div className="w-full h-full flex items-center justify-center">
                          <Package className="w-12 h-12 text-zinc-700" />
                        </div>
                      )}
                    </div>

                    {/* Info */}
                    <div className="p-3">
                      <h4 className="text-white font-bold text-sm line-clamp-2 mb-2">
                        {funko.title}
                      </h4>
                      {funko.series && funko.series.length > 0 && (
                        <div className="flex flex-wrap gap-1">
                          {funko.series.slice(0, 2).map((s, i) => (
                            <span
                              key={i}
                              className="inline-flex items-center gap-1 px-2 py-0.5 bg-zinc-700 rounded text-xs text-zinc-400"
                            >
                              <Tag className="w-3 h-3" />
                              {s.name.length > 15 ? s.name.substring(0, 15) + '...' : s.name}
                            </span>
                          ))}
                        </div>
                      )}
                    </div>
                  </div>
                ))}
              </div>

              {/* Pagination */}
              {totalPages > 1 && (
                <div className="flex items-center justify-center gap-4 mt-6">
                  <button
                    onClick={() => handlePageChange(page - 1)}
                    disabled={page <= 1 || loading}
                    className="p-2 bg-zinc-800 rounded-lg hover:bg-zinc-700 disabled:opacity-50 disabled:cursor-not-allowed"
                  >
                    <ChevronLeft className="w-5 h-5 text-zinc-400" />
                  </button>
                  <span className="text-zinc-400">
                    Page {page} of {totalPages}
                  </span>
                  <button
                    onClick={() => handlePageChange(page + 1)}
                    disabled={page >= totalPages || loading}
                    className="p-2 bg-zinc-800 rounded-lg hover:bg-zinc-700 disabled:opacity-50 disabled:cursor-not-allowed"
                  >
                    <ChevronRight className="w-5 h-5 text-zinc-400" />
                  </button>
                </div>
              )}
            </>
          )}
        </div>

        {/* Footer */}
        <div className="p-4 border-t border-zinc-800 text-center">
          <p className="text-xs text-zinc-600">
            Data sourced from community database
          </p>
        </div>
      </div>
    </div>
  );
}
