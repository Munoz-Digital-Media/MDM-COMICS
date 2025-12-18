/**
 * FilterBar - Search and filter controls
 */

import React from 'react';
import { Search, X } from 'lucide-react';

export default function FilterBar({ filters, onFilterChange }) {
  return (
    <div className="flex flex-wrap items-center gap-3 mb-4" role="search" aria-label="Filter results">
      {filters.map((filter) => {
        if (filter.type === 'search') {
          return (
            <div key={filter.name} className="relative flex-1 min-w-[200px] max-w-md">
              <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-zinc-500" />
              <input
                type="text"
                placeholder={filter.placeholder || 'Search...'}
                value={filter.value}
                onChange={(e) => onFilterChange(filter.name, e.target.value)}
                aria-label={filter.label}
                className="w-full pl-10 pr-8 py-2 bg-zinc-800 border border-zinc-700 rounded-lg text-white placeholder-zinc-500 focus:outline-none focus:border-orange-500 focus:ring-1 focus:ring-orange-500/50"
              />
              {filter.value && (
                <button
                  onClick={() => onFilterChange(filter.name, '')}
                  className="absolute right-3 top-1/2 -translate-y-1/2 text-zinc-500 hover:text-white"
                  aria-label="Clear search"
                >
                  <X className="w-4 h-4" />
                </button>
              )}
            </div>
          );
        }

        if (filter.type === 'select') {
          return (
            <select
              key={filter.name}
              value={filter.value}
              onChange={(e) => onFilterChange(filter.name, e.target.value)}
              aria-label={filter.label}
              className="px-3 py-2 bg-zinc-800 border border-zinc-700 rounded-lg text-white focus:outline-none focus:border-orange-500 cursor-pointer"
            >
              {filter.options.map((opt) => (
                <option key={opt.value} value={opt.value}>
                  {opt.label}
                </option>
              ))}
            </select>
          );
        }

        return null;
      })}
    </div>
  );
}
