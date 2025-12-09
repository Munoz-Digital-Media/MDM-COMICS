/**
 * InventorySummary - Inventory value, counts, and low stock alerts
 * Phase 3: MDM Admin Console Inventory System v1.3.0
 */
import React, { useState, useEffect, useMemo } from 'react';
import {
  DollarSign, Package, AlertTriangle, TrendingUp, TrendingDown,
  Loader2, RefreshCw, Download, ArrowUpDown, ChevronUp, ChevronDown,
  ChevronLeft, ChevronRight
} from 'lucide-react';
import { adminAPI } from '../../../services/adminApi';
import PriceChangeDrawer from './PriceChangeDrawer';

function SummaryCard({ title, value, subtitle, icon: Icon, color = 'zinc' }) {
  const colors = {
    zinc: 'bg-zinc-800 border-zinc-700',
    orange: 'bg-orange-500/20 border-orange-500/30',
    green: 'bg-green-500/20 border-green-500/30',
    red: 'bg-red-500/20 border-red-500/30',
    blue: 'bg-blue-500/20 border-blue-500/30',
    purple: 'bg-purple-500/20 border-purple-500/30',
  };

  const iconColors = {
    zinc: 'text-zinc-400',
    orange: 'text-orange-400',
    green: 'text-green-400',
    red: 'text-red-400',
    blue: 'text-blue-400',
    purple: 'text-purple-400',
  };

  return (
    <div className={`p-5 rounded-xl border ${colors[color]}`}>
      <div className="flex items-center gap-3 mb-3">
        <div className={`p-2 rounded-lg ${colors[color]}`}>
          <Icon className={`w-5 h-5 ${iconColors[color]}`} />
        </div>
        <span className="text-sm text-zinc-400">{title}</span>
      </div>
      <p className="text-2xl font-bold text-white mb-1">{value}</p>
      {subtitle && <p className="text-xs text-zinc-500">{subtitle}</p>}
    </div>
  );
}

export default function InventorySummary() {
  const [summary, setSummary] = useState(null);
  const [lowStock, setLowStock] = useState([]);
  const [priceChanges, setPriceChanges] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [lowStockThreshold, setLowStockThreshold] = useState(5);
  const [refreshing, setRefreshing] = useState(false);

  // Price Changes filter/sort state
  const [priceFilter, setPriceFilter] = useState('all'); // 'all', 'comic', 'funko'
  const [priceSort, setPriceSort] = useState({ field: 'change_pct', dir: 'desc' });

  // Drawer state
  const [drawerOpen, setDrawerOpen] = useState(false);
  const [selectedPriceChange, setSelectedPriceChange] = useState(null);

  // Pagination state
  const [pricePage, setPricePage] = useState(1);
  const PRICE_PAGE_SIZE = 10;

  const fetchData = async () => {
    try {
      setError(null);
      const [summaryData, lowStockData, priceData] = await Promise.all([
        adminAPI.getInventorySummary(),
        adminAPI.getLowStockItems(lowStockThreshold),
        adminAPI.getPriceChanges(7, 10),
      ]);
      setSummary(summaryData);
      setLowStock(lowStockData.items || []);
      setPriceChanges(priceData.changes || []);
    } catch (err) {
      setError(err.message);
    } finally {
      setLoading(false);
      setRefreshing(false);
    }
  };

  useEffect(() => {
    fetchData();
  }, [lowStockThreshold]);

  const handleRefresh = () => {
    setRefreshing(true);
    fetchData();
  };

  // Filtered and sorted price changes
  const filteredPriceChanges = useMemo(() => {
    let filtered = [...priceChanges];

    // Apply filter
    if (priceFilter !== 'all') {
      filtered = filtered.filter(c => c.entity_type === priceFilter);
    }

    // Apply sort
    filtered.sort((a, b) => {
      let aVal, bVal;
      switch (priceSort.field) {
        case 'name':
          aVal = (a.entity_name || '').toLowerCase();
          bVal = (b.entity_name || '').toLowerCase();
          return priceSort.dir === 'asc' ? aVal.localeCompare(bVal) : bVal.localeCompare(aVal);
        case 'old_value':
          aVal = a.old_value || 0;
          bVal = b.old_value || 0;
          break;
        case 'new_value':
          aVal = a.new_value || 0;
          bVal = b.new_value || 0;
          break;
        case 'change_pct':
        default:
          // Sort by actual value so gains and losses are separated
          aVal = a.change_pct || 0;
          bVal = b.change_pct || 0;
          break;
      }
      return priceSort.dir === 'asc' ? aVal - bVal : bVal - aVal;
    });

    return filtered;
  }, [priceChanges, priceFilter, priceSort]);

  // Paginated price changes
  const paginatedPriceChanges = useMemo(() => {
    const start = (pricePage - 1) * PRICE_PAGE_SIZE;
    return filteredPriceChanges.slice(start, start + PRICE_PAGE_SIZE);
  }, [filteredPriceChanges, pricePage]);

  const totalPricePages = Math.ceil(filteredPriceChanges.length / PRICE_PAGE_SIZE);

  // Reset page when filter changes
  useEffect(() => {
    setPricePage(1);
  }, [priceFilter]);

  const handlePriceSort = (field) => {
    setPriceSort(prev => {
      // If clicking same field, toggle direction
      if (prev.field === field) {
        return { field, dir: prev.dir === 'desc' ? 'asc' : 'desc' };
      }
      // New field: default to desc (biggest first) for numeric, asc for name
      return { field, dir: field === 'name' ? 'asc' : 'desc' };
    });
  };

  const SortIcon = ({ field }) => {
    if (priceSort.field !== field) {
      return <ArrowUpDown className="w-3 h-3 text-zinc-600" />;
    }
    return priceSort.dir === 'asc'
      ? <ChevronUp className="w-3 h-3 text-orange-400" />
      : <ChevronDown className="w-3 h-3 text-orange-400" />;
  };

  if (loading) {
    return (
      <div className="flex items-center justify-center h-64">
        <Loader2 className="w-8 h-8 text-orange-500 animate-spin" />
      </div>
    );
  }

  if (error) {
    return (
      <div className="bg-red-500/10 border border-red-500/20 rounded-xl p-6 text-center">
        <AlertTriangle className="w-8 h-8 text-red-400 mx-auto mb-2" />
        <p className="text-red-400 mb-4">{error}</p>
        <button
          onClick={handleRefresh}
          className="px-4 py-2 bg-red-500/20 text-red-400 rounded-lg hover:bg-red-500/30"
        >
          Retry
        </button>
      </div>
    );
  }

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex items-center justify-between">
        <h2 className="text-xl font-semibold text-white">Inventory Report</h2>
        <button
          onClick={handleRefresh}
          disabled={refreshing}
          className="flex items-center gap-2 px-3 py-1.5 bg-zinc-800 rounded-lg text-sm text-zinc-400 hover:text-white disabled:opacity-50"
        >
          <RefreshCw className={`w-4 h-4 ${refreshing ? 'animate-spin' : ''}`} />
          Refresh
        </button>
      </div>

      {/* Summary Cards */}
      <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
        <SummaryCard
          title="Total Products"
          value={summary?.total_products?.toLocaleString() || '0'}
          icon={Package}
          color="blue"
        />
        <SummaryCard
          title="Total Stock Units"
          value={summary?.total_stock_units?.toLocaleString() || '0'}
          icon={Package}
          color="purple"
        />
        <SummaryCard
          title="Retail Value"
          value={`$${(summary?.total_retail_value || 0).toLocaleString(undefined, { minimumFractionDigits: 2 })}`}
          subtitle="At current prices"
          icon={DollarSign}
          color="green"
        />
        <SummaryCard
          title="Cost Value"
          value={`$${(summary?.total_cost_value || 0).toLocaleString(undefined, { minimumFractionDigits: 2 })}`}
          subtitle="Original prices"
          icon={DollarSign}
          color="orange"
        />
      </div>

      {/* Category Breakdown */}
      {summary?.by_category && Object.keys(summary.by_category).length > 0 && (
        <div className="bg-zinc-900 border border-zinc-800 rounded-xl p-5">
          <h3 className="text-sm font-semibold text-zinc-400 mb-4">By Category</h3>
          <div className="space-y-3">
            {Object.entries(summary.by_category).map(([category, data]) => (
              <div key={category} className="flex items-center justify-between">
                <div className="flex items-center gap-3">
                  <div className={`w-3 h-3 rounded-full ${
                    category === 'comics' ? 'bg-blue-500' : 'bg-purple-500'
                  }`} />
                  <span className="text-sm text-white capitalize">{category}</span>
                </div>
                <div className="text-right">
                  <span className="text-sm text-white">{data.count} items</span>
                  <span className="text-sm text-zinc-500 ml-3">
                    ${data.value?.toLocaleString(undefined, { minimumFractionDigits: 2 })}
                  </span>
                </div>
              </div>
            ))}
          </div>
        </div>
      )}

      {/* Two Column Layout */}
      <div className="grid md:grid-cols-2 gap-6">
        {/* Low Stock Items */}
        <div className="bg-zinc-900 border border-zinc-800 rounded-xl p-5">
          <div className="flex items-center justify-between mb-4">
            <h3 className="text-sm font-semibold text-zinc-400">Low Stock Items</h3>
            <div className="flex items-center gap-2">
              <span className="text-xs text-zinc-500">Threshold:</span>
              <select
                value={lowStockThreshold}
                onChange={(e) => setLowStockThreshold(Number(e.target.value))}
                className="bg-zinc-800 border border-zinc-700 rounded px-2 py-1 text-xs text-white"
              >
                <option value={3}>3</option>
                <option value={5}>5</option>
                <option value={10}>10</option>
                <option value={20}>20</option>
              </select>
            </div>
          </div>

          {lowStock.length === 0 ? (
            <p className="text-sm text-zinc-500 text-center py-4">No low stock items</p>
          ) : (
            <div className="space-y-2 max-h-80 overflow-auto">
              {lowStock.map(item => (
                <div
                  key={item.product_id}
                  className="flex items-center justify-between p-3 bg-zinc-800/50 rounded-lg"
                >
                  <div className="flex items-center gap-2 min-w-0">
                    <div className={`w-2 h-2 rounded-full flex-shrink-0 ${
                      item.current_stock === 0 ? 'bg-red-500' : 'bg-yellow-500'
                    }`} />
                    <span className="text-sm text-white truncate">{item.name}</span>
                  </div>
                  <span className={`text-sm font-medium flex-shrink-0 ml-2 ${
                    item.current_stock === 0 ? 'text-red-400' : 'text-yellow-400'
                  }`}>
                    {item.current_stock}
                  </span>
                </div>
              ))}
            </div>
          )}

          <div className="mt-4 pt-4 border-t border-zinc-800">
            <p className="text-xs text-zinc-500">
              {lowStock.length} items at or below {lowStockThreshold} units
            </p>
          </div>
        </div>

        {/* Recent Price Changes */}
        <div className="bg-zinc-900 border border-zinc-800 rounded-xl p-5">
          <div className="flex items-center justify-between mb-4">
            <h3 className="text-sm font-semibold text-zinc-400">Price Changes (Last 7 Days)</h3>
            {/* Filter Buttons */}
            <div className="flex gap-1">
              {[
                { key: 'all', label: 'ALL' },
                { key: 'comic', label: 'COMICS' },
                { key: 'funko', label: 'FUNKOS' },
              ].map(({ key, label }) => (
                <button
                  key={key}
                  onClick={() => setPriceFilter(key)}
                  className={`px-2 py-1 text-xs rounded transition-colors ${
                    priceFilter === key
                      ? 'bg-orange-500 text-white'
                      : 'bg-zinc-800 text-zinc-400 hover:bg-zinc-700'
                  }`}
                >
                  {label}
                </button>
              ))}
            </div>
          </div>

          {/* Sortable Headers */}
          <div className="flex items-center gap-2 mb-2 px-3 py-2 bg-zinc-800/30 rounded-lg text-xs">
            <button
              onClick={() => handlePriceSort('name')}
              className="flex items-center gap-1 text-zinc-400 hover:text-white flex-1 min-w-0"
            >
              Name <SortIcon field="name" />
            </button>
            <button
              onClick={() => handlePriceSort('old_value')}
              className="flex items-center gap-1 text-zinc-400 hover:text-white w-16 justify-end"
            >
              Prev <SortIcon field="old_value" />
            </button>
            <span className="w-4 text-center text-zinc-600">→</span>
            <button
              onClick={() => handlePriceSort('new_value')}
              className="flex items-center gap-1 text-zinc-400 hover:text-white w-16 justify-start"
            >
              <SortIcon field="new_value" /> Curr
            </button>
            <button
              onClick={() => handlePriceSort('change_pct')}
              className="flex items-center gap-1 text-zinc-400 hover:text-white w-16 justify-end"
            >
              Δ% <SortIcon field="change_pct" />
            </button>
          </div>

          {filteredPriceChanges.length === 0 ? (
            <p className="text-sm text-zinc-500 text-center py-4">
              {priceChanges.length === 0 ? 'No significant price changes' : 'No matches for filter'}
            </p>
          ) : (
            <div className="space-y-2 max-h-80 overflow-auto">
              {paginatedPriceChanges.map((change, idx) => (
                <div
                  key={idx}
                  onClick={() => {
                    setSelectedPriceChange(change);
                    setDrawerOpen(true);
                  }}
                  className="flex items-center gap-2 p-3 bg-zinc-800/50 rounded-lg cursor-pointer hover:bg-zinc-700/50 transition-colors"
                >
                  <div className="min-w-0 flex-1">
                    <p className="text-sm text-white truncate">{change.entity_name}</p>
                    <p className="text-xs text-zinc-500 capitalize">{change.entity_type} • {change.field}</p>
                  </div>
                  <span className="text-xs text-zinc-500 w-16 text-right">${change.old_value?.toFixed(2)}</span>
                  <span className="w-4 text-center text-zinc-600">→</span>
                  <span className="text-xs text-white w-16">${change.new_value?.toFixed(2)}</span>
                  <div className={`flex items-center gap-0.5 w-16 justify-end ${
                    change.change_pct > 0 ? 'text-green-400' : 'text-red-400'
                  }`}>
                    {change.change_pct > 0 ? (
                      <TrendingUp className="w-3 h-3" />
                    ) : (
                      <TrendingDown className="w-3 h-3" />
                    )}
                    <span className="text-xs">
                      {change.change_pct > 0 ? '+' : ''}{change.change_pct?.toFixed(1)}%
                    </span>
                  </div>
                </div>
              ))}
            </div>
          )}

          {/* Pagination Controls */}
          <div className="mt-4 pt-4 border-t border-zinc-800 flex items-center justify-between">
            <p className="text-xs text-zinc-500">
              {filteredPriceChanges.length} of {priceChanges.length} changes (≥10%)
            </p>
            {totalPricePages > 1 && (
              <div className="flex items-center gap-2">
                <button
                  onClick={() => setPricePage(p => Math.max(1, p - 1))}
                  disabled={pricePage === 1}
                  className="p-1 rounded bg-zinc-800 text-zinc-400 hover:text-white hover:bg-zinc-700 disabled:opacity-40 disabled:cursor-not-allowed"
                >
                  <ChevronLeft className="w-4 h-4" />
                </button>
                <span className="text-xs text-zinc-400">
                  {pricePage} / {totalPricePages}
                </span>
                <button
                  onClick={() => setPricePage(p => Math.min(totalPricePages, p + 1))}
                  disabled={pricePage === totalPricePages}
                  className="p-1 rounded bg-zinc-800 text-zinc-400 hover:text-white hover:bg-zinc-700 disabled:opacity-40 disabled:cursor-not-allowed"
                >
                  <ChevronRight className="w-4 h-4" />
                </button>
              </div>
            )}
          </div>
        </div>
      </div>

      {/* Profit Margin */}
      {summary && summary.total_retail_value > 0 && (
        <div className="bg-zinc-900 border border-zinc-800 rounded-xl p-5">
          <h3 className="text-sm font-semibold text-zinc-400 mb-4">Potential Margin</h3>
          <div className="flex items-center gap-8">
            <div>
              <p className="text-2xl font-bold text-green-400">
                ${(summary.potential_margin || (summary.total_retail_value - summary.total_cost_value)).toLocaleString(undefined, { minimumFractionDigits: 2 })}
              </p>
              <p className="text-xs text-zinc-500">Gross margin (retail - cost)</p>
            </div>
            <div>
              <p className="text-2xl font-bold text-white">
                {(((summary.total_retail_value - summary.total_cost_value) / summary.total_retail_value) * 100).toFixed(1)}%
              </p>
              <p className="text-xs text-zinc-500">Margin percentage</p>
            </div>
          </div>
        </div>
      )}

      {/* Price Change Drawer */}
      <PriceChangeDrawer
        isOpen={drawerOpen}
        onClose={() => {
          setDrawerOpen(false);
          setSelectedPriceChange(null);
        }}
        priceChange={selectedPriceChange}
        onCreateProduct={async (productData) => {
          // TODO: Integrate with product creation API
          console.log('Create product:', productData);
          alert('Product creation coming soon! Data logged to console.');
        }}
      />
    </div>
  );
}
