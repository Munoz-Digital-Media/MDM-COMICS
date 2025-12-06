/**
 * InventorySummary - Inventory value, counts, and low stock alerts
 * Phase 3: MDM Admin Console Inventory System v1.3.0
 */
import React, { useState, useEffect } from 'react';
import {
  DollarSign, Package, AlertTriangle, TrendingUp, TrendingDown,
  Loader2, RefreshCw, Download
} from 'lucide-react';
import { adminAPI } from '../../../services/adminApi';

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
          <h3 className="text-sm font-semibold text-zinc-400 mb-4">Price Changes (Last 7 Days)</h3>

          {priceChanges.length === 0 ? (
            <p className="text-sm text-zinc-500 text-center py-4">No significant price changes</p>
          ) : (
            <div className="space-y-2 max-h-80 overflow-auto">
              {priceChanges.map((change, idx) => (
                <div
                  key={idx}
                  className="flex items-center justify-between p-3 bg-zinc-800/50 rounded-lg"
                >
                  <div className="min-w-0 flex-1">
                    <p className="text-sm text-white truncate">{change.entity_name}</p>
                    <p className="text-xs text-zinc-500 capitalize">{change.entity_type} • {change.field}</p>
                  </div>
                  <div className="flex items-center gap-2 ml-2">
                    <span className="text-xs text-zinc-500">${change.old_value?.toFixed(2)}</span>
                    <span className="text-zinc-600">→</span>
                    <span className="text-xs text-white">${change.new_value?.toFixed(2)}</span>
                    <div className={`flex items-center gap-0.5 ${
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
                </div>
              ))}
            </div>
          )}

          <div className="mt-4 pt-4 border-t border-zinc-800">
            <p className="text-xs text-zinc-500">
              Showing changes greater than 10%
            </p>
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
    </div>
  );
}
