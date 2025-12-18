/**
 * AdminDashboard - Overview with key metrics and quick actions
 * Phase 3: MDM Admin Console Inventory System v1.4.0
 */
import React, { useState, useEffect, useCallback } from 'react';
import {
  Package, DollarSign, AlertTriangle, QrCode,
  TrendingUp, TrendingDown, RefreshCw, Loader2, Info
} from 'lucide-react';
import { adminAPI } from '../../services/adminApi';
import PipelineStatus from './pipeline/PipelineStatus';

// App Version - Update these when deploying new versions
const APP_VERSION = 'v1.4.0';
const BACKEND_VERSION = 'v1.18.0';
const BUILD_DATE = '2025-12-14';

function StatCard({ title, value, subtitle, icon: Icon, trend, trendUp, color = 'orange', onClick }) {
  const colors = {
    orange: 'bg-orange-500/20 text-orange-400 border-orange-500/30',
    red: 'bg-red-500/20 text-red-400 border-red-500/30',
    green: 'bg-green-500/20 text-green-400 border-green-500/30',
    blue: 'bg-blue-500/20 text-blue-400 border-blue-500/30',
    purple: 'bg-purple-500/20 text-purple-400 border-purple-500/30',
  };

  return (
    <div
      onClick={onClick}
      className={`bg-zinc-900 border border-zinc-800 rounded-xl p-5 ${onClick ? 'cursor-pointer hover:border-zinc-700 transition-colors' : ''}`}
    >
      <div className="flex items-start justify-between mb-3">
        <div className={`p-2 rounded-lg border ${colors[color]}`}>
          <Icon className="w-5 h-5" />
        </div>
        {trend && (
          <div className={`flex items-center gap-1 text-xs ${trendUp ? 'text-green-400' : 'text-red-400'}`}>
            {trendUp ? <TrendingUp className="w-3 h-3" /> : <TrendingDown className="w-3 h-3" />}
            {trend}
          </div>
        )}
      </div>
      <p className="text-2xl font-bold text-white mb-1">{value}</p>
      <p className="text-sm text-zinc-500">{title}</p>
      {subtitle && <p className="text-xs text-zinc-600 mt-1">{subtitle}</p>}
    </div>
  );
}

function QuickAction({ label, icon: Icon, onClick, color = 'zinc' }) {
  const colors = {
    zinc: 'bg-zinc-800 hover:bg-zinc-700 text-zinc-300',
    orange: 'bg-orange-500/20 hover:bg-orange-500/30 text-orange-400 border border-orange-500/30',
    red: 'bg-red-500/20 hover:bg-red-500/30 text-red-400 border border-red-500/30',
  };

  return (
    <button
      onClick={onClick}
      className={`flex items-center gap-2 px-4 py-2.5 rounded-lg transition-colors ${colors[color]}`}
    >
      <Icon className="w-4 h-4" />
      <span className="text-sm font-medium">{label}</span>
    </button>
  );
}

export default function AdminDashboard({ onNavigate }) {
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [dashboard, setDashboard] = useState(null);
  const [lowStock, setLowStock] = useState([]);
  const [refreshing, setRefreshing] = useState(false);

  const fetchDashboard = useCallback(async () => {
    try {
      setError(null);
      const [dashData, lowStockData] = await Promise.all([
        adminAPI.getDashboard(),
        adminAPI.getLowStockItems(5),
      ]);
      setDashboard(dashData);
      setLowStock(lowStockData.items || []);
    } catch (err) {
      console.error('Dashboard fetch error:', err);
      setError(err.message);
    } finally {
      setLoading(false);
      setRefreshing(false);
    }
  }, []);

  useEffect(() => {
    fetchDashboard();
  }, [fetchDashboard]);

  const handleRefresh = () => {
    setRefreshing(true);
    fetchDashboard();
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
          className="px-4 py-2 bg-red-500/20 text-red-400 rounded-lg hover:bg-red-500/30 transition-colors"
        >
          Retry
        </button>
      </div>
    );
  }

  const stats = dashboard || {
    total_products: 0,
    total_value: 0,
    low_stock_count: 0,
    pending_queue: 0,
    recent_orders: 0,
  };

  return (
    <div className="space-y-6">
      {/* Header with refresh and version */}
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-3">
          <h2 className="text-xl font-semibold text-white">Overview</h2>
          <div className="flex items-center gap-2">
            <span className="text-xs px-2 py-0.5 bg-orange-500/20 text-orange-400 rounded-full border border-orange-500/30">
              {APP_VERSION}
            </span>
            <div className="group relative">
              <Info className="w-3.5 h-3.5 text-zinc-500 cursor-help" />
              <div className="absolute left-0 top-full mt-1 hidden group-hover:block z-50">
                <div className="bg-zinc-800 border border-zinc-700 rounded-lg p-2 shadow-lg whitespace-nowrap">
                  <p className="text-xs text-zinc-400">Frontend: <span className="text-white">{APP_VERSION}</span></p>
                  <p className="text-xs text-zinc-400">Backend: <span className="text-white">{BACKEND_VERSION}</span></p>
                  <p className="text-xs text-zinc-400">Build: <span className="text-white">{BUILD_DATE}</span></p>
                </div>
              </div>
            </div>
          </div>
        </div>
        <button
          onClick={handleRefresh}
          disabled={refreshing}
          className="flex items-center gap-2 px-3 py-1.5 bg-zinc-800 rounded-lg text-sm text-zinc-400 hover:text-white transition-colors disabled:opacity-50"
        >
          <RefreshCw className={`w-4 h-4 ${refreshing ? 'animate-spin' : ''}`} />
          Refresh
        </button>
      </div>

      {/* Stats Grid */}
      <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
        <StatCard
          title="Total Products"
          value={stats.total_products?.toLocaleString() || '0'}
          icon={Package}
          color="blue"
          onClick={() => onNavigate('products')}
        />
        <StatCard
          title="Inventory Value"
          value={`$${(stats.total_value || 0).toLocaleString(undefined, { minimumFractionDigits: 2 })}`}
          icon={DollarSign}
          color="green"
          onClick={() => onNavigate('reports')}
        />
        <StatCard
          title="Low Stock Alerts"
          value={stats.low_stock_count || 0}
          icon={AlertTriangle}
          color={stats.low_stock_count > 0 ? 'red' : 'green'}
          onClick={() => onNavigate('reports')}
        />
        <StatCard
          title="Pending Scans"
          value={stats.pending_queue || 0}
          icon={QrCode}
          color="purple"
          onClick={() => onNavigate('queue')}
        />
      </div>

      {/* Quick Actions */}
      <div className="bg-zinc-900 border border-zinc-800 rounded-xl p-5">
        <h3 className="text-sm font-semibold text-zinc-400 mb-4">Quick Actions</h3>
        <div className="flex flex-wrap gap-3">
          <QuickAction
            label="Process Queue"
            icon={QrCode}
            onClick={() => onNavigate('ingestion')}
            color="orange"
          />
          <QuickAction
            label="Low Stock Report"
            icon={AlertTriangle}
            onClick={() => onNavigate('reports')}
            color={stats.low_stock_count > 0 ? 'red' : 'zinc'}
          />
        </div>
      </div>

      {/* Low Stock Items */}
      {lowStock.length > 0 && (
        <div className="bg-zinc-900 border border-zinc-800 rounded-xl p-5">
          <div className="flex items-center justify-between mb-4">
            <h3 className="text-sm font-semibold text-zinc-400">Low Stock Items</h3>
            <button
              onClick={() => onNavigate('reports')}
              className="text-xs text-orange-400 hover:text-orange-300"
            >
              View All
            </button>
          </div>
          <div className="space-y-2">
            {lowStock.slice(0, 5).map(item => (
              <div
                key={item.product_id}
                className="flex items-center justify-between p-3 bg-zinc-800/50 rounded-lg"
              >
                <div className="flex items-center gap-3">
                  <div className={`w-2 h-2 rounded-full ${item.current_stock === 0 ? 'bg-red-500' : 'bg-yellow-500'}`} />
                  <span className="text-sm text-white truncate max-w-xs">{item.name}</span>
                </div>
                <span className={`text-sm font-medium ${item.current_stock === 0 ? 'text-red-400' : 'text-yellow-400'}`}>
                  {item.current_stock} left
                </span>
              </div>
            ))}
          </div>
        </div>
      )}

      {/* GCD Import Pipeline Status */}
      <PipelineStatus compact={false} />

      {/* Recent Activity */}
      <div className="grid md:grid-cols-2 gap-4">
        {/* Recent Orders */}
        <div className="bg-zinc-900 border border-zinc-800 rounded-xl p-5">
          <div className="flex items-center justify-between mb-4">
            <h3 className="text-sm font-semibold text-zinc-400">Recent Orders</h3>
            <button
              onClick={() => onNavigate('orders')}
              className="text-xs text-orange-400 hover:text-orange-300"
            >
              View All
            </button>
          </div>
          {stats.recent_orders_list?.length > 0 ? (
            <div className="space-y-2">
              {stats.recent_orders_list.slice(0, 5).map(order => (
                <div
                  key={order.id}
                  className="flex items-center justify-between p-3 bg-zinc-800/50 rounded-lg"
                >
                  <div>
                    <p className="text-sm text-white">{order.order_number || `#${order.id}`}</p>
                    <p className="text-xs text-zinc-500">{order.customer_email}</p>
                  </div>
                  <span className={`text-xs px-2 py-1 rounded-full ${
                    order.status === 'paid' ? 'bg-green-500/20 text-green-400' :
                    order.status === 'shipped' ? 'bg-blue-500/20 text-blue-400' :
                    'bg-zinc-700 text-zinc-400'
                  }`}>
                    {order.status}
                  </span>
                </div>
              ))}
            </div>
          ) : (
            <p className="text-sm text-zinc-500 text-center py-4">No recent orders</p>
          )}
        </div>

        {/* System Status */}
        <div className="bg-zinc-900 border border-zinc-800 rounded-xl p-5">
          <h3 className="text-sm font-semibold text-zinc-400 mb-4">System Status</h3>
          <div className="space-y-3">
            <div className="flex items-center justify-between">
              <span className="text-sm text-zinc-400">Database</span>
              <span className="text-xs px-2 py-1 bg-green-500/20 text-green-400 rounded-full">Connected</span>
            </div>
            <div className="flex items-center justify-between">
              <span className="text-sm text-zinc-400">Price Sync</span>
              <span className="text-xs px-2 py-1 bg-green-500/20 text-green-400 rounded-full">Active</span>
            </div>
            <div className="flex items-center justify-between">
              <span className="text-sm text-zinc-400">Stock Cleanup</span>
              <span className="text-xs px-2 py-1 bg-green-500/20 text-green-400 rounded-full">Running</span>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}
