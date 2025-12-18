/**
 * FulfillmentStats - Dashboard header with key metrics
 */

import { API_BASE } from '../../../config/api.config.js';
import React, { useState, useEffect } from 'react';
import { ShoppingCart, Truck, RefreshCw, AlertTriangle, CheckCircle, Clock } from 'lucide-react';

export default function FulfillmentStats() {
  const [stats, setStats] = useState({
    awaitingFulfillment: 0,
    shippedToday: 0,
    pendingRefunds: 0,
    deliveredWeek: 0,
    exceptions: 0,
  });
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    const fetchStats = async () => {
      try {
        

        // Fetch order stats
        const ordersRes = await fetch(`${API_BASE}/admin/orders?status=paid&limit=1`, {
          credentials: 'include',
        });
        const ordersData = await ordersRes.json();

        // Fetch refund stats
        const refundsRes = await fetch(`${API_BASE}/admin/refunds/stats`, {
          credentials: 'include',
        });
        const refundsData = await refundsRes.json();

        setStats({
          awaitingFulfillment: ordersData.total || 0,
          shippedToday: 0, // Would need shipments endpoint with date filter
          pendingRefunds: refundsData.pending_review || 0,
          deliveredWeek: 0,
          exceptions: refundsData.pending_vendor_credit || 0,
        });
      } catch (err) {
        console.error('Failed to load stats:', err);
      } finally {
        setLoading(false);
      }
    };

    fetchStats();
  }, []);

  const statCards = [
    {
      label: 'Awaiting Fulfillment',
      value: stats.awaitingFulfillment,
      icon: Clock,
      color: 'yellow',
      urgent: stats.awaitingFulfillment > 0,
    },
    {
      label: 'Shipped Today',
      value: stats.shippedToday,
      icon: Truck,
      color: 'blue',
    },
    {
      label: 'Pending Refunds',
      value: stats.pendingRefunds,
      icon: RefreshCw,
      color: 'orange',
      urgent: stats.pendingRefunds > 0,
    },
    {
      label: 'Needs Attention',
      value: stats.exceptions,
      icon: AlertTriangle,
      color: 'red',
      urgent: stats.exceptions > 0,
    },
  ];

  const colorClasses = {
    yellow: 'bg-yellow-500/10 text-yellow-400 border-yellow-500/30',
    blue: 'bg-blue-500/10 text-blue-400 border-blue-500/30',
    orange: 'bg-orange-500/10 text-orange-400 border-orange-500/30',
    red: 'bg-red-500/10 text-red-400 border-red-500/30',
    green: 'bg-green-500/10 text-green-400 border-green-500/30',
  };

  return (
    <div className="grid grid-cols-2 md:grid-cols-4 gap-3 p-4 bg-zinc-900/30 border-b border-zinc-800">
      {statCards.map((stat) => (
        <div
          key={stat.label}
          className={`flex items-center gap-3 p-3 rounded-lg border ${colorClasses[stat.color]} ${
            stat.urgent ? 'animate-pulse' : ''
          }`}
        >
          <stat.icon className="w-5 h-5 flex-shrink-0" />
          <div className="min-w-0">
            <p className="text-lg font-bold truncate">
              {loading ? '...' : stat.value}
            </p>
            <p className="text-xs opacity-80 truncate">{stat.label}</p>
          </div>
        </div>
      ))}
    </div>
  );
}
