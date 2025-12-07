/**
 * AdminLayout - Full-page admin console layout with sidebar navigation
 * Phase 3: MDM Admin Console Inventory System v1.3.0
 */
import React, { useState, useEffect } from 'react';
import {
  LayoutDashboard, Package, ShoppingCart, BarChart3,
  QrCode, X, Menu, ChevronLeft, AlertTriangle, Truck, Camera
} from 'lucide-react';
import AdminDashboard from './AdminDashboard';
import ProductList from './products/ProductList';
import ScanQueue from './queue/ScanQueue';
import OrderList from './orders/OrderList';
import InventorySummary from './reports/InventorySummary';
import ShipmentList from './shipping/ShipmentList';
import ScannerApp from '../scanner/ScannerApp';

const NAV_ITEMS = [
  { id: 'dashboard', label: 'Dashboard', icon: LayoutDashboard },
  { id: 'products', label: 'Products', icon: Package },
  { id: 'scanner', label: 'Scanner', icon: Camera },
  { id: 'queue', label: 'Scan Queue', icon: QrCode },
  { id: 'orders', label: 'Orders', icon: ShoppingCart },
  { id: 'shipping', label: 'Shipping', icon: Truck },
  { id: 'reports', label: 'Reports', icon: BarChart3 },
];

export default function AdminLayout({ onClose }) {
  const [activeSection, setActiveSection] = useState('dashboard');
  const [sidebarCollapsed, setSidebarCollapsed] = useState(false);
  const [mobileMenuOpen, setMobileMenuOpen] = useState(false);

  // Close mobile menu when section changes
  useEffect(() => {
    setMobileMenuOpen(false);
  }, [activeSection]);

  const renderContent = () => {
    switch (activeSection) {
      case 'dashboard':
        return <AdminDashboard onNavigate={setActiveSection} />;
      case 'products':
        return <ProductList />;
      case 'scanner':
        return <ScannerApp onClose={() => setActiveSection('dashboard')} embedded />;
      case 'queue':
        return <ScanQueue />;
      case 'orders':
        return <OrderList />;
      case 'shipping':
        return <ShipmentList />;
      case 'reports':
        return <InventorySummary />;
      default:
        return <AdminDashboard onNavigate={setActiveSection} />;
    }
  };

  return (
    <div className="fixed inset-0 z-50 bg-zinc-950 flex">
      {/* Mobile overlay */}
      {mobileMenuOpen && (
        <div
          className="fixed inset-0 bg-black/50 z-40 md:hidden"
          onClick={() => setMobileMenuOpen(false)}
        />
      )}

      {/* Sidebar */}
      <aside className={`
        fixed md:relative z-50 h-full bg-zinc-900 border-r border-zinc-800
        transition-all duration-300 flex flex-col
        ${mobileMenuOpen ? 'translate-x-0' : '-translate-x-full md:translate-x-0'}
        ${sidebarCollapsed ? 'w-16' : 'w-64'}
      `}>
        {/* Logo / Header */}
        <div className="p-4 border-b border-zinc-800 flex items-center justify-between">
          {!sidebarCollapsed && (
            <div className="flex items-center gap-2">
              <div className="w-8 h-8 bg-red-500/20 border border-red-500/30 rounded-lg flex items-center justify-center">
                <span className="font-comic text-red-400 text-sm">M</span>
              </div>
              <span className="font-bold text-white">Admin</span>
            </div>
          )}
          <button
            onClick={() => setSidebarCollapsed(!sidebarCollapsed)}
            className="p-1.5 hover:bg-zinc-800 rounded-lg transition-colors hidden md:block"
          >
            <ChevronLeft className={`w-4 h-4 text-zinc-400 transition-transform ${sidebarCollapsed ? 'rotate-180' : ''}`} />
          </button>
        </div>

        {/* Navigation */}
        <nav className="flex-1 p-2 space-y-1">
          {NAV_ITEMS.map(item => {
            const Icon = item.icon;
            const isActive = activeSection === item.id;
            return (
              <button
                key={item.id}
                onClick={() => setActiveSection(item.id)}
                className={`
                  w-full flex items-center gap-3 px-3 py-2.5 rounded-lg transition-all
                  ${isActive
                    ? 'bg-red-500/20 text-red-400 border border-red-500/30'
                    : 'text-zinc-400 hover:bg-zinc-800 hover:text-white'
                  }
                `}
                title={sidebarCollapsed ? item.label : undefined}
              >
                <Icon className="w-5 h-5 flex-shrink-0" />
                {!sidebarCollapsed && <span className="text-sm font-medium">{item.label}</span>}
              </button>
            );
          })}
        </nav>

        {/* Close button at bottom */}
        <div className="p-4 border-t border-zinc-800">
          <button
            onClick={onClose}
            className={`
              w-full flex items-center gap-3 px-3 py-2.5 rounded-lg
              text-zinc-400 hover:bg-zinc-800 hover:text-white transition-colors
            `}
          >
            <X className="w-5 h-5 flex-shrink-0" />
            {!sidebarCollapsed && <span className="text-sm font-medium">Exit Admin</span>}
          </button>
        </div>
      </aside>

      {/* Main Content */}
      <main className="flex-1 flex flex-col overflow-hidden">
        {/* Top bar */}
        <header className="h-14 bg-zinc-900 border-b border-zinc-800 flex items-center justify-between px-4">
          <div className="flex items-center gap-4">
            <button
              onClick={() => setMobileMenuOpen(true)}
              className="p-2 hover:bg-zinc-800 rounded-lg md:hidden"
            >
              <Menu className="w-5 h-5 text-zinc-400" />
            </button>
            <h1 className="text-lg font-semibold text-white">
              {NAV_ITEMS.find(n => n.id === activeSection)?.label || 'Dashboard'}
            </h1>
          </div>
          <div className="flex items-center gap-2">
            {/* Quick action buttons can go here */}
          </div>
        </header>

        {/* Content area */}
        <div className="flex-1 overflow-auto p-4 md:p-6">
          {renderContent()}
        </div>
      </main>
    </div>
  );
}
