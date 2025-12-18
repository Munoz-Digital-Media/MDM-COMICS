/**
 * AdminLayout - Full-page admin console layout with sidebar navigation
 * Phase 3: MDM Admin Console Inventory System v1.3.0
 * BUNDLE-001: Added nested navigation with feature flag support
 * FULFILLMENT-001: Consolidated Orders/Shipping/Refunds into Fulfillment module
 */
import React, { useState, useEffect } from 'react';
import {
  LayoutDashboard, Package, BarChart3, Search,
  X, Menu, ChevronLeft, ChevronDown, Users, Palette, Boxes, ClipboardList
} from 'lucide-react';
import AdminDashboard from './AdminDashboard';
import ProductList from './products/ProductList';
import InventorySummary from './reports/InventorySummary';
import UserList from './users/UserList';
import BrandAssets from './branding/BrandAssets';
import IngestionManager from './ingestion/IngestionManager';
import BundleList from './bundles/BundleList';
import FulfillmentManager from './fulfillment/FulfillmentManager';
import { FEATURES } from '../../features';

// Build navigation items with feature flag support
const buildNavItems = () => {
  const items = [
    { id: 'dashboard', label: 'Dashboard', icon: LayoutDashboard },
    {
      id: 'products',
      label: 'Products',
      icon: Package,
      children: FEATURES.BUNDLES_ENABLED ? [
        { id: 'bundles', label: 'Bundles', icon: Boxes },
      ] : undefined,
    },
    { id: 'ingestion', label: 'Ingestion', icon: Search },
    { id: 'fulfillment', label: 'Fulfillment', icon: ClipboardList },
    { id: 'users', label: 'Users', icon: Users },
    { id: 'branding', label: 'Branding', icon: Palette },
    { id: 'reports', label: 'Reports', icon: BarChart3 },
  ];

  return items;
};

const NAV_ITEMS = buildNavItems();

export default function AdminLayout({ onClose }) {
  const [activeSection, setActiveSection] = useState('dashboard');
  const [sidebarCollapsed, setSidebarCollapsed] = useState(false);
  const [mobileMenuOpen, setMobileMenuOpen] = useState(false);
  const [expandedItems, setExpandedItems] = useState(['products']); // Products expanded by default

  // Close mobile menu when section changes
  useEffect(() => {
    setMobileMenuOpen(false);
  }, [activeSection]);

  // Toggle expanded state for parent nav items
  const toggleExpanded = (itemId) => {
    setExpandedItems(prev =>
      prev.includes(itemId)
        ? prev.filter(id => id !== itemId)
        : [...prev, itemId]
    );
  };

  // Check if a section or any of its children is active
  const isItemOrChildActive = (item) => {
    if (activeSection === item.id) return true;
    if (item.children) {
      return item.children.some(child => activeSection === child.id);
    }
    return false;
  };

  const renderContent = () => {
    switch (activeSection) {
      case 'dashboard':
        return <AdminDashboard onNavigate={setActiveSection} />;
      case 'products':
        return <ProductList />;
      case 'bundles':
        return <BundleList />;
      case 'ingestion':
        return <IngestionManager />;
      case 'fulfillment':
        return <FulfillmentManager />;
      case 'users':
        return <UserList />;
      case 'branding':
        return <BrandAssets />;
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
            const hasChildren = item.children && item.children.length > 0;
            const isExpanded = expandedItems.includes(item.id);
            const isParentActive = isItemOrChildActive(item);

            return (
              <div key={item.id}>
                <button
                  onClick={() => {
                    if (hasChildren && !sidebarCollapsed) {
                      toggleExpanded(item.id);
                    }
                    setActiveSection(item.id);
                  }}
                  className={`
                    w-full flex items-center gap-3 px-3 py-2.5 rounded-lg transition-all
                    ${isActive
                      ? 'bg-red-500/20 text-red-400 border border-red-500/30'
                      : isParentActive && !isActive
                        ? 'bg-zinc-800/50 text-zinc-300'
                        : 'text-zinc-400 hover:bg-zinc-800 hover:text-white'
                    }
                  `}
                  title={sidebarCollapsed ? item.label : undefined}
                >
                  <Icon className="w-5 h-5 flex-shrink-0" />
                  {!sidebarCollapsed && (
                    <>
                      <span className="text-sm font-medium flex-1 text-left">{item.label}</span>
                      {hasChildren && (
                        <ChevronDown className={`w-4 h-4 transition-transform ${isExpanded ? 'rotate-180' : ''}`} />
                      )}
                    </>
                  )}
                </button>

                {/* Nested children */}
                {hasChildren && isExpanded && !sidebarCollapsed && (
                  <div className="ml-4 mt-1 space-y-1 border-l border-zinc-800 pl-2">
                    {item.children.map(child => {
                      const ChildIcon = child.icon;
                      const isChildActive = activeSection === child.id;
                      return (
                        <button
                          key={child.id}
                          onClick={() => setActiveSection(child.id)}
                          className={`
                            w-full flex items-center gap-3 px-3 py-2 rounded-lg transition-all text-sm
                            ${isChildActive
                              ? 'bg-red-500/20 text-red-400 border border-red-500/30'
                              : 'text-zinc-400 hover:bg-zinc-800 hover:text-white'
                            }
                          `}
                        >
                          <ChildIcon className="w-4 h-4 flex-shrink-0" />
                          <span className="font-medium">{child.label}</span>
                        </button>
                      );
                    })}
                  </div>
                )}
              </div>
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
              {(() => {
                // Check top-level items
                const topItem = NAV_ITEMS.find(n => n.id === activeSection);
                if (topItem) return topItem.label;
                // Check nested items
                for (const item of NAV_ITEMS) {
                  if (item.children) {
                    const child = item.children.find(c => c.id === activeSection);
                    if (child) return child.label;
                  }
                }
                return 'Dashboard';
              })()}
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
