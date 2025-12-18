/**
 * FulfillmentManager - Unified fulfillment operations
 *
 * Per constitution_ui.json:
 * - WCAG 2.2 AA compliant
 * - Full keyboard navigation
 * - ARIA labels and live regions
 * - Focus management
 */

import React, { useState, useRef, useCallback } from 'react';
import OrdersTab from './tabs/OrdersTab';
import ShipmentsTab from './tabs/ShipmentsTab';
import RefundsTab from './tabs/RefundsTab';
import FulfillmentStats from './FulfillmentStats';

const TABS = {
  ORDERS: 'Orders',
  SHIPMENTS: 'Shipments',
  REFUNDS: 'Refunds',
};

export default function FulfillmentManager() {
  const [activeTab, setActiveTab] = useState(TABS.ORDERS);
  const announcerRef = useRef(null);

  // Screen reader announcements
  const announce = useCallback((message) => {
    if (announcerRef.current) {
      announcerRef.current.textContent = message;
      setTimeout(() => {
        if (announcerRef.current) {
          announcerRef.current.textContent = '';
        }
      }, 1000);
    }
  }, []);

  // Keyboard navigation for tabs
  const handleTabKeyDown = (e, tabKeys, currentIndex) => {
    switch (e.key) {
      case 'ArrowLeft':
        e.preventDefault();
        const prevIndex = currentIndex > 0 ? currentIndex - 1 : tabKeys.length - 1;
        setActiveTab(tabKeys[prevIndex]);
        break;
      case 'ArrowRight':
        e.preventDefault();
        const nextIndex = currentIndex < tabKeys.length - 1 ? currentIndex + 1 : 0;
        setActiveTab(tabKeys[nextIndex]);
        break;
      case 'Home':
        e.preventDefault();
        setActiveTab(tabKeys[0]);
        break;
      case 'End':
        e.preventDefault();
        setActiveTab(tabKeys[tabKeys.length - 1]);
        break;
      default:
        break;
    }
  };

  const renderContent = () => {
    switch (activeTab) {
      case TABS.ORDERS:
        return <OrdersTab announce={announce} />;
      case TABS.SHIPMENTS:
        return <ShipmentsTab announce={announce} />;
      case TABS.REFUNDS:
        return <RefundsTab announce={announce} />;
      default:
        return <OrdersTab announce={announce} />;
    }
  };

  const tabKeys = Object.values(TABS);
  const currentIndex = tabKeys.indexOf(activeTab);

  return (
    <div
      className="fulfillment-manager h-full flex flex-col"
      role="main"
      aria-label="Fulfillment Management"
    >
      {/* Screen reader announcer */}
      <div
        ref={announcerRef}
        role="status"
        aria-live="assertive"
        className="sr-only"
      />

      {/* Stats Dashboard Header */}
      <FulfillmentStats />

      {/* Tab Navigation */}
      <header className="flex border-b border-zinc-700 bg-zinc-900/50">
        <div
          role="tablist"
          aria-label="Fulfillment sections"
          className="flex"
        >
          {tabKeys.map((tab, index) => (
            <button
              key={tab}
              role="tab"
              id={`tab-${tab.toLowerCase()}`}
              aria-selected={activeTab === tab}
              aria-controls={`panel-${tab.toLowerCase()}`}
              tabIndex={activeTab === tab ? 0 : -1}
              onClick={() => setActiveTab(tab)}
              onKeyDown={(e) => handleTabKeyDown(e, tabKeys, index)}
              className={`px-6 py-3 text-sm font-medium transition-colors focus:outline-none focus:ring-2 focus:ring-orange-500/50 ${
                activeTab === tab
                  ? 'border-b-2 border-orange-500 text-white bg-zinc-800/50'
                  : 'text-zinc-400 hover:text-white hover:bg-zinc-800/30'
              }`}
            >
              {tab}
            </button>
          ))}
        </div>
      </header>

      {/* Tab Content */}
      <main
        id={`panel-${activeTab.toLowerCase()}`}
        role="tabpanel"
        aria-labelledby={`tab-${activeTab.toLowerCase()}`}
        className="flex-1 overflow-auto p-4"
      >
        {renderContent()}
      </main>

      {/* Keyboard shortcuts help */}
      <footer className="px-4 py-2 border-t border-zinc-800 bg-zinc-900/50 text-xs text-zinc-500 flex gap-4" aria-label="Keyboard shortcuts">
        <span><kbd className="px-1.5 py-0.5 bg-zinc-800 rounded text-zinc-400">Tab</kbd> Navigate</span>
        <span><kbd className="px-1.5 py-0.5 bg-zinc-800 rounded text-zinc-400">Enter</kbd> Select</span>
        <span><kbd className="px-1.5 py-0.5 bg-zinc-800 rounded text-zinc-400">Arrows</kbd> Move</span>
        <span><kbd className="px-1.5 py-0.5 bg-zinc-800 rounded text-zinc-400">Esc</kbd> Close</span>
      </footer>
    </div>
  );
}
