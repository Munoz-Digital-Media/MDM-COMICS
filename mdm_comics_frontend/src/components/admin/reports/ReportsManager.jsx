/**
 * ReportsManager - Tabbed interface for Reports and Users
 * Consolidates reporting and user management functionality
 */
import React, { useState } from 'react';
import InventorySummary from './InventorySummary';
import UserList from '../users/UserList';

const TABS = {
  INVENTORY: 'Inventory',
  USERS: 'Users',
};

export default function ReportsManager() {
  const [activeTab, setActiveTab] = useState(TABS.INVENTORY);

  const availableTabs = [TABS.INVENTORY, TABS.USERS];

  const renderContent = () => {
    switch (activeTab) {
      case TABS.INVENTORY:
        return <InventorySummary />;
      case TABS.USERS:
        return <UserList />;
      default:
        return <InventorySummary />;
    }
  };

  return (
    <div className="reports-manager">
      <header className="flex border-b border-zinc-700 mb-4">
        {availableTabs.map((tab) => (
          <button
            key={tab}
            onClick={() => setActiveTab(tab)}
            className={`px-6 py-3 text-sm font-medium transition-colors ${
              activeTab === tab
                ? 'border-b-2 border-orange-500 text-white bg-zinc-800/50'
                : 'text-zinc-400 hover:text-white hover:bg-zinc-800/30'
            }`}
          >
            {tab}
          </button>
        ))}
      </header>
      <main>
        {renderContent()}
      </main>
    </div>
  );
}
