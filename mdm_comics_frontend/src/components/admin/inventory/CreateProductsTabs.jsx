/**
 * CreateProductsTabs - Sub-tab navigation for product creation
 * Implements FEAT_01: MDM Comics Inventory Create Products Screen
 *
 * Sub-tabs:
 * - Create Comics (STORY_02)
 * - Create Funkos (STORY_03)
 * - Create Supplies (STORY_04)
 * - Create Bundles (STORY_05, STORY_06)
 */
import React, { useState } from 'react';
import { BookOpen, Ghost, Package, Boxes } from 'lucide-react';
import CreateComicsForm from './CreateComicsForm';
import CreateFunkosForm from './CreateFunkosForm';
import CreateSuppliesForm from './CreateSuppliesForm';
import CreateBundlesTab from './CreateBundlesTab';

const SUB_TABS = [
  {
    id: 'comics',
    label: 'Create Comics',
    icon: BookOpen,
    color: '#e94560', // Comics accent color from design system
  },
  {
    id: 'funkos',
    label: 'Create Funkos',
    icon: Ghost,
    color: '#9b59b6', // Funkos accent color from design system
  },
  {
    id: 'supplies',
    label: 'Create Supplies',
    icon: Package,
    color: '#00b894', // Supplies accent color from design system
  },
  {
    id: 'bundles',
    label: 'Create Bundles',
    icon: Boxes,
    color: '#0d9488', // Bundles accent color from design system
  },
];

export default function CreateProductsTabs() {
  const [activeSubTab, setActiveSubTab] = useState('comics');

  const renderContent = () => {
    switch (activeSubTab) {
      case 'comics':
        return <CreateComicsForm />;
      case 'funkos':
        return <CreateFunkosForm />;
      case 'supplies':
        return <CreateSuppliesForm />;
      case 'bundles':
        return <CreateBundlesTab />;
      default:
        return <CreateComicsForm />;
    }
  };

  const activeTabConfig = SUB_TABS.find(tab => tab.id === activeSubTab);

  return (
    <div className="create-products-tabs">
      {/* Sub-tab navigation */}
      <div className="flex gap-2 mb-6 flex-wrap">
        {SUB_TABS.map((tab) => {
          const Icon = tab.icon;
          const isActive = activeSubTab === tab.id;

          return (
            <button
              key={tab.id}
              onClick={() => setActiveSubTab(tab.id)}
              className={`flex items-center gap-2 px-4 py-2 rounded-lg text-sm font-medium transition-all ${
                isActive
                  ? 'text-white shadow-lg'
                  : 'bg-zinc-800/50 text-zinc-400 hover:bg-zinc-800 hover:text-white'
              }`}
              style={isActive ? {
                backgroundColor: `${tab.color}20`,
                borderColor: `${tab.color}50`,
                border: `1px solid ${tab.color}50`,
                color: tab.color,
              } : {}}
            >
              <Icon className="w-4 h-4" />
              {tab.label}
            </button>
          );
        })}
      </div>

      {/* Active sub-tab content */}
      <div
        className="rounded-lg border p-6"
        style={{
          borderColor: `${activeTabConfig?.color}30`,
          backgroundColor: `${activeTabConfig?.color}05`,
        }}
      >
        {renderContent()}
      </div>
    </div>
  );
}
