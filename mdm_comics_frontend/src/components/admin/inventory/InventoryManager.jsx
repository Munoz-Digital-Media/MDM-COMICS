/**
 * InventoryManager - Main inventory screen with tabbed navigation
 * Implements EPIC_01: MDM Comics Catalog Screen (renamed to Inventory)
 *
 * Tabs:
 * - Products (STORY_01): View/edit/delete/history for all products
 * - Create Products (FEAT_01): Sub-tabs for creating Comics, Funkos, Supplies, Bundles
 */
import React, { useState } from 'react';
import ProductList from '../products/ProductList';
import CreateProductsTabs from './CreateProductsTabs';

const TABS = {
  PRODUCTS: 'Products',
  CREATE_PRODUCTS: 'Create Products',
};

export default function InventoryManager() {
  const [activeTab, setActiveTab] = useState(TABS.PRODUCTS);

  const renderContent = () => {
    switch (activeTab) {
      case TABS.PRODUCTS:
        return <ProductList />;
      case TABS.CREATE_PRODUCTS:
        return <CreateProductsTabs />;
      default:
        return <ProductList />;
    }
  };

  return (
    <div className="inventory-manager">
      <header className="flex border-b border-zinc-700 mb-4">
        {Object.values(TABS).map((tab) => (
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
