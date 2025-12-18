import React, { useState } from 'react';
import ProductList from '../products/ProductList';
import BundleList from '../bundles/BundleList';
import ProductCreator from './ProductCreator';
import { FEATURES } from '../../../features';

const TABS = {
  PRODUCTS: 'Products',
  SEARCH_CREATE: 'Search & Create',
  BUNDLES: 'Bundles',
};

export default function CatalogManager() {
  const [activeTab, setActiveTab] = useState(TABS.PRODUCTS);

  // Build available tabs based on feature flags
  const availableTabs = [TABS.PRODUCTS, TABS.SEARCH_CREATE];
  if (FEATURES.BUNDLES_ENABLED) {
    availableTabs.push(TABS.BUNDLES);
  }

  const renderContent = () => {
    switch (activeTab) {
      case TABS.PRODUCTS:
        return <ProductList />;
      case TABS.SEARCH_CREATE:
        return <ProductCreator />;
      case TABS.BUNDLES:
        return <BundleList />;
      default:
        return <ProductList />;
    }
  };

  return (
    <div className="catalog-manager">
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
