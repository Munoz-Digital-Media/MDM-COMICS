/**
 * EmptyState - Empty list placeholder
 */

import React from 'react';
import { Package, ShoppingCart, Truck, RefreshCw } from 'lucide-react';

const icons = {
  Package,
  ShoppingCart,
  Truck,
  RefreshCw,
};

export default function EmptyState({ icon = 'Package', title, description, action }) {
  const Icon = icons[icon] || Package;

  return (
    <div className="flex flex-col items-center justify-center py-16 text-center" role="status">
      <div className="w-16 h-16 rounded-full bg-zinc-800 flex items-center justify-center mb-4">
        <Icon className="w-8 h-8 text-zinc-500" />
      </div>
      <h3 className="text-lg font-medium text-white mb-2">{title}</h3>
      <p className="text-sm text-zinc-500 max-w-md">{description}</p>
      {action && <div className="mt-4">{action}</div>}
    </div>
  );
}
