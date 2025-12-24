/**
 * CreateBundlesTab - Bundle management tab
 * Implements STORY_05: MDM Comics Create Bundles Screen
 * Implements STORY_06: MDM Comics Create Bundle Modal
 *
 * This component wraps the existing BundleList component and provides
 * the Create Bundles interface as specified in the EPIC.
 */
import React from 'react';
import { Boxes } from 'lucide-react';
import BundleList from '../bundles/BundleList';

export default function CreateBundlesTab() {
  return (
    <div className="space-y-4">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div>
          <h2 className="text-xl font-bold text-white flex items-center gap-2">
            <Boxes className="w-6 h-6 text-[#0d9488]" />
            Create Bundles
          </h2>
          <p className="text-sm text-zinc-400 mt-1">Create and manage product bundles</p>
        </div>
      </div>

      {/* Bundle List with full functionality */}
      <BundleList />
    </div>
  );
}
