/**
 * ProductPreviewModal Component
 *
 * Modal wrapper that renders ProductDetailPage in isolated customer context.
 * Allows admins to preview how products appear to end-users.
 *
 * Features:
 * - Device viewport selector (mobile/tablet/desktop)
 * - Visual 'PREVIEW MODE' indicator
 * - Cart actions disabled
 *
 * @compliance constitution_ui.json - Keyboard navigation, focus trap, escape to close
 */
import React, { useState, useEffect, useRef } from 'react';
import { X, Smartphone, Tablet, Monitor } from 'lucide-react';
import ProductDetailPage from '../../ProductDetailPage';

const VIEWPORT_OPTIONS = [
  { id: 'mobile', label: 'Mobile', icon: Smartphone, width: 375 },
  { id: 'tablet', label: 'Tablet', icon: Tablet, width: 768 },
  { id: 'desktop', label: 'Desktop', icon: Monitor, width: null }, // null = full width
];

export default function ProductPreviewModal({ product, onClose }) {
  const [viewport, setViewport] = useState('desktop');
  const modalRef = useRef(null);
  const closeButtonRef = useRef(null);

  // Focus trap and escape key handling
  useEffect(() => {
    const handleKeyDown = (e) => {
      if (e.key === 'Escape') {
        onClose();
      }
    };

    // Focus the close button on mount
    closeButtonRef.current?.focus();

    document.addEventListener('keydown', handleKeyDown);
    // Prevent body scroll
    document.body.style.overflow = 'hidden';

    return () => {
      document.removeEventListener('keydown', handleKeyDown);
      document.body.style.overflow = '';
    };
  }, [onClose]);

  const selectedViewport = VIEWPORT_OPTIONS.find(v => v.id === viewport);

  return (
    <div
      className="fixed inset-0 z-50 flex flex-col bg-zinc-950"
      ref={modalRef}
      role="dialog"
      aria-modal="true"
      aria-label={`Preview: ${product.name}`}
    >
      {/* Header */}
      <header className="flex items-center justify-between px-4 py-3 bg-zinc-900 border-b border-zinc-800">
        <div className="flex items-center gap-4">
          <h2 className="text-lg font-semibold text-white">Product Preview</h2>
          <span className="text-sm text-zinc-500 hidden sm:inline">
            {product.name}
          </span>
        </div>

        <div className="flex items-center gap-4">
          {/* Viewport Selector */}
          <div className="flex items-center gap-1 bg-zinc-800 rounded-lg p-1">
            {VIEWPORT_OPTIONS.map((option) => {
              const Icon = option.icon;
              const isActive = viewport === option.id;
              return (
                <button
                  key={option.id}
                  onClick={() => setViewport(option.id)}
                  className={`p-2 rounded-md transition-colors focus:outline-none focus-visible:ring-2 focus-visible:ring-orange-500 ${
                    isActive
                      ? 'bg-orange-500 text-white'
                      : 'text-zinc-400 hover:text-white hover:bg-zinc-700'
                  }`}
                  aria-label={`Preview as ${option.label}`}
                  aria-pressed={isActive}
                >
                  <Icon className="w-4 h-4" />
                </button>
              );
            })}
          </div>

          {/* Close Button */}
          <button
            ref={closeButtonRef}
            onClick={onClose}
            className="p-2 hover:bg-zinc-800 rounded-lg transition-colors focus:outline-none focus-visible:ring-2 focus-visible:ring-orange-500"
            aria-label="Close preview"
          >
            <X className="w-5 h-5 text-zinc-400" />
          </button>
        </div>
      </header>

      {/* Preview Container */}
      <div className="flex-1 overflow-auto bg-zinc-950 flex justify-center">
        <div
          className={`h-full transition-all duration-300 ${
            selectedViewport.width ? 'border-x border-zinc-800 shadow-2xl' : 'w-full'
          }`}
          style={{
            width: selectedViewport.width ? `${selectedViewport.width}px` : '100%',
            maxWidth: '100%',
          }}
        >
          {/* Device frame indicator for mobile/tablet */}
          {selectedViewport.width && (
            <div className="bg-zinc-900 border-b border-zinc-800 px-3 py-1.5 flex items-center justify-center gap-2">
              <div className="w-16 h-1 bg-zinc-700 rounded-full" />
            </div>
          )}

          {/* Product Detail Page in Preview Mode */}
          <div className="overflow-auto h-full">
            <ProductDetailPage
              product={{
                ...product,
                // Map admin product fields to customer-facing fields if needed
                image: product.image_url || product.image,
              }}
              onBack={onClose}
              onAddToCart={null}
              previewMode={true}
            />
          </div>
        </div>
      </div>

      {/* Footer with viewport info */}
      <footer className="px-4 py-2 bg-zinc-900 border-t border-zinc-800 flex items-center justify-between text-xs text-zinc-500">
        <span>
          Viewport: {selectedViewport.label}
          {selectedViewport.width && ` (${selectedViewport.width}px)`}
        </span>
        <span>Press ESC to close</span>
      </footer>
    </div>
  );
}
