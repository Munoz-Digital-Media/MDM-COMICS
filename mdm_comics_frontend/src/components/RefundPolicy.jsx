/**
 * RefundPolicy Component
 * BCW Refund Request Module v1.0.0
 *
 * Displays the full refund and return policy.
 * BCW Supplies = Refundable (30 days, 15% restocking)
 * Collectibles (comics, Funkos, graded) = FINAL SALE
 */
import React from 'react';
import { X, RefreshCw, AlertTriangle, Package, Clock, Info, CheckCircle } from 'lucide-react';

export default function RefundPolicy({ onClose }) {
  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center p-4">
      <div className="absolute inset-0 bg-black/60 backdrop-blur-sm" onClick={onClose} />
      <div className="relative bg-zinc-900 rounded-2xl border border-zinc-800 w-full max-w-2xl max-h-[90vh] overflow-hidden shadow-2xl flex flex-col">
        {/* Header */}
        <div className="flex items-center justify-between p-4 border-b border-zinc-800 flex-shrink-0">
          <h2 className="text-xl font-bold text-white flex items-center gap-2">
            <RefreshCw className="w-5 h-5 text-orange-500" />
            Return & Refund Policy
          </h2>
          <button
            onClick={onClose}
            className="p-2 hover:bg-zinc-800 rounded-lg transition-colors"
          >
            <X className="w-5 h-5 text-zinc-400" />
          </button>
        </div>

        {/* Content */}
        <div className="flex-1 overflow-auto p-6 space-y-6">
          {/* Summary Cards */}
          <div className="grid md:grid-cols-2 gap-4">
            {/* BCW Supplies */}
            <div className="bg-green-500/10 border border-green-500/30 rounded-xl p-4">
              <div className="flex items-center gap-2 mb-3">
                <div className="p-2 bg-green-500/20 rounded-lg">
                  <RefreshCw className="w-5 h-5 text-green-400" />
                </div>
                <div>
                  <h3 className="font-semibold text-green-400">BCW Supplies</h3>
                  <p className="text-xs text-green-400/70">Returns Accepted</p>
                </div>
              </div>
              <ul className="space-y-2 text-sm text-zinc-300">
                <li className="flex items-center gap-2">
                  <CheckCircle className="w-4 h-4 text-green-400 flex-shrink-0" />
                  30-day return window
                </li>
                <li className="flex items-center gap-2">
                  <CheckCircle className="w-4 h-4 text-green-400 flex-shrink-0" />
                  15% restocking fee applies
                </li>
                <li className="flex items-center gap-2">
                  <CheckCircle className="w-4 h-4 text-green-400 flex-shrink-0" />
                  Must be unopened & original packaging
                </li>
              </ul>
            </div>

            {/* Collectibles */}
            <div className="bg-orange-500/10 border border-orange-500/30 rounded-xl p-4">
              <div className="flex items-center gap-2 mb-3">
                <div className="p-2 bg-orange-500/20 rounded-lg">
                  <AlertTriangle className="w-5 h-5 text-orange-400" />
                </div>
                <div>
                  <h3 className="font-semibold text-orange-400">Collectibles</h3>
                  <p className="text-xs text-orange-400/70">Final Sale</p>
                </div>
              </div>
              <ul className="space-y-2 text-sm text-zinc-300">
                <li className="flex items-center gap-2">
                  <X className="w-4 h-4 text-orange-400 flex-shrink-0" />
                  Comic Books - All sales final
                </li>
                <li className="flex items-center gap-2">
                  <X className="w-4 h-4 text-orange-400 flex-shrink-0" />
                  Funko Pops - All sales final
                </li>
                <li className="flex items-center gap-2">
                  <X className="w-4 h-4 text-orange-400 flex-shrink-0" />
                  Graded Items - All sales final
                </li>
              </ul>
            </div>
          </div>

          {/* Detailed Policy */}
          <div className="space-y-4">
            <h3 className="text-lg font-semibold text-white flex items-center gap-2">
              <Info className="w-5 h-5 text-orange-500" />
              Detailed Policy
            </h3>

            {/* BCW Supplies Section */}
            <div className="bg-zinc-800/50 rounded-xl p-4">
              <h4 className="font-medium text-white mb-2 flex items-center gap-2">
                <Package className="w-4 h-4 text-green-400" />
                BCW Collector Supplies
              </h4>
              <div className="text-sm text-zinc-400 space-y-2">
                <p>
                  BCW comic bags, boards, boxes, and other storage supplies may be returned within
                  <strong className="text-white"> 30 days</strong> of delivery for a refund.
                </p>
                <p>
                  <strong className="text-white">Requirements:</strong>
                </p>
                <ul className="list-disc list-inside ml-2 space-y-1">
                  <li>Items must be <strong className="text-white">unopened</strong> and in original packaging</li>
                  <li>Original receipt or order confirmation required</li>
                  <li>A <strong className="text-orange-400">15% restocking fee</strong> will be deducted from the refund</li>
                  <li>Customer is responsible for return shipping costs</li>
                </ul>
              </div>
            </div>

            {/* Collectibles Section */}
            <div className="bg-zinc-800/50 rounded-xl p-4">
              <h4 className="font-medium text-white mb-2 flex items-center gap-2">
                <AlertTriangle className="w-4 h-4 text-orange-400" />
                Collectible Items (Final Sale)
              </h4>
              <div className="text-sm text-zinc-400 space-y-2">
                <p>
                  Due to the condition-sensitive nature of collectibles, <strong className="text-orange-400">all sales are final</strong> for:
                </p>
                <ul className="list-disc list-inside ml-2 space-y-1">
                  <li><strong className="text-white">Comic Books</strong> - Bagged, boarded, raw, and slabbed comics</li>
                  <li><strong className="text-white">Funko Pop! Figures</strong> - All Funko products</li>
                  <li><strong className="text-white">Graded Items</strong> - CGC, CBCS, PSA, and other graded collectibles</li>
                </ul>
                <p className="mt-3">
                  <strong className="text-white">Why Final Sale?</strong> Collectible value depends heavily on condition.
                  Once an item leaves our facility, we cannot verify that it has been stored properly or
                  hasn't been swapped with another item.
                </p>
              </div>
            </div>

            {/* Process Section */}
            <div className="bg-zinc-800/50 rounded-xl p-4">
              <h4 className="font-medium text-white mb-2 flex items-center gap-2">
                <Clock className="w-4 h-4 text-orange-500" />
                Refund Process
              </h4>
              <div className="text-sm text-zinc-400 space-y-2">
                <ol className="list-decimal list-inside ml-2 space-y-2">
                  <li>
                    <strong className="text-white">Submit Request:</strong> Go to "My Orders" and select the eligible item to request a refund
                  </li>
                  <li>
                    <strong className="text-white">Review:</strong> Our team will review your request within 1-2 business days
                  </li>
                  <li>
                    <strong className="text-white">Approval:</strong> If approved, you'll receive return shipping instructions
                  </li>
                  <li>
                    <strong className="text-white">Return:</strong> Ship the item back within 7 days of approval
                  </li>
                  <li>
                    <strong className="text-white">Refund:</strong> Once received and inspected, refund will be processed within 5-7 business days
                  </li>
                </ol>
              </div>
            </div>

            {/* Exceptions */}
            <div className="bg-red-500/10 border border-red-500/30 rounded-xl p-4">
              <h4 className="font-medium text-red-400 mb-2">Exceptions</h4>
              <div className="text-sm text-zinc-400 space-y-2">
                <p>
                  Refunds may be issued for <strong className="text-white">any item</strong> (including collectibles) in the following cases:
                </p>
                <ul className="list-disc list-inside ml-2 space-y-1">
                  <li><strong className="text-white">Damaged in transit:</strong> Item arrived damaged due to shipping</li>
                  <li><strong className="text-white">Wrong item sent:</strong> You received a different item than ordered</li>
                  <li><strong className="text-white">Significantly not as described:</strong> Item condition differs significantly from listing</li>
                </ul>
                <p className="mt-2 text-xs">
                  Photo documentation is required for damage/condition claims. Please contact us within 48 hours of delivery.
                </p>
              </div>
            </div>
          </div>

          {/* Contact */}
          <div className="text-center pt-4 border-t border-zinc-800">
            <p className="text-sm text-zinc-500">
              Questions about returns? Contact us at{' '}
              <a href="mailto:returns@mdmcomics.com" className="text-orange-500 hover:text-orange-400">
                returns@mdmcomics.com
              </a>
            </p>
          </div>
        </div>
      </div>
    </div>
  );
}
