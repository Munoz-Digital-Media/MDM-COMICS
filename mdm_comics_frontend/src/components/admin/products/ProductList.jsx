/**
 * ProductList - Searchable, filterable product table with stock management
 * Phase 3: MDM Admin Console Inventory System v1.3.0
 */
import React, { useState, useEffect, useCallback } from 'react';
import {
  Search, Filter, Plus, Minus, Edit2, Trash2, RotateCcw,
  ChevronLeft, ChevronRight, Loader2, AlertTriangle, X,
  Package, History
} from 'lucide-react';
import { adminAPI } from '../../../services/adminApi';

function StockAdjustmentModal({ product, onClose, onSave }) {
  const [quantity, setQuantity] = useState(0);
  const [movementType, setMovementType] = useState('adjustment');
  const [reason, setReason] = useState('');
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);

  const handleSubmit = async (e) => {
    e.preventDefault();
    if (quantity === 0) return;

    setLoading(true);
    setError(null);

    try {
      await adminAPI.adjustStock(product.id, {
        quantity,
        movement_type: movementType,
        reason,
      });
      onSave();
    } catch (err) {
      setError(err.message);
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center">
      <div className="absolute inset-0 bg-black/60" onClick={onClose} />
      <div className="relative bg-zinc-900 border border-zinc-800 rounded-xl p-6 w-full max-w-md">
        <div className="flex items-center justify-between mb-4">
          <h3 className="text-lg font-semibold text-white">Adjust Stock</h3>
          <button onClick={onClose} className="p-1 hover:bg-zinc-800 rounded">
            <X className="w-5 h-5 text-zinc-400" />
          </button>
        </div>

        <p className="text-sm text-zinc-400 mb-4">
          {product.name} - Current stock: <span className="text-white font-medium">{product.stock}</span>
        </p>

        {error && (
          <div className="bg-red-500/10 border border-red-500/20 rounded-lg p-3 mb-4">
            <p className="text-sm text-red-400">{error}</p>
          </div>
        )}

        <form onSubmit={handleSubmit} className="space-y-4">
          <div>
            <label className="block text-sm text-zinc-400 mb-1">Adjustment Type</label>
            <select
              value={movementType}
              onChange={(e) => setMovementType(e.target.value)}
              className="w-full bg-zinc-800 border border-zinc-700 rounded-lg px-3 py-2 text-white focus:outline-none focus:border-orange-500"
            >
              <option value="adjustment">Manual Adjustment</option>
              <option value="received">Received / Restocked</option>
              <option value="damaged">Damaged / Write-off</option>
              <option value="returned">Customer Return</option>
            </select>
          </div>

          <div>
            <label className="block text-sm text-zinc-400 mb-1">Quantity Change</label>
            <div className="flex items-center gap-2">
              <button
                type="button"
                onClick={() => setQuantity(q => q - 1)}
                className="p-2 bg-zinc-800 rounded-lg hover:bg-zinc-700"
              >
                <Minus className="w-4 h-4 text-zinc-400" />
              </button>
              <input
                type="number"
                value={quantity}
                onChange={(e) => setQuantity(parseInt(e.target.value) || 0)}
                className="flex-1 bg-zinc-800 border border-zinc-700 rounded-lg px-3 py-2 text-white text-center focus:outline-none focus:border-orange-500"
              />
              <button
                type="button"
                onClick={() => setQuantity(q => q + 1)}
                className="p-2 bg-zinc-800 rounded-lg hover:bg-zinc-700"
              >
                <Plus className="w-4 h-4 text-zinc-400" />
              </button>
            </div>
            <p className="text-xs text-zinc-500 mt-1">
              New stock will be: <span className={`font-medium ${product.stock + quantity < 0 ? 'text-red-400' : 'text-green-400'}`}>
                {Math.max(0, product.stock + quantity)}
              </span>
            </p>
          </div>

          <div>
            <label className="block text-sm text-zinc-400 mb-1">Reason (optional)</label>
            <input
              type="text"
              value={reason}
              onChange={(e) => setReason(e.target.value)}
              placeholder="e.g., Inventory count correction"
              className="w-full bg-zinc-800 border border-zinc-700 rounded-lg px-3 py-2 text-white placeholder-zinc-500 focus:outline-none focus:border-orange-500"
            />
          </div>

          <div className="flex gap-3">
            <button
              type="button"
              onClick={onClose}
              className="flex-1 px-4 py-2 bg-zinc-800 text-zinc-300 rounded-lg hover:bg-zinc-700"
            >
              Cancel
            </button>
            <button
              type="submit"
              disabled={quantity === 0 || loading}
              className="flex-1 px-4 py-2 bg-orange-500 text-white rounded-lg hover:bg-orange-600 disabled:opacity-50 disabled:cursor-not-allowed"
            >
              {loading ? 'Saving...' : 'Save Adjustment'}
            </button>
          </div>
        </form>
      </div>
    </div>
  );
}

function StockHistoryModal({ product, onClose }) {
  const [history, setHistory] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);

  useEffect(() => {
    const fetchHistory = async () => {
      try {
        const data = await adminAPI.getStockHistory(product.id);
        setHistory(data.movements || []);
      } catch (err) {
        setError(err.message);
      } finally {
        setLoading(false);
      }
    };
    fetchHistory();
  }, [product.id]);

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center">
      <div className="absolute inset-0 bg-black/60" onClick={onClose} />
      <div className="relative bg-zinc-900 border border-zinc-800 rounded-xl p-6 w-full max-w-lg max-h-[80vh] overflow-hidden flex flex-col">
        <div className="flex items-center justify-between mb-4">
          <h3 className="text-lg font-semibold text-white">Stock History</h3>
          <button onClick={onClose} className="p-1 hover:bg-zinc-800 rounded">
            <X className="w-5 h-5 text-zinc-400" />
          </button>
        </div>

        <p className="text-sm text-zinc-400 mb-4">{product.name}</p>

        {loading ? (
          <div className="flex items-center justify-center py-8">
            <Loader2 className="w-6 h-6 text-orange-500 animate-spin" />
          </div>
        ) : error ? (
          <div className="bg-red-500/10 border border-red-500/20 rounded-lg p-3">
            <p className="text-sm text-red-400">{error}</p>
          </div>
        ) : history.length === 0 ? (
          <p className="text-sm text-zinc-500 text-center py-8">No stock history</p>
        ) : (
          <div className="flex-1 overflow-auto space-y-2">
            {history.map(item => (
              <div key={item.id} className="p-3 bg-zinc-800/50 rounded-lg">
                <div className="flex items-center justify-between mb-1">
                  <span className={`text-sm font-medium ${item.quantity > 0 ? 'text-green-400' : 'text-red-400'}`}>
                    {item.quantity > 0 ? '+' : ''}{item.quantity}
                  </span>
                  <span className="text-xs text-zinc-500">
                    {new Date(item.created_at).toLocaleString()}
                  </span>
                </div>
                <div className="flex items-center justify-between text-xs text-zinc-400">
                  <span className="capitalize">{item.movement_type.replace('_', ' ')}</span>
                  <span>{item.previous_stock} → {item.new_stock}</span>
                </div>
                {item.reason && (
                  <p className="text-xs text-zinc-500 mt-1">{item.reason}</p>
                )}
              </div>
            ))}
          </div>
        )}

        <div className="mt-4 pt-4 border-t border-zinc-800">
          <button
            onClick={onClose}
            className="w-full px-4 py-2 bg-zinc-800 text-zinc-300 rounded-lg hover:bg-zinc-700"
          >
            Close
          </button>
        </div>
      </div>
    </div>
  );
}

export default function ProductList() {
  const [products, setProducts] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [search, setSearch] = useState('');
  const [category, setCategory] = useState('');
  const [lowStockOnly, setLowStockOnly] = useState(false);
  const [includeDeleted, setIncludeDeleted] = useState(false);
  const [offset, setOffset] = useState(0);
  const [total, setTotal] = useState(0);
  const [selectedProduct, setSelectedProduct] = useState(null);
  const [showStockModal, setShowStockModal] = useState(false);
  const [showHistoryModal, setShowHistoryModal] = useState(false);

  const limit = 25;

  const fetchProducts = useCallback(async () => {
    setLoading(true);
    setError(null);

    try {
      const data = await adminAPI.getAdminProducts({
        search,
        category,
        lowStock: lowStockOnly,
        includeDeleted,
        limit,
        offset,
      });
      setProducts(data.items || []);
      setTotal(data.total || 0);
    } catch (err) {
      setError(err.message);
    } finally {
      setLoading(false);
    }
  }, [search, category, lowStockOnly, includeDeleted, offset]);

  useEffect(() => {
    fetchProducts();
  }, [fetchProducts]);

  // Debounced search - resets offset on search change
  useEffect(() => {
    const timer = setTimeout(() => {
      setOffset(0);
    }, 300);
    return () => clearTimeout(timer);
  }, [search]);

  const handleDelete = async (product) => {
    if (!confirm(`Delete "${product.name}"? This will soft-delete the product.`)) return;

    try {
      await adminAPI.deleteProduct(null, product.id);
      fetchProducts();
    } catch (err) {
      alert('Failed to delete: ' + err.message);
    }
  };

  const handleRestore = async (product) => {
    try {
      await adminAPI.restoreProduct(product.id);
      fetchProducts();
    } catch (err) {
      alert('Failed to restore: ' + err.message);
    }
  };

  const handleStockAdjust = (product) => {
    setSelectedProduct(product);
    setShowStockModal(true);
  };

  const handleStockHistory = (product) => {
    setSelectedProduct(product);
    setShowHistoryModal(true);
  };

  const totalPages = Math.ceil(total / limit);
  const currentPage = Math.floor(offset / limit) + 1;

  return (
    <div className="space-y-4">
      {/* Filters */}
      <div className="flex flex-wrap gap-3">
        <div className="relative flex-1 min-w-[200px]">
          <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-zinc-500" />
          <input
            type="text"
            value={search}
            onChange={(e) => setSearch(e.target.value)}
            placeholder="Search products..."
            className="w-full pl-10 pr-4 py-2 bg-zinc-900 border border-zinc-800 rounded-lg text-white placeholder-zinc-500 focus:outline-none focus:border-orange-500"
          />
        </div>

        <select
          value={category}
          onChange={(e) => { setCategory(e.target.value); setOffset(0); }}
          className="px-3 py-2 bg-zinc-900 border border-zinc-800 rounded-lg text-zinc-300 focus:outline-none focus:border-orange-500"
        >
          <option value="">All Categories</option>
          <option value="comics">Comics</option>
          <option value="funko">Funko</option>
        </select>

        <label className="flex items-center gap-2 px-3 py-2 bg-zinc-900 border border-zinc-800 rounded-lg cursor-pointer">
          <input
            type="checkbox"
            checked={lowStockOnly}
            onChange={(e) => { setLowStockOnly(e.target.checked); setOffset(0); }}
            className="rounded border-zinc-600"
          />
          <span className="text-sm text-zinc-300">Low Stock Only</span>
        </label>

        <label className="flex items-center gap-2 px-3 py-2 bg-zinc-900 border border-zinc-800 rounded-lg cursor-pointer">
          <input
            type="checkbox"
            checked={includeDeleted}
            onChange={(e) => { setIncludeDeleted(e.target.checked); setOffset(0); }}
            className="rounded border-zinc-600"
          />
          <span className="text-sm text-zinc-300">Include Deleted</span>
        </label>
      </div>

      {/* Error state */}
      {error && (
        <div className="bg-red-500/10 border border-red-500/20 rounded-lg p-4">
          <p className="text-sm text-red-400">{error}</p>
        </div>
      )}

      {/* Products table */}
      <div className="bg-zinc-900 border border-zinc-800 rounded-xl overflow-hidden">
        {loading ? (
          <div className="flex items-center justify-center py-12">
            <Loader2 className="w-6 h-6 text-orange-500 animate-spin" />
          </div>
        ) : products.length === 0 ? (
          <div className="text-center py-12">
            <Package className="w-12 h-12 text-zinc-700 mx-auto mb-3" />
            <p className="text-zinc-500">No products found</p>
          </div>
        ) : (
          <div className="overflow-x-auto">
            <table className="w-full">
              <thead>
                <tr className="border-b border-zinc-800 text-left">
                  <th className="px-4 py-3 text-xs font-semibold text-zinc-500 uppercase">Product</th>
                  <th className="px-4 py-3 text-xs font-semibold text-zinc-500 uppercase">Category</th>
                  <th className="px-4 py-3 text-xs font-semibold text-zinc-500 uppercase">SKU</th>
                  <th className="px-4 py-3 text-xs font-semibold text-zinc-500 uppercase">Price</th>
                  <th className="px-4 py-3 text-xs font-semibold text-zinc-500 uppercase">Stock</th>
                  <th className="px-4 py-3 text-xs font-semibold text-zinc-500 uppercase">Bin</th>
                  <th className="px-4 py-3 text-xs font-semibold text-zinc-500 uppercase">Actions</th>
                </tr>
              </thead>
              <tbody>
                {products.map(product => (
                  <tr
                    key={product.id}
                    className={`border-b border-zinc-800/50 hover:bg-zinc-800/30 ${product.deleted_at ? 'opacity-50' : ''}`}
                  >
                    <td className="px-4 py-3">
                      <div className="flex items-center gap-3">
                        <img
                          src={product.image_url || 'https://placehold.co/40x40/27272a/f59e0b?text=?'}
                          alt={product.name}
                          className="w-10 h-10 rounded object-cover"
                        />
                        <div>
                          <p className="text-sm font-medium text-white truncate max-w-xs">{product.name}</p>
                          {product.deleted_at && (
                            <span className="text-xs text-red-400">Deleted</span>
                          )}
                        </div>
                      </div>
                    </td>
                    <td className="px-4 py-3">
                      <span className="text-sm text-zinc-400 capitalize">{product.category}</span>
                    </td>
                    <td className="px-4 py-3">
                      <span className="text-xs text-zinc-500 font-mono">{product.sku || '-'}</span>
                    </td>
                    <td className="px-4 py-3">
                      <span className="text-sm text-white">${product.price?.toFixed(2)}</span>
                    </td>
                    <td className="px-4 py-3">
                      <span className={`text-sm font-medium ${
                        product.stock === 0 ? 'text-red-400' :
                        product.stock <= 5 ? 'text-yellow-400' :
                        'text-green-400'
                      }`}>
                        {product.stock}
                      </span>
                    </td>
                    <td className="px-4 py-3">
                      <span className="text-xs text-zinc-500">{product.bin_id || '-'}</span>
                    </td>
                    <td className="px-4 py-3">
                      <div className="flex items-center gap-1">
                        {product.deleted_at ? (
                          <button
                            onClick={() => handleRestore(product)}
                            className="p-1.5 hover:bg-green-500/20 rounded text-green-400"
                            title="Restore"
                          >
                            <RotateCcw className="w-4 h-4" />
                          </button>
                        ) : (
                          <>
                            <button
                              onClick={() => handleStockAdjust(product)}
                              className="p-1.5 hover:bg-zinc-700 rounded text-zinc-400"
                              title="Adjust Stock"
                            >
                              <Edit2 className="w-4 h-4" />
                            </button>
                            <button
                              onClick={() => handleStockHistory(product)}
                              className="p-1.5 hover:bg-zinc-700 rounded text-zinc-400"
                              title="Stock History"
                            >
                              <History className="w-4 h-4" />
                            </button>
                            <button
                              onClick={() => handleDelete(product)}
                              className="p-1.5 hover:bg-red-500/20 rounded text-red-400"
                              title="Delete"
                            >
                              <Trash2 className="w-4 h-4" />
                            </button>
                          </>
                        )}
                      </div>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}

        {/* Pagination */}
        {total > limit && (
          <div className="flex items-center justify-between px-4 py-3 border-t border-zinc-800">
            <p className="text-sm text-zinc-500">
              Showing {offset + 1} - {Math.min(offset + limit, total)} of {total}
            </p>
            <div className="flex items-center gap-2">
              <button
                onClick={() => setOffset(o => Math.max(0, o - limit))}
                disabled={offset === 0}
                className="p-1.5 hover:bg-zinc-800 rounded disabled:opacity-50"
              >
                <ChevronLeft className="w-4 h-4 text-zinc-400" />
              </button>
              <span className="text-sm text-zinc-400">
                Page {currentPage} of {totalPages}
              </span>
              <button
                onClick={() => setOffset(o => o + limit)}
                disabled={offset + limit >= total}
                className="p-1.5 hover:bg-zinc-800 rounded disabled:opacity-50"
              >
                <ChevronRight className="w-4 h-4 text-zinc-400" />
              </button>
            </div>
          </div>
        )}
      </div>

      {/* Modals */}
      {showStockModal && selectedProduct && (
        <StockAdjustmentModal
          product={selectedProduct}
          onClose={() => { setShowStockModal(false); setSelectedProduct(null); }}
          onSave={() => { setShowStockModal(false); setSelectedProduct(null); fetchProducts(); }}
        />
      )}

      {showHistoryModal && selectedProduct && (
        <StockHistoryModal
          product={selectedProduct}
          onClose={() => { setShowHistoryModal(false); setSelectedProduct(null); }}
        />
      )}
    </div>
  );
}
