/**
 * BundleList - Admin bundle management with CRUD operations
 * Bundle Builder Tool v1.0.0
 */
import React, { useState, useEffect, useCallback } from 'react';
import {
  Search, Plus, Edit2, Trash2, Copy, Eye, EyeOff,
  ChevronLeft, ChevronRight, Loader2, Package, X,
  DollarSign, Tag, Calendar, Archive
} from 'lucide-react';
import { adminAPI } from '../../../services/adminApi';

// Status badge component
function StatusBadge({ status }) {
  const styles = {
    DRAFT: 'bg-zinc-500/20 text-zinc-400 border-zinc-500/30',
    ACTIVE: 'bg-green-500/20 text-green-400 border-green-500/30',
    INACTIVE: 'bg-yellow-500/20 text-yellow-400 border-yellow-500/30',
    ARCHIVED: 'bg-red-500/20 text-red-400 border-red-500/30',
  };

  return (
    <span className={`px-2 py-0.5 text-xs font-medium rounded border ${styles[status] || styles.DRAFT}`}>
      {status}
    </span>
  );
}

// Create/Edit Bundle Modal
function BundleModal({ bundle, onClose, onSave }) {
  const isEdit = !!bundle;
  const [formData, setFormData] = useState({
    name: bundle?.name || '',
    short_description: bundle?.short_description || '',
    description: bundle?.description || '',
    bundle_price: bundle?.bundle_price || '',
    category: bundle?.category || '',
    badge_text: bundle?.badge_text || '',
    image_url: bundle?.image_url || '',
  });
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);

  const handleSubmit = async (e) => {
    e.preventDefault();
    if (!formData.name || !formData.bundle_price) {
      setError('Name and price are required');
      return;
    }

    setLoading(true);
    setError(null);

    try {
      const data = {
        ...formData,
        bundle_price: parseFloat(formData.bundle_price),
      };

      if (isEdit) {
        await adminAPI.updateBundle(bundle.id, data);
      } else {
        await adminAPI.createBundle(data);
      }
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
      <div className="relative bg-zinc-900 border border-zinc-800 rounded-xl p-6 w-full max-w-lg max-h-[90vh] overflow-y-auto">
        <div className="flex items-center justify-between mb-4">
          <h3 className="text-lg font-semibold text-white">
            {isEdit ? 'Edit Bundle' : 'Create Bundle'}
          </h3>
          <button onClick={onClose} className="p-1 hover:bg-zinc-800 rounded">
            <X className="w-5 h-5 text-zinc-400" />
          </button>
        </div>

        {error && (
          <div className="bg-red-500/10 border border-red-500/20 rounded-lg p-3 mb-4">
            <p className="text-sm text-red-400">{error}</p>
          </div>
        )}

        <form onSubmit={handleSubmit} className="space-y-4">
          <div>
            <label className="block text-sm text-zinc-400 mb-1">Bundle Name *</label>
            <input
              type="text"
              value={formData.name}
              onChange={(e) => setFormData({ ...formData, name: e.target.value })}
              placeholder="e.g., Comic Storage Starter Kit"
              className="w-full bg-zinc-800 border border-zinc-700 rounded-lg px-3 py-2 text-white placeholder-zinc-500 focus:outline-none focus:border-orange-500"
            />
          </div>

          <div>
            <label className="block text-sm text-zinc-400 mb-1">Short Description</label>
            <input
              type="text"
              value={formData.short_description}
              onChange={(e) => setFormData({ ...formData, short_description: e.target.value })}
              placeholder="Brief description for listings"
              className="w-full bg-zinc-800 border border-zinc-700 rounded-lg px-3 py-2 text-white placeholder-zinc-500 focus:outline-none focus:border-orange-500"
            />
          </div>

          <div>
            <label className="block text-sm text-zinc-400 mb-1">Full Description</label>
            <textarea
              value={formData.description}
              onChange={(e) => setFormData({ ...formData, description: e.target.value })}
              placeholder="Detailed description..."
              rows={3}
              className="w-full bg-zinc-800 border border-zinc-700 rounded-lg px-3 py-2 text-white placeholder-zinc-500 focus:outline-none focus:border-orange-500 resize-none"
            />
          </div>

          <div className="grid grid-cols-2 gap-4">
            <div>
              <label className="block text-sm text-zinc-400 mb-1">Bundle Price *</label>
              <div className="relative">
                <DollarSign className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-zinc-500" />
                <input
                  type="number"
                  step="0.01"
                  min="0"
                  value={formData.bundle_price}
                  onChange={(e) => setFormData({ ...formData, bundle_price: e.target.value })}
                  placeholder="0.00"
                  className="w-full pl-9 pr-3 py-2 bg-zinc-800 border border-zinc-700 rounded-lg text-white placeholder-zinc-500 focus:outline-none focus:border-orange-500"
                />
              </div>
            </div>

            <div>
              <label className="block text-sm text-zinc-400 mb-1">Category</label>
              <select
                value={formData.category}
                onChange={(e) => setFormData({ ...formData, category: e.target.value })}
                className="w-full bg-zinc-800 border border-zinc-700 rounded-lg px-3 py-2 text-white focus:outline-none focus:border-orange-500"
              >
                <option value="">Select category</option>
                <option value="supplies">Supplies</option>
                <option value="storage">Storage</option>
                <option value="starter-kit">Starter Kit</option>
                <option value="premium">Premium</option>
              </select>
            </div>
          </div>

          <div>
            <label className="block text-sm text-zinc-400 mb-1">Badge Text</label>
            <input
              type="text"
              value={formData.badge_text}
              onChange={(e) => setFormData({ ...formData, badge_text: e.target.value })}
              placeholder="e.g., Best Value, New, Limited"
              className="w-full bg-zinc-800 border border-zinc-700 rounded-lg px-3 py-2 text-white placeholder-zinc-500 focus:outline-none focus:border-orange-500"
            />
          </div>

          <div>
            <label className="block text-sm text-zinc-400 mb-1">Image URL</label>
            <input
              type="url"
              value={formData.image_url}
              onChange={(e) => setFormData({ ...formData, image_url: e.target.value })}
              placeholder="https://example.com/bundle-image.jpg"
              className="w-full bg-zinc-800 border border-zinc-700 rounded-lg px-3 py-2 text-white placeholder-zinc-500 focus:outline-none focus:border-orange-500"
            />
            {formData.image_url && (
              <div className="mt-2 p-2 bg-zinc-800 rounded-lg border border-zinc-700">
                <img
                  src={formData.image_url}
                  alt="Bundle preview"
                  className="w-20 h-20 object-contain mx-auto rounded"
                  onError={(e) => { e.target.style.display = 'none'; }}
                />
              </div>
            )}
          </div>

          <div className="flex gap-3 pt-4">
            <button
              type="button"
              onClick={onClose}
              className="flex-1 px-4 py-2 bg-zinc-800 text-zinc-300 rounded-lg hover:bg-zinc-700"
            >
              Cancel
            </button>
            <button
              type="submit"
              disabled={loading}
              className="flex-1 px-4 py-2 bg-orange-500 text-white rounded-lg hover:bg-orange-600 disabled:opacity-50 disabled:cursor-not-allowed"
            >
              {loading ? 'Saving...' : isEdit ? 'Update Bundle' : 'Create Bundle'}
            </button>
          </div>
        </form>
      </div>
    </div>
  );
}

export default function BundleList() {
  const [bundles, setBundles] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [search, setSearch] = useState('');
  const [statusFilter, setStatusFilter] = useState('');
  const [categoryFilter, setCategoryFilter] = useState('');
  const [page, setPage] = useState(1);
  const [total, setTotal] = useState(0);
  const [showModal, setShowModal] = useState(false);
  const [selectedBundle, setSelectedBundle] = useState(null);

  const perPage = 25;

  const fetchBundles = useCallback(async () => {
    setLoading(true);
    setError(null);

    try {
      const data = await adminAPI.getBundles({
        search,
        status: statusFilter,
        category: categoryFilter,
        page,
        per_page: perPage,
      });
      setBundles(data.items || []);
      setTotal(data.total || 0);
    } catch (err) {
      setError(err.message);
    } finally {
      setLoading(false);
    }
  }, [search, statusFilter, categoryFilter, page]);

  useEffect(() => {
    fetchBundles();
  }, [fetchBundles]);

  // Reset page when filters change
  useEffect(() => {
    setPage(1);
  }, [search, statusFilter, categoryFilter]);

  const handleCreate = () => {
    setSelectedBundle(null);
    setShowModal(true);
  };

  const handleEdit = (bundle) => {
    setSelectedBundle(bundle);
    setShowModal(true);
  };

  const handleDelete = async (bundle) => {
    if (!confirm(`Archive bundle "${bundle.name}"?`)) return;

    try {
      await adminAPI.archiveBundle(bundle.id);
      fetchBundles();
    } catch (err) {
      setError('Failed to archive: ' + err.message);
    }
  };

  const handleDuplicate = async (bundle) => {
    try {
      await adminAPI.duplicateBundle(bundle.id);
      fetchBundles();
    } catch (err) {
      setError('Failed to duplicate: ' + err.message);
    }
  };

  const handlePublish = async (bundle) => {
    try {
      await adminAPI.publishBundle(bundle.id);
      fetchBundles();
    } catch (err) {
      setError('Failed to publish: ' + err.message);
    }
  };

  const handleUnpublish = async (bundle) => {
    try {
      await adminAPI.unpublishBundle(bundle.id);
      fetchBundles();
    } catch (err) {
      setError('Failed to unpublish: ' + err.message);
    }
  };

  const totalPages = Math.ceil(total / perPage);

  return (
    <div className="space-y-4">
      {/* Header with Create button */}
      <div className="flex items-center justify-between">
        <div className="flex flex-wrap gap-3 flex-1">
          <div className="relative flex-1 min-w-[200px] max-w-md">
            <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-zinc-500" />
            <input
              type="text"
              value={search}
              onChange={(e) => setSearch(e.target.value)}
              placeholder="Search bundles..."
              className="w-full pl-10 pr-4 py-2 bg-zinc-900 border border-zinc-800 rounded-lg text-white placeholder-zinc-500 focus:outline-none focus:border-orange-500"
            />
          </div>

          <select
            value={statusFilter}
            onChange={(e) => setStatusFilter(e.target.value)}
            className="px-3 py-2 bg-zinc-900 border border-zinc-800 rounded-lg text-zinc-300 focus:outline-none focus:border-orange-500"
          >
            <option value="">All Status</option>
            <option value="DRAFT">Draft</option>
            <option value="ACTIVE">Active</option>
            <option value="INACTIVE">Inactive</option>
            <option value="ARCHIVED">Archived</option>
          </select>

          <select
            value={categoryFilter}
            onChange={(e) => setCategoryFilter(e.target.value)}
            className="px-3 py-2 bg-zinc-900 border border-zinc-800 rounded-lg text-zinc-300 focus:outline-none focus:border-orange-500"
          >
            <option value="">All Categories</option>
            <option value="supplies">Supplies</option>
            <option value="storage">Storage</option>
            <option value="starter-kit">Starter Kit</option>
            <option value="premium">Premium</option>
          </select>
        </div>

        <button
          onClick={handleCreate}
          className="flex items-center gap-2 px-4 py-2 bg-orange-500 text-white rounded-lg hover:bg-orange-600 transition-colors"
        >
          <Plus className="w-4 h-4" />
          <span>Create Bundle</span>
        </button>
      </div>

      {/* Error state */}
      {error && (
        <div className="bg-red-500/10 border border-red-500/20 rounded-lg p-4">
          <p className="text-sm text-red-400">{error}</p>
        </div>
      )}

      {/* Bundles table */}
      <div className="bg-zinc-900 border border-zinc-800 rounded-xl overflow-hidden">
        {loading ? (
          <div className="flex items-center justify-center py-12">
            <Loader2 className="w-6 h-6 text-orange-500 animate-spin" />
          </div>
        ) : bundles.length === 0 ? (
          <div className="text-center py-12">
            <Package className="w-12 h-12 text-zinc-700 mx-auto mb-3" />
            <p className="text-zinc-500">No bundles found</p>
            <button
              onClick={handleCreate}
              className="mt-4 text-sm text-orange-400 hover:text-orange-300"
            >
              Create your first bundle
            </button>
          </div>
        ) : (
          <div className="overflow-x-auto">
            <table className="w-full">
              <thead>
                <tr className="border-b border-zinc-800 text-left">
                  <th className="px-4 py-3 text-xs font-semibold text-zinc-500 uppercase">Bundle</th>
                  <th className="px-4 py-3 text-xs font-semibold text-zinc-500 uppercase">SKU</th>
                  <th className="px-4 py-3 text-xs font-semibold text-zinc-500 uppercase">Price</th>
                  <th className="px-4 py-3 text-xs font-semibold text-zinc-500 uppercase">Savings</th>
                  <th className="px-4 py-3 text-xs font-semibold text-zinc-500 uppercase">Items</th>
                  <th className="px-4 py-3 text-xs font-semibold text-zinc-500 uppercase">Status</th>
                  <th className="px-4 py-3 text-xs font-semibold text-zinc-500 uppercase">Actions</th>
                </tr>
              </thead>
              <tbody>
                {bundles.map(bundle => (
                  <tr
                    key={bundle.id}
                    className={`border-b border-zinc-800/50 hover:bg-zinc-800/30 ${bundle.status === 'ARCHIVED' ? 'opacity-50' : ''}`}
                  >
                    <td className="px-4 py-3">
                      <div className="flex items-center gap-3">
                        {bundle.image_url ? (
                          <img
                            src={bundle.image_url}
                            alt={bundle.name}
                            className="w-10 h-10 rounded object-contain"
                          />
                        ) : (
                          <div className="w-10 h-10 bg-zinc-800 rounded flex items-center justify-center">
                            <Package className="w-5 h-5 text-zinc-600" />
                          </div>
                        )}
                        <div>
                          <p className="text-sm font-medium text-white truncate max-w-xs">{bundle.name}</p>
                          {bundle.category && (
                            <span className="text-xs text-zinc-500">{bundle.category}</span>
                          )}
                        </div>
                      </div>
                    </td>
                    <td className="px-4 py-3">
                      <span className="text-xs text-zinc-500 font-mono">{bundle.sku}</span>
                    </td>
                    <td className="px-4 py-3">
                      <span className="text-sm text-white font-medium">
                        ${bundle.bundle_price?.toFixed(2)}
                      </span>
                      {bundle.compare_at_price && (
                        <span className="text-xs text-zinc-500 line-through ml-2">
                          ${bundle.compare_at_price?.toFixed(2)}
                        </span>
                      )}
                    </td>
                    <td className="px-4 py-3">
                      {bundle.savings_percent ? (
                        <span className="text-sm text-green-400">
                          {bundle.savings_percent.toFixed(0)}% off
                        </span>
                      ) : (
                        <span className="text-sm text-zinc-500">-</span>
                      )}
                    </td>
                    <td className="px-4 py-3">
                      <span className="text-sm text-zinc-400">{bundle.item_count || 0}</span>
                    </td>
                    <td className="px-4 py-3">
                      <StatusBadge status={bundle.status} />
                    </td>
                    <td className="px-4 py-3">
                      <div className="flex items-center gap-1">
                        {bundle.status === 'DRAFT' && (
                          <button
                            onClick={() => handlePublish(bundle)}
                            className="p-1.5 hover:bg-green-500/20 rounded text-green-400"
                            title="Publish"
                          >
                            <Eye className="w-4 h-4" />
                          </button>
                        )}
                        {bundle.status === 'ACTIVE' && (
                          <button
                            onClick={() => handleUnpublish(bundle)}
                            className="p-1.5 hover:bg-yellow-500/20 rounded text-yellow-400"
                            title="Unpublish"
                          >
                            <EyeOff className="w-4 h-4" />
                          </button>
                        )}
                        <button
                          onClick={() => handleEdit(bundle)}
                          className="p-1.5 hover:bg-zinc-700 rounded text-zinc-400"
                          title="Edit"
                        >
                          <Edit2 className="w-4 h-4" />
                        </button>
                        <button
                          onClick={() => handleDuplicate(bundle)}
                          className="p-1.5 hover:bg-zinc-700 rounded text-zinc-400"
                          title="Duplicate"
                        >
                          <Copy className="w-4 h-4" />
                        </button>
                        {bundle.status !== 'ARCHIVED' && (
                          <button
                            onClick={() => handleDelete(bundle)}
                            className="p-1.5 hover:bg-red-500/20 rounded text-red-400"
                            title="Archive"
                          >
                            <Archive className="w-4 h-4" />
                          </button>
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
        {total > perPage && (
          <div className="flex items-center justify-between px-4 py-3 border-t border-zinc-800">
            <p className="text-sm text-zinc-500">
              Showing {(page - 1) * perPage + 1} - {Math.min(page * perPage, total)} of {total}
            </p>
            <div className="flex items-center gap-2">
              <button
                onClick={() => setPage(p => Math.max(1, p - 1))}
                disabled={page === 1}
                className="p-1.5 hover:bg-zinc-800 rounded disabled:opacity-50"
              >
                <ChevronLeft className="w-4 h-4 text-zinc-400" />
              </button>
              <span className="text-sm text-zinc-400">
                Page {page} of {totalPages}
              </span>
              <button
                onClick={() => setPage(p => p + 1)}
                disabled={page >= totalPages}
                className="p-1.5 hover:bg-zinc-800 rounded disabled:opacity-50"
              >
                <ChevronRight className="w-4 h-4 text-zinc-400" />
              </button>
            </div>
          </div>
        )}
      </div>

      {/* Modal */}
      {showModal && (
        <BundleModal
          bundle={selectedBundle}
          onClose={() => { setShowModal(false); setSelectedBundle(null); }}
          onSave={() => { setShowModal(false); setSelectedBundle(null); fetchBundles(); }}
        />
      )}
    </div>
  );
}
