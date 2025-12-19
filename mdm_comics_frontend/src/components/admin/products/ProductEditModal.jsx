/**
 * ProductEditModal - Full product editor for all fields
 * Admin Console v1.4.0
 */
import React, { useState, useEffect } from 'react';
import { X, Save, Loader2, Image as ImageIcon, AlertTriangle } from 'lucide-react';
import { adminAPI } from '../../../services/adminApi';

const FEATURED_LIMIT = 5; // Max featured products per category section on homepage

export default function ProductEditModal({ product, onClose, onSave }) {
  const [form, setForm] = useState({
    name: '',
    description: '',
    category: 'comics',
    subcategory: '',
    price: '',
    original_price: '',
    stock: 0,
    image_url: '',
    tags: [],
    featured: false,
    upc: '',
    isbn: '',
    issue_number: '',
    publisher: '',
    year: '',
  });
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);
  const [tagInput, setTagInput] = useState('');
  const [featuredCount, setFeaturedCount] = useState(0);
  const [featuredLimitReached, setFeaturedLimitReached] = useState(false);

  // Initialize form with product data
  useEffect(() => {
    if (product) {
      setForm({
        name: product.name || '',
        description: product.description || '',
        category: product.category || 'comics',
        subcategory: product.subcategory || '',
        price: product.price?.toString() || '',
        original_price: product.original_price?.toString() || '',
        stock: product.stock || 0,
        image_url: product.image_url || '',
        tags: product.tags || [],
        featured: product.featured || false,
        upc: product.upc || '',
        isbn: product.isbn || '',
        issue_number: product.issue_number || '',
        publisher: product.publisher || '',
        year: product.year?.toString() || '',
      });
    }
  }, [product]);

  // Fetch featured product count to enforce limit
  useEffect(() => {
    const fetchFeaturedCount = async () => {
      try {
        const data = await adminAPI.getAdminProducts({ limit: 100, offset: 0 });
        const featured = (data.items || []).filter(p => p.featured === true);
        setFeaturedCount(featured.length);
        // Check if limit reached (excluding current product if already featured)
        const currentIsFeatured = product?.featured || false;
        const effectiveCount = currentIsFeatured ? featured.length - 1 : featured.length;
        setFeaturedLimitReached(effectiveCount >= FEATURED_LIMIT);
      } catch (err) {
        console.error('Failed to fetch featured count:', err);
      }
    };
    fetchFeaturedCount();
  }, [product]);

  const handleFeaturedChange = (checked) => {
    if (checked && featuredLimitReached && !product?.featured) {
      // Don't allow checking if limit reached and this product isn't already featured
      return;
    }
    setForm({ ...form, featured: checked });
  };

  const handleSubmit = async (e) => {
    e.preventDefault();
    if (!form.name || !form.price) {
      setError('Name and Price are required');
      return;
    }

    setLoading(true);
    setError(null);

    try {
      const updateData = {
        name: form.name,
        description: form.description || null,
        category: form.category,
        subcategory: form.subcategory || null,
        price: parseFloat(form.price),
        original_price: form.original_price ? parseFloat(form.original_price) : null,
        stock: parseInt(form.stock) || 0,
        image_url: form.image_url || null,
        tags: form.tags,
        featured: form.featured,
        upc: form.upc || null,
        isbn: form.isbn || null,
        issue_number: form.issue_number || null,
        publisher: form.publisher || null,
        year: form.year ? parseInt(form.year) : null,
      };

      await adminAPI.updateProduct(null, product.id, updateData);
      onSave();
    } catch (err) {
      setError(err.message);
    } finally {
      setLoading(false);
    }
  };

  const addTag = () => {
    if (tagInput.trim() && !form.tags.includes(tagInput.trim())) {
      setForm({ ...form, tags: [...form.tags, tagInput.trim()] });
      setTagInput('');
    }
  };

  const removeTag = (tag) => {
    setForm({ ...form, tags: form.tags.filter(t => t !== tag) });
  };

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center">
      <div className="absolute inset-0 bg-black/60" onClick={onClose} />
      <div className="relative bg-zinc-900 border border-zinc-800 rounded-xl w-full max-w-4xl mx-4 max-h-[90vh] overflow-hidden flex flex-col">
        {/* Header */}
        <div className="flex items-center justify-between p-4 border-b border-zinc-800">
          <h3 className="text-lg font-semibold text-white">Edit Product</h3>
          <button onClick={onClose} className="p-1 hover:bg-zinc-800 rounded">
            <X className="w-5 h-5 text-zinc-400" />
          </button>
        </div>

        {/* Form */}
        <div className="flex-1 overflow-auto p-6">
          {error && (
            <div className="bg-red-500/10 border border-red-500/20 rounded-lg p-3 mb-4">
              <p className="text-sm text-red-400">{error}</p>
            </div>
          )}

          <div className="grid md:grid-cols-2 gap-6">
            {/* Left Column - Form Fields */}
            <div className="space-y-4">
              {/* SKU & Category */}
              <div className="grid grid-cols-2 gap-3">
                <div>
                  <label className="block text-sm text-zinc-400 mb-1">SKU</label>
                  <input
                    type="text"
                    value={product?.sku || ''}
                    disabled
                    className="w-full px-3 py-2 bg-zinc-800/50 border border-zinc-700 rounded-lg text-zinc-500 cursor-not-allowed font-mono"
                  />
                </div>
                <div>
                  <label className="block text-sm text-zinc-400 mb-1">Category</label>
                  <select
                    value={form.category}
                    onChange={(e) => setForm({ ...form, category: e.target.value })}
                    className="w-full px-3 py-2 bg-zinc-800 border border-zinc-700 rounded-lg text-white focus:border-orange-500 focus:outline-none"
                  >
                    <option value="comics">Comics</option>
                    <option value="funko">Funko</option>
                    <option value="supplies">Supplies</option>
                  </select>
                </div>
              </div>

              {/* Name */}
              <div>
                <label className="block text-sm text-zinc-400 mb-1">Name *</label>
                <input
                  type="text"
                  value={form.name}
                  onChange={(e) => setForm({ ...form, name: e.target.value })}
                  className="w-full px-3 py-2 bg-zinc-800 border border-zinc-700 rounded-lg text-white focus:border-orange-500 focus:outline-none"
                  required
                />
              </div>

              {/* Description */}
              <div>
                <label className="block text-sm text-zinc-400 mb-1">Description</label>
                <textarea
                  value={form.description}
                  onChange={(e) => setForm({ ...form, description: e.target.value })}
                  rows={4}
                  className="w-full px-3 py-2 bg-zinc-800 border border-zinc-700 rounded-lg text-white focus:border-orange-500 focus:outline-none resize-none"
                />
              </div>

              {/* Pricing & Stock */}
              <div className="grid grid-cols-3 gap-3">
                <div>
                  <label className="block text-sm text-zinc-400 mb-1">Price *</label>
                  <input
                    type="number"
                    step="0.01"
                    min="0.01"
                    value={form.price}
                    onChange={(e) => setForm({ ...form, price: e.target.value })}
                    className="w-full px-3 py-2 bg-zinc-800 border border-zinc-700 rounded-lg text-white focus:border-orange-500 focus:outline-none"
                    required
                  />
                </div>
                <div>
                  <label className="block text-sm text-zinc-400 mb-1">Original Price</label>
                  <input
                    type="number"
                    step="0.01"
                    min="0"
                    value={form.original_price}
                    onChange={(e) => setForm({ ...form, original_price: e.target.value })}
                    className="w-full px-3 py-2 bg-zinc-800 border border-zinc-700 rounded-lg text-white focus:border-orange-500 focus:outline-none"
                  />
                </div>
                <div>
                  <label className="block text-sm text-zinc-400 mb-1">Stock</label>
                  <input
                    type="number"
                    min="0"
                    value={form.stock}
                    onChange={(e) => setForm({ ...form, stock: e.target.value })}
                    className="w-full px-3 py-2 bg-zinc-800 border border-zinc-700 rounded-lg text-white focus:border-orange-500 focus:outline-none"
                  />
                </div>
              </div>

              {/* Publisher, Issue #, Year */}
              <div className="grid grid-cols-3 gap-3">
                <div>
                  <label className="block text-sm text-zinc-400 mb-1">Publisher</label>
                  <input
                    type="text"
                    value={form.publisher}
                    onChange={(e) => setForm({ ...form, publisher: e.target.value })}
                    className="w-full px-3 py-2 bg-zinc-800 border border-zinc-700 rounded-lg text-white focus:border-orange-500 focus:outline-none"
                  />
                </div>
                <div>
                  <label className="block text-sm text-zinc-400 mb-1">Issue #</label>
                  <input
                    type="text"
                    value={form.issue_number}
                    onChange={(e) => setForm({ ...form, issue_number: e.target.value })}
                    className="w-full px-3 py-2 bg-zinc-800 border border-zinc-700 rounded-lg text-white focus:border-orange-500 focus:outline-none"
                  />
                </div>
                <div>
                  <label className="block text-sm text-zinc-400 mb-1">Year</label>
                  <input
                    type="number"
                    min="1900"
                    max="2099"
                    value={form.year}
                    onChange={(e) => setForm({ ...form, year: e.target.value })}
                    className="w-full px-3 py-2 bg-zinc-800 border border-zinc-700 rounded-lg text-white focus:border-orange-500 focus:outline-none"
                  />
                </div>
              </div>

              {/* UPC & Subcategory */}
              <div className="grid grid-cols-2 gap-3">
                <div>
                  <label className="block text-sm text-zinc-400 mb-1">UPC Barcode</label>
                  <input
                    type="text"
                    value={form.upc}
                    onChange={(e) => setForm({ ...form, upc: e.target.value.replace(/[^0-9]/g, '') })}
                    className="w-full px-3 py-2 bg-zinc-800 border border-zinc-700 rounded-lg text-white font-mono focus:border-orange-500 focus:outline-none"
                  />
                </div>
                <div>
                  <label className="block text-sm text-zinc-400 mb-1">Subcategory</label>
                  <input
                    type="text"
                    value={form.subcategory}
                    onChange={(e) => setForm({ ...form, subcategory: e.target.value })}
                    placeholder="e.g., Marvel, DC, BCW"
                    className="w-full px-3 py-2 bg-zinc-800 border border-zinc-700 rounded-lg text-white placeholder-zinc-600 focus:border-orange-500 focus:outline-none"
                  />
                </div>
              </div>

              {/* Image URL */}
              <div>
                <label className="block text-sm text-zinc-400 mb-1">Image URL</label>
                <input
                  type="url"
                  value={form.image_url}
                  onChange={(e) => setForm({ ...form, image_url: e.target.value })}
                  placeholder="https://..."
                  className="w-full px-3 py-2 bg-zinc-800 border border-zinc-700 rounded-lg text-white placeholder-zinc-600 focus:border-orange-500 focus:outline-none"
                />
              </div>

              {/* Featured */}
              <div className="space-y-2">
                <label className={`flex items-center gap-2 ${featuredLimitReached && !product?.featured ? 'cursor-not-allowed opacity-50' : 'cursor-pointer'}`}>
                  <input
                    type="checkbox"
                    checked={form.featured}
                    onChange={(e) => handleFeaturedChange(e.target.checked)}
                    disabled={featuredLimitReached && !product?.featured && !form.featured}
                    className="w-4 h-4 rounded"
                  />
                  <span className="text-zinc-300">Featured Product</span>
                </label>
                {featuredLimitReached && !product?.featured && (
                  <div className="flex items-center gap-2 text-amber-400 text-xs">
                    <AlertTriangle className="w-3 h-3" />
                    <span>{featuredCount} Featured Products already defined. Please deselect 1 or more to add something new.</span>
                  </div>
                )}
              </div>
            </div>

            {/* Right Column - Preview */}
            <div className="bg-zinc-800 rounded-xl p-4">
              <h4 className="font-medium text-zinc-400 mb-3">Preview</h4>
              <div className="bg-zinc-900 rounded-lg overflow-hidden">
                <div className="h-48 bg-zinc-700 flex items-center justify-center">
                  {form.image_url ? (
                    <img
                      src={form.image_url}
                      alt="Preview"
                      className="w-full h-full object-contain"
                      onError={(e) => { e.target.style.display = 'none'; }}
                    />
                  ) : (
                    <ImageIcon className="w-16 h-16 text-zinc-600" />
                  )}
                </div>
                <div className="p-4">
                  <p className="text-xs text-orange-500 mb-1">{form.subcategory || form.publisher || form.category}</p>
                  <h4 className="font-bold text-white mb-2">{form.name || 'Product Name'}</h4>
                  <p className="text-sm text-zinc-500 mb-3 line-clamp-2">{form.description || 'Description...'}</p>
                  <div className="flex items-center justify-between">
                    <div>
                      <span className="text-xl font-bold text-white">${form.price || '0.00'}</span>
                      {form.original_price && (
                        <span className="ml-2 text-sm text-zinc-500 line-through">${form.original_price}</span>
                      )}
                    </div>
                    <span className="text-sm text-zinc-400">Stock: {form.stock}</span>
                  </div>
                </div>
              </div>

              {/* Tags */}
              <div className="mt-4">
                <label className="block text-sm text-zinc-400 mb-2">Tags</label>
                <div className="flex gap-2 mb-2 flex-wrap">
                  {form.tags.map(tag => (
                    <span
                      key={tag}
                      className="px-2 py-1 bg-orange-500/20 text-orange-400 text-xs rounded-full flex items-center gap-1"
                    >
                      {tag}
                      <button
                        type="button"
                        onClick={() => removeTag(tag)}
                        className="hover:text-white"
                      >
                        <X className="w-3 h-3" />
                      </button>
                    </span>
                  ))}
                </div>
                <div className="flex gap-2">
                  <input
                    type="text"
                    value={tagInput}
                    onChange={(e) => setTagInput(e.target.value)}
                    onKeyPress={(e) => e.key === 'Enter' && (e.preventDefault(), addTag())}
                    placeholder="Add tag..."
                    className="flex-1 px-3 py-2 bg-zinc-900 border border-zinc-700 rounded-lg text-white placeholder-zinc-600 focus:border-orange-500 focus:outline-none"
                  />
                  <button
                    type="button"
                    onClick={addTag}
                    className="px-3 py-2 bg-zinc-700 text-zinc-300 rounded-lg hover:bg-zinc-600"
                  >
                    Add
                  </button>
                </div>
              </div>
            </div>
          </div>
        </div>

        {/* Footer */}
        <div className="flex items-center justify-end gap-3 p-4 border-t border-zinc-800">
          <button
            type="button"
            onClick={onClose}
            className="px-4 py-2 bg-zinc-800 text-zinc-300 rounded-lg hover:bg-zinc-700"
          >
            Cancel
          </button>
          <button
            onClick={handleSubmit}
            disabled={loading}
            className="px-4 py-2 bg-orange-500 text-white rounded-lg hover:bg-orange-600 disabled:opacity-50 flex items-center gap-2"
          >
            {loading ? (
              <Loader2 className="w-4 h-4 animate-spin" />
            ) : (
              <Save className="w-4 h-4" />
            )}
            Save Changes
          </button>
        </div>
      </div>
    </div>
  );
}
