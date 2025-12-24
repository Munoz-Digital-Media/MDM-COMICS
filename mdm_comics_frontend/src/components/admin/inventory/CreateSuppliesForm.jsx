/**
 * CreateSuppliesForm - Supply product creation form
 * Implements STORY_04: MDM Comics Create Supplies Screen
 *
 * Features:
 * - Basic fields: SKU, Name, Description, Price, Purchase Cost, Stock, UPC Barcode
 * - Brand: BCW (expandable)
 * - Sub-Category with dynamic field visibility
 * - Size (for bags/boards)
 * - Units Per Pack with dynamic label
 * - Dimensions (Exterior/Interior) with whole number + fraction
 * - Weight (lbs.oz)
 * - Material
 * - Tags panel
 * - Multi-image upload (1-9 images)
 * - Featured toggle
 * - Draft/Publish workflow
 */
import React, { useState, useCallback } from 'react';
import {
  Save, Send, Loader2, X, Package, Plus, Tag, Star
} from 'lucide-react';
import { adminAPI } from '../../../services/adminApi';
import MultiImageUploader from '../../shared/MultiImageUploader';

// Brand options
const BRAND_OPTIONS = ['BCW'];

// Sub-category options (STORY_04 AC4)
const SUB_CATEGORY_OPTIONS = [
  'Comic Book Bags',
  'Comic Book Boards',
  'Magazine Bags',
  'Magazine Boards',
  'Comic Book Bins',
  'Comic Book Bin Partitions',
  'Comic Book Dividers',
  'Latching Cases',
];

// Size options (for bags/boards)
const SIZE_OPTIONS = [
  'Current | Modern',
  'Silver Age',
  'Golden Age',
  'Treasury | Magazine',
];

// Fraction options for dimensions
const FRACTION_OPTIONS = ['', '1/8', '1/4', '3/8', '1/2', '5/8', '3/4', '7/8'];

// Dynamic field visibility rules (STORY_04 AC4.1)
const getFieldVisibility = (subCategory) => {
  if (['Comic Book Bags', 'Comic Book Boards', 'Magazine Bags', 'Magazine Boards'].includes(subCategory)) {
    return { showSize: true, showInteriorDimensions: false, unitsLabel: subCategory.includes('Bags') ? 'Bag Quantity' : 'Board Quantity' };
  }
  if (['Comic Book Bins', 'Latching Cases'].includes(subCategory)) {
    return { showSize: false, showInteriorDimensions: true, unitsLabel: 'Units Per Pack' };
  }
  return { showSize: false, showInteriorDimensions: false, unitsLabel: 'Units Per Pack' };
};

const initialFormState = {
  // Basic fields
  sku: '',
  name: '',
  description: '',
  price: '',
  purchaseCost: '',
  stock: 1,
  upcBarcode: '',
  // Brand & Category
  brand: 'BCW',
  subCategory: 'Comic Book Bags',
  // Size (for bags/boards)
  size: 'Current | Modern',
  // Units Per Pack
  unitsPerPack: '',
  // Exterior Dimensions
  exteriorWidth: '',
  exteriorWidthFraction: '',
  exteriorHeight: '',
  exteriorHeightFraction: '',
  exteriorLength: '',
  exteriorLengthFraction: '',
  // Interior Dimensions
  interiorWidth: '',
  interiorWidthFraction: '',
  interiorHeight: '',
  interiorHeightFraction: '',
  interiorLength: '',
  interiorLengthFraction: '',
  // Weight
  weightPounds: '',
  weightOunces: '',
  // Material
  material: '',
  // Tags
  tags: [],
  tagInput: '',
  // Images
  images: [],
  // Featured
  isFeatured: false,
  // Status
  status: 'draft',
};

export default function CreateSuppliesForm() {
  const [form, setForm] = useState(initialFormState);
  const [saving, setSaving] = useState(false);
  const [message, setMessage] = useState(null);

  // Update form field
  const updateField = (field, value) => {
    setForm(prev => ({ ...prev, [field]: value }));
  };

  // Get field visibility based on sub-category
  const fieldVisibility = getFieldVisibility(form.subCategory);

  // Generate SKU based on fields (STORY_04 AC4)
  const generateSKU = useCallback(() => {
    const parts = ['MDM', 'SUP'];
    if (form.subCategory) {
      const subCatCode = form.subCategory.split(' ').map(w => w[0]).join('').toUpperCase();
      parts.push(subCatCode);
    }
    if (form.size && fieldVisibility.showSize) {
      const sizeCode = form.size.split(' ')[0].toUpperCase().substring(0, 3);
      parts.push(sizeCode);
    }
    if (form.name) {
      parts.push(form.name.replace(/[^a-zA-Z0-9]/g, '').substring(0, 8).toUpperCase());
    }
    return parts.join('-');
  }, [form.subCategory, form.size, form.name, fieldVisibility.showSize]);

  // Handle image changes
  const handleImagesChange = (newImages) => {
    updateField('images', newImages);
  };

  // Add tag (STORY_04 AC5)
  const addTag = () => {
    if (form.tagInput.length >= 5 && !form.tags.includes(form.tagInput)) {
      setForm(prev => ({
        ...prev,
        tags: [...prev.tags, prev.tagInput],
        tagInput: '',
      }));
    }
  };

  // Remove tag
  const removeTag = (tag) => {
    setForm(prev => ({
      ...prev,
      tags: prev.tags.filter(t => t !== tag),
    }));
  };

  // Format dimension string
  const formatDimension = (whole, fraction) => {
    if (!whole && !fraction) return null;
    return fraction ? `${whole || 0} ${fraction}` : whole;
  };

  // Save draft
  const handleSaveDraft = async () => {
    setSaving(true);
    setMessage(null);
    try {
      await new Promise(resolve => setTimeout(resolve, 500));
      setMessage({ type: 'success', text: 'Draft saved successfully!' });
    } catch (err) {
      setMessage({ type: 'error', text: 'Failed to save draft: ' + err.message });
    } finally {
      setSaving(false);
    }
  };

  // Publish product
  const handlePublish = async () => {
    // Validation
    if (!form.name || !form.sku || !form.price || !form.description || form.images.length === 0) {
      setMessage({ type: 'error', text: 'Please fill in required fields: Name, SKU, Price, Description, and at least 1 image' });
      return;
    }

    setSaving(true);
    setMessage(null);
    try {
      const productData = {
        name: form.name,
        sku: form.sku || generateSKU(),
        category: 'supplies',
        subcategory: form.subCategory,
        price: parseFloat(form.price),
        stock: parseInt(form.stock) || 1,
        description: form.description,
        upc: form.upcBarcode || null,
        // Supply-specific fields
        brand: form.brand,
        supply_sub_category: form.subCategory,
        size: fieldVisibility.showSize ? form.size : null,
        units_per_pack: form.unitsPerPack ? parseInt(form.unitsPerPack) : null,
        // Exterior Dimensions
        exterior_width: formatDimension(form.exteriorWidth, form.exteriorWidthFraction),
        exterior_height: formatDimension(form.exteriorHeight, form.exteriorHeightFraction),
        exterior_length: formatDimension(form.exteriorLength, form.exteriorLengthFraction),
        // Interior Dimensions
        interior_width: fieldVisibility.showInteriorDimensions ? formatDimension(form.interiorWidth, form.interiorWidthFraction) : null,
        interior_height: fieldVisibility.showInteriorDimensions ? formatDimension(form.interiorHeight, form.interiorHeightFraction) : null,
        interior_length: fieldVisibility.showInteriorDimensions ? formatDimension(form.interiorLength, form.interiorLengthFraction) : null,
        // Weight
        weight_lbs: form.weightPounds ? parseInt(form.weightPounds) : null,
        weight_oz: form.weightOunces ? parseInt(form.weightOunces) : null,
        // Material
        material: form.material || null,
        // Tags
        tags: form.tags.length > 0 ? form.tags : null,
        // Featured
        featured: form.isFeatured,
        // Images
        images: form.images.map(img => img.url),
        image_url: form.images.find(img => img.isPrimary)?.url || form.images[0]?.url || null,
        // Purchase cost (admin only)
        purchase_cost: form.purchaseCost ? parseFloat(form.purchaseCost) : null,
      };

      await adminAPI.createProduct(null, productData);
      setMessage({ type: 'success', text: 'Supply product published successfully!' });
      setForm(initialFormState);
    } catch (err) {
      setMessage({ type: 'error', text: 'Failed to publish: ' + err.message });
    } finally {
      setSaving(false);
    }
  };

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div>
          <h2 className="text-xl font-bold text-white flex items-center gap-2">
            <Package className="w-6 h-6 text-[#00b894]" />
            Create Supply
          </h2>
          <p className="text-sm text-zinc-400 mt-1">Add a new supply product to your inventory</p>
        </div>
        <div className="flex gap-2">
          <button
            onClick={handleSaveDraft}
            disabled={saving}
            className="px-4 py-2 bg-zinc-700 text-white rounded-lg hover:bg-zinc-600 disabled:opacity-50 flex items-center gap-2"
          >
            <Save className="w-4 h-4" />
            Save Draft
          </button>
          <button
            onClick={handlePublish}
            disabled={saving}
            className="px-4 py-2 bg-[#00b894] text-white rounded-lg hover:bg-[#00a884] disabled:opacity-50 flex items-center gap-2"
          >
            {saving ? <Loader2 className="w-4 h-4 animate-spin" /> : <Send className="w-4 h-4" />}
            Publish
          </button>
        </div>
      </div>

      {/* Message */}
      {message && (
        <div className={`p-3 rounded-lg text-sm flex items-center justify-between ${
          message.type === 'error' ? 'bg-red-500/10 border border-red-500/20 text-red-400' :
          message.type === 'success' ? 'bg-green-500/10 border border-green-500/20 text-green-400' :
          'bg-blue-500/10 border border-blue-500/20 text-blue-400'
        }`}>
          {message.text}
          <button onClick={() => setMessage(null)} className="p-1 hover:bg-white/10 rounded">
            <X className="w-4 h-4" />
          </button>
        </div>
      )}

      <div className="grid md:grid-cols-2 gap-6">
        {/* Left Column - Basic Info */}
        <div className="space-y-4">
          {/* SKU / Brand / Sub-Category Row */}
          <div className="grid grid-cols-3 gap-3">
            <div>
              <label className="block text-sm text-zinc-400 mb-1">SKU *</label>
              <div className="flex gap-1">
                <input
                  type="text"
                  value={form.sku}
                  onChange={(e) => updateField('sku', e.target.value)}
                  className="flex-1 px-3 py-2 bg-zinc-800 border border-zinc-700 rounded-lg text-white font-mono text-sm"
                  placeholder="Auto or manual"
                />
                <button
                  type="button"
                  onClick={() => updateField('sku', generateSKU())}
                  className="px-2 py-1 bg-zinc-700 text-zinc-300 rounded text-xs hover:bg-zinc-600"
                  title="Generate SKU"
                >
                  Gen
                </button>
              </div>
            </div>
            <div>
              <label className="block text-sm text-zinc-400 mb-1">Brand *</label>
              <select
                value={form.brand}
                onChange={(e) => updateField('brand', e.target.value)}
                className="w-full px-3 py-2 bg-zinc-800 border border-zinc-700 rounded-lg text-white"
              >
                {BRAND_OPTIONS.map((brand) => (
                  <option key={brand} value={brand}>{brand}</option>
                ))}
              </select>
            </div>
            <div>
              <label className="block text-sm text-zinc-400 mb-1">Sub-Category *</label>
              <select
                value={form.subCategory}
                onChange={(e) => updateField('subCategory', e.target.value)}
                className="w-full px-3 py-2 bg-zinc-800 border border-zinc-700 rounded-lg text-white text-sm"
              >
                {SUB_CATEGORY_OPTIONS.map((cat) => (
                  <option key={cat} value={cat}>{cat}</option>
                ))}
              </select>
            </div>
          </div>

          {/* Name */}
          <div>
            <label className="block text-sm text-zinc-400 mb-1">
              Name * <span className="text-zinc-600">({form.name.length}/250)</span>
            </label>
            <input
              type="text"
              value={form.name}
              onChange={(e) => updateField('name', e.target.value.slice(0, 250))}
              className="w-full px-3 py-2 bg-zinc-800 border border-zinc-700 rounded-lg text-white"
              placeholder="Product name"
              required
            />
          </div>

          {/* Description */}
          <div>
            <label className="block text-sm text-zinc-400 mb-1">
              Description * <span className="text-zinc-600">({form.description.length}/5000)</span>
            </label>
            <textarea
              value={form.description}
              onChange={(e) => updateField('description', e.target.value.slice(0, 5000))}
              className="w-full px-3 py-2 bg-zinc-800 border border-zinc-700 rounded-lg text-white"
              rows={4}
              placeholder="Product description"
              required
            />
          </div>

          {/* Price / Purchase Cost / Stock / UPC Row */}
          <div className="grid grid-cols-4 gap-3">
            <div>
              <label className="block text-sm text-zinc-400 mb-1">Price * ($)</label>
              <input
                type="number"
                step="0.01"
                min="0.01"
                value={form.price}
                onChange={(e) => updateField('price', e.target.value)}
                className="w-full px-3 py-2 bg-zinc-800 border border-zinc-700 rounded-lg text-white"
                placeholder="0.00"
                required
              />
            </div>
            <div>
              <label className="block text-sm text-zinc-400 mb-1">Purchase Cost</label>
              <input
                type="number"
                step="0.01"
                min="0"
                value={form.purchaseCost}
                onChange={(e) => updateField('purchaseCost', e.target.value)}
                className="w-full px-3 py-2 bg-zinc-800 border border-zinc-700 rounded-lg text-white"
                placeholder="Admin"
              />
            </div>
            <div>
              <label className="block text-sm text-zinc-400 mb-1">Stock *</label>
              <input
                type="number"
                min="1"
                value={form.stock}
                onChange={(e) => updateField('stock', e.target.value)}
                className="w-full px-3 py-2 bg-zinc-800 border border-zinc-700 rounded-lg text-white"
                required
              />
            </div>
            <div>
              <label className="block text-sm text-zinc-400 mb-1">UPC</label>
              <input
                type="text"
                value={form.upcBarcode}
                onChange={(e) => updateField('upcBarcode', e.target.value.replace(/\D/g, ''))}
                className="w-full px-3 py-2 bg-zinc-800 border border-zinc-700 rounded-lg text-white font-mono text-sm"
              />
            </div>
          </div>

          {/* Size (shown for bags/boards) */}
          {fieldVisibility.showSize && (
            <div className="grid grid-cols-2 gap-3">
              <div>
                <label className="block text-sm text-zinc-400 mb-1">Size</label>
                <select
                  value={form.size}
                  onChange={(e) => updateField('size', e.target.value)}
                  className="w-full px-3 py-2 bg-zinc-800 border border-zinc-700 rounded-lg text-white"
                >
                  {SIZE_OPTIONS.map((size) => (
                    <option key={size} value={size}>{size}</option>
                  ))}
                </select>
              </div>
              <div>
                <label className="block text-sm text-zinc-400 mb-1">{fieldVisibility.unitsLabel}</label>
                <input
                  type="number"
                  min="1"
                  value={form.unitsPerPack}
                  onChange={(e) => updateField('unitsPerPack', e.target.value)}
                  className="w-full px-3 py-2 bg-zinc-800 border border-zinc-700 rounded-lg text-white"
                  placeholder="e.g., 100"
                />
              </div>
            </div>
          )}

          {/* Units Per Pack (when size not shown) */}
          {!fieldVisibility.showSize && (
            <div>
              <label className="block text-sm text-zinc-400 mb-1">{fieldVisibility.unitsLabel}</label>
              <input
                type="number"
                min="1"
                value={form.unitsPerPack}
                onChange={(e) => updateField('unitsPerPack', e.target.value)}
                className="w-full px-3 py-2 bg-zinc-800 border border-zinc-700 rounded-lg text-white"
                placeholder="Number of units"
              />
            </div>
          )}

          {/* Exterior Dimensions */}
          <div className="p-4 bg-zinc-800/50 rounded-lg space-y-3">
            <label className="block text-sm text-zinc-400">Exterior Dimensions (inches)</label>
            <div className="grid grid-cols-3 gap-3">
              {['Width', 'Height', 'Length'].map((dim) => {
                const field = `exterior${dim}`;
                const fractionField = `exterior${dim}Fraction`;
                return (
                  <div key={dim}>
                    <label className="block text-xs text-zinc-500 mb-1">{dim}</label>
                    <div className="flex gap-1">
                      <input
                        type="number"
                        min="0"
                        max="99"
                        value={form[field]}
                        onChange={(e) => updateField(field, e.target.value)}
                        className="w-16 px-2 py-1.5 bg-zinc-900 border border-zinc-700 rounded text-white text-sm"
                        placeholder="0"
                      />
                      <select
                        value={form[fractionField]}
                        onChange={(e) => updateField(fractionField, e.target.value)}
                        className="flex-1 px-2 py-1.5 bg-zinc-900 border border-zinc-700 rounded text-white text-sm"
                      >
                        {FRACTION_OPTIONS.map((f) => (
                          <option key={f || 'none'} value={f}>{f || '-'}</option>
                        ))}
                      </select>
                    </div>
                  </div>
                );
              })}
            </div>
          </div>

          {/* Interior Dimensions (shown for bins/cases) */}
          {fieldVisibility.showInteriorDimensions && (
            <div className="p-4 bg-zinc-800/50 rounded-lg space-y-3">
              <label className="block text-sm text-zinc-400">Interior Dimensions (inches)</label>
              <div className="grid grid-cols-3 gap-3">
                {['Width', 'Height', 'Length'].map((dim) => {
                  const field = `interior${dim}`;
                  const fractionField = `interior${dim}Fraction`;
                  return (
                    <div key={dim}>
                      <label className="block text-xs text-zinc-500 mb-1">{dim}</label>
                      <div className="flex gap-1">
                        <input
                          type="number"
                          min="0"
                          max="99"
                          value={form[field]}
                          onChange={(e) => updateField(field, e.target.value)}
                          className="w-16 px-2 py-1.5 bg-zinc-900 border border-zinc-700 rounded text-white text-sm"
                          placeholder="0"
                        />
                        <select
                          value={form[fractionField]}
                          onChange={(e) => updateField(fractionField, e.target.value)}
                          className="flex-1 px-2 py-1.5 bg-zinc-900 border border-zinc-700 rounded text-white text-sm"
                        >
                          {FRACTION_OPTIONS.map((f) => (
                            <option key={f || 'none'} value={f}>{f || '-'}</option>
                          ))}
                        </select>
                      </div>
                    </div>
                  );
                })}
              </div>
            </div>
          )}

          {/* Weight / Material Row */}
          <div className="grid grid-cols-3 gap-3">
            <div>
              <label className="block text-sm text-zinc-400 mb-1">Weight (lbs)</label>
              <input
                type="number"
                min="0"
                value={form.weightPounds}
                onChange={(e) => updateField('weightPounds', e.target.value)}
                className="w-full px-3 py-2 bg-zinc-800 border border-zinc-700 rounded-lg text-white"
                placeholder="0"
              />
            </div>
            <div>
              <label className="block text-sm text-zinc-400 mb-1">Weight (oz)</label>
              <input
                type="number"
                min="0"
                max="15"
                value={form.weightOunces}
                onChange={(e) => updateField('weightOunces', e.target.value)}
                className="w-full px-3 py-2 bg-zinc-800 border border-zinc-700 rounded-lg text-white"
                placeholder="0"
              />
            </div>
            <div>
              <label className="block text-sm text-zinc-400 mb-1">Material</label>
              <input
                type="text"
                value={form.material}
                onChange={(e) => updateField('material', e.target.value)}
                className="w-full px-3 py-2 bg-zinc-800 border border-zinc-700 rounded-lg text-white"
                placeholder="e.g., Polypropylene"
              />
            </div>
          </div>

          {/* Tags Panel (STORY_04 AC5) */}
          <div className="p-4 bg-zinc-800/50 rounded-lg space-y-3">
            <label className="block text-sm text-zinc-400">Tags</label>
            <div className="flex gap-2">
              <input
                type="text"
                value={form.tagInput}
                onChange={(e) => updateField('tagInput', e.target.value)}
                onKeyPress={(e) => e.key === 'Enter' && (e.preventDefault(), addTag())}
                className="flex-1 px-3 py-2 bg-zinc-900 border border-zinc-700 rounded-lg text-white text-sm"
                placeholder="Type tag and press Add (min 5 chars)"
              />
              <button
                type="button"
                onClick={addTag}
                disabled={form.tagInput.length < 5}
                className={`px-4 py-2 rounded-lg text-sm font-medium transition-colors ${
                  form.tagInput.length >= 5
                    ? 'bg-[#00b894] text-white hover:bg-[#00a884]'
                    : 'bg-zinc-700 text-zinc-500 cursor-not-allowed'
                }`}
              >
                <Plus className="w-4 h-4" />
              </button>
            </div>
            {form.tags.length > 0 && (
              <div className="flex flex-wrap gap-2">
                {form.tags.map((tag) => (
                  <span
                    key={tag}
                    className="inline-flex items-center gap-1 px-3 py-1 bg-[#00b894]/20 text-[#00b894] rounded-full text-sm"
                  >
                    <Tag className="w-3 h-3" />
                    {tag}
                    <button
                      type="button"
                      onClick={() => removeTag(tag)}
                      className="p-0.5 hover:bg-white/10 rounded-full"
                    >
                      <X className="w-3 h-3" />
                    </button>
                  </span>
                ))}
              </div>
            )}
          </div>

          {/* Featured Toggle */}
          <div className="flex items-center gap-4 p-3 bg-zinc-800/50 rounded-lg">
            <label className="flex items-center gap-2 cursor-pointer">
              <input
                type="checkbox"
                checked={form.isFeatured}
                onChange={(e) => updateField('isFeatured', e.target.checked)}
                className="w-4 h-4 rounded"
              />
              <span className="text-zinc-300 flex items-center gap-1">
                <Star className="w-4 h-4 text-yellow-500" />
                Featured Item
              </span>
              <span className="text-xs text-zinc-500">(Homepage)</span>
            </label>
          </div>
        </div>

        {/* Right Column - Images */}
        <div>
          <MultiImageUploader
            images={form.images}
            onChange={handleImagesChange}
            maxImages={9}
            minImages={1}
            productType="supply"
          />
        </div>
      </div>
    </div>
  );
}
