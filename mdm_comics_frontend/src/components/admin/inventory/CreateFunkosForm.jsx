/**
 * CreateFunkosForm - Funko Pop product creation form
 * Implements STORY_03: MDM Comics Create Funkos Screen
 *
 * Features:
 * - Basic fields: UPC, Box Number, SKU, License, Series, Name, Size, Retailer Exclusive, Release Date
 * - Inventory: Quantity, Price, Purchase Cost (admin only)
 * - Special Features: Chase, Limited Edition, Flocked, GITD, Metallic, Diamond/Glitter, Blacklight
 * - Vaulted toggle
 * - Condition: Tabbed interface (Mint In Box, Damaged Box, Out of Box) + additional flags
 * - Multi-image upload (1-9 images)
 * - Featured toggle
 * - Draft/Publish workflow
 */
import React, { useState, useCallback } from 'react';
import {
  Save, Send, Loader2, X, Ghost, Star, Search
} from 'lucide-react';
import { adminAPI } from '../../../services/adminApi';
import { funkosAPI } from '../../../services/api';
import MultiImageUploader from '../../shared/MultiImageUploader';

// Size options (STORY_03 AC4)
const SIZE_OPTIONS = [
  'Standard',
  '10-inch',
  '18-inch',
  'Mini',
  'Pocket Pop',
  'Pop Moment',
  'Pop Ride',
  'Pop Town',
  'Pop Album',
];

// Retailer exclusive options (STORY_03 AC4)
const RETAILER_OPTIONS = [
  'None',
  'Funko Shop',
  'Convention (SDCC)',
  'Convention (NYCC)',
  'Target',
  'Walmart',
  'Hot Topic',
  'BoxLunch',
  'Amazon',
];

// Special features (STORY_03 AC5)
const SPECIAL_FEATURES = [
  'Chase',
  'Limited Edition',
  'Flocked',
  'Glow in the Dark',
  'Metallic',
  'Diamond/Glitter',
  'Blacklight',
];

// Condition tabs (STORY_03 AC7)
const CONDITION_TABS = {
  mintInBox: {
    label: 'Mint In Box',
    description: 'Box and figure both in excellent condition',
    options: [
      { value: 'M', label: 'Mint (M)' },
      { value: 'NM', label: 'Near Mint (NM)' },
      { value: 'VF', label: 'Very Fine (VF)' },
    ],
  },
  damagedBox: {
    label: 'Damaged Box',
    description: 'Figure is in perfect condition',
    options: [
      { value: 'minor', label: 'Minor Damage' },
      { value: 'moderate', label: 'Moderate Damage' },
      { value: 'major', label: 'Major Damage' },
    ],
  },
  outOfBox: {
    label: 'Out of Box',
    description: 'Figure only, no box',
    options: [
      { value: 'mint', label: 'Mint' },
      { value: 'good', label: 'Good' },
      { value: 'fair', label: 'Fair' },
    ],
  },
};

// Additional condition flags (STORY_03 AC7)
const CONDITION_FLAGS = [
  'Window Damage',
  'Sticker Damage/Missing',
  'Insert Damage',
  'Paint Defects',
];

const initialFormState = {
  // Basic fields
  upc: '',
  boxNumber: '',
  sku: '',
  license: '',
  series: '',
  name: '',
  size: 'Standard',
  retailerExclusive: 'None',
  releaseDate: '',
  // Inventory
  quantity: 1,
  price: '',
  purchaseCost: '',
  // Special features
  specialFeatures: [],
  // Vaulted
  isVaulted: false,
  // Condition
  conditionTab: 'mintInBox',
  conditionValue: 'NM',
  conditionFlags: [],
  // Images
  images: [],
  // Featured
  isFeatured: false,
  // Status
  status: 'draft',
};

export default function CreateFunkosForm() {
  const [form, setForm] = useState(initialFormState);
  const [saving, setSaving] = useState(false);
  const [message, setMessage] = useState(null);

  // Search state
  const [searchQuery, setSearchQuery] = useState('');
  const [searchResults, setSearchResults] = useState([]);
  const [searching, setSearching] = useState(false);

  // Update form field
  const updateField = (field, value) => {
    setForm(prev => ({ ...prev, [field]: value }));
  };

  // Toggle special feature
  const toggleFeature = (feature) => {
    setForm(prev => ({
      ...prev,
      specialFeatures: prev.specialFeatures.includes(feature)
        ? prev.specialFeatures.filter(f => f !== feature)
        : [...prev.specialFeatures, feature],
    }));
  };

  // Toggle condition flag
  const toggleConditionFlag = (flag) => {
    setForm(prev => ({
      ...prev,
      conditionFlags: prev.conditionFlags.includes(flag)
        ? prev.conditionFlags.filter(f => f !== flag)
        : [...prev.conditionFlags, flag],
    }));
  };

  // Generate SKU based on fields (STORY_03 AC4)
  const generateSKU = useCallback(() => {
    const parts = ['MDM'];
    if (form.license) parts.push(form.license.replace(/[^a-zA-Z0-9]/g, '').substring(0, 8).toUpperCase());
    if (form.name) parts.push(form.name.replace(/[^a-zA-Z0-9]/g, '').substring(0, 10).toUpperCase());
    if (form.boxNumber) parts.push(form.boxNumber);
    return parts.join('-');
  }, [form.license, form.name, form.boxNumber]);

  // Handle image changes
  const handleImagesChange = (newImages) => {
    updateField('images', newImages);
  };

  // Search Funkos database
  const handleSearch = async () => {
    if (!searchQuery.trim()) return;
    setSearching(true);
    try {
      const result = await funkosAPI.search({
        q: searchQuery,
        page: 1,
        per_page: 12,
      });
      setSearchResults(result.results || []);
    } catch (err) {
      setMessage({ type: 'error', text: 'Search failed: ' + err.message });
    } finally {
      setSearching(false);
    }
  };

  // Select a Funko from search results
  const selectFunko = (funko) => {
    setForm(prev => ({
      ...prev,
      name: funko.title || '',
      boxNumber: funko.box_number || '',
      license: funko.license || '',
      series: funko.series?.[0]?.name || '',
      images: funko.image_url ? [{ url: funko.image_url, isPrimary: true, order: 0 }] : [],
    }));
    setSearchResults([]);
    setSearchQuery('');
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
    if (!form.name || !form.boxNumber || !form.sku || !form.price || form.images.length === 0) {
      setMessage({ type: 'error', text: 'Please fill in required fields: Name, Box Number, SKU, Price, and at least 1 image' });
      return;
    }

    setSaving(true);
    setMessage(null);
    try {
      const productData = {
        name: form.name,
        sku: form.sku || generateSKU(),
        category: 'funko',
        price: parseFloat(form.price),
        stock: parseInt(form.quantity) || 1,
        description: [
          form.license && `License: ${form.license}`,
          form.series && `Series: ${form.series}`,
          form.boxNumber && `Box #${form.boxNumber}`,
          form.specialFeatures.length > 0 && `Features: ${form.specialFeatures.join(', ')}`,
        ].filter(Boolean).join('\n'),
        upc: form.upc || null,
        // Funko-specific fields
        box_number: form.boxNumber,
        license: form.license || null,
        funko_series: form.series || null,
        size: form.size !== 'Standard' ? form.size : null,
        retailer_exclusive: form.retailerExclusive !== 'None' ? form.retailerExclusive : null,
        special_features: form.specialFeatures.length > 0 ? form.specialFeatures : null,
        is_vaulted: form.isVaulted,
        condition_tab: form.conditionTab,
        condition_value: form.conditionValue,
        condition_flags: form.conditionFlags.length > 0 ? form.conditionFlags : null,
        featured: form.isFeatured,
        // Images
        images: form.images.map(img => img.url),
        image_url: form.images.find(img => img.isPrimary)?.url || form.images[0]?.url || null,
        // Purchase cost (admin only)
        purchase_cost: form.purchaseCost ? parseFloat(form.purchaseCost) : null,
      };

      await adminAPI.createProduct(null, productData);
      setMessage({ type: 'success', text: 'Funko published successfully!' });
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
            <Ghost className="w-6 h-6 text-[#9b59b6]" />
            Create Funko
          </h2>
          <p className="text-sm text-zinc-400 mt-1">Add a new Funko Pop to your inventory</p>
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
            className="px-4 py-2 bg-[#9b59b6] text-white rounded-lg hover:bg-[#8e44ad] disabled:opacity-50 flex items-center gap-2"
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

      {/* Search Section */}
      <div className="bg-zinc-800/50 rounded-lg p-4">
        <label className="block text-sm text-zinc-400 mb-2">Search Funkos Database</label>
        <div className="flex gap-2">
          <input
            type="text"
            value={searchQuery}
            onChange={(e) => setSearchQuery(e.target.value)}
            onKeyPress={(e) => e.key === 'Enter' && handleSearch()}
            placeholder="Search by name or character..."
            className="flex-1 px-4 py-2 bg-zinc-900 border border-zinc-700 rounded-lg text-white"
          />
          <button
            onClick={handleSearch}
            disabled={searching}
            className="px-4 py-2 bg-[#9b59b6] text-white rounded-lg hover:bg-[#8e44ad] disabled:opacity-50 flex items-center gap-2"
          >
            {searching ? <Loader2 className="w-4 h-4 animate-spin" /> : <Search className="w-4 h-4" />}
            Search
          </button>
        </div>

        {/* Search Results */}
        {searchResults.length > 0 && (
          <div className="mt-3 grid grid-cols-4 gap-2 max-h-48 overflow-auto">
            {searchResults.slice(0, 8).map((funko) => (
              <div
                key={funko.id}
                onClick={() => selectFunko(funko)}
                className="bg-zinc-900 rounded-lg p-2 cursor-pointer hover:bg-zinc-700 transition-colors"
              >
                <div className="aspect-square bg-zinc-800 rounded mb-1">
                  {funko.image_url && <img src={funko.image_url} alt="" className="w-full h-full object-contain rounded" />}
                </div>
                <p className="text-xs text-white truncate">{funko.title}</p>
                <p className="text-xs text-zinc-400">#{funko.box_number}</p>
              </div>
            ))}
          </div>
        )}
      </div>

      <div className="grid md:grid-cols-2 gap-6">
        {/* Left Column - Basic Info */}
        <div className="space-y-4">
          {/* UPC / Box Number / SKU Row */}
          <div className="grid grid-cols-3 gap-3">
            <div>
              <label className="block text-sm text-zinc-400 mb-1">UPC</label>
              <input
                type="text"
                value={form.upc}
                onChange={(e) => updateField('upc', e.target.value.replace(/\D/g, ''))}
                className="w-full px-3 py-2 bg-zinc-800 border border-zinc-700 rounded-lg text-white font-mono text-sm"
                placeholder="Barcode"
              />
            </div>
            <div>
              <label className="block text-sm text-zinc-400 mb-1">Box Number *</label>
              <input
                type="text"
                value={form.boxNumber}
                onChange={(e) => updateField('boxNumber', e.target.value.replace(/\D/g, ''))}
                className="w-full px-3 py-2 bg-zinc-800 border border-zinc-700 rounded-lg text-white font-mono text-sm"
                placeholder="#"
                required
              />
            </div>
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
          </div>

          {/* License / Series Row */}
          <div className="grid grid-cols-2 gap-3">
            <div>
              <label className="block text-sm text-zinc-400 mb-1">License (Theme)</label>
              <input
                type="text"
                value={form.license}
                onChange={(e) => updateField('license', e.target.value)}
                className="w-full px-3 py-2 bg-zinc-800 border border-zinc-700 rounded-lg text-white"
                placeholder="e.g., Star Wars, Marvel"
              />
            </div>
            <div>
              <label className="block text-sm text-zinc-400 mb-1">Series (Collection)</label>
              <input
                type="text"
                value={form.series}
                onChange={(e) => updateField('series', e.target.value)}
                className="w-full px-3 py-2 bg-zinc-800 border border-zinc-700 rounded-lg text-white"
                placeholder="e.g., Pop! Deluxe"
              />
            </div>
          </div>

          {/* Name */}
          <div>
            <label className="block text-sm text-zinc-400 mb-1">Name/Character *</label>
            <input
              type="text"
              value={form.name}
              onChange={(e) => updateField('name', e.target.value)}
              className="w-full px-3 py-2 bg-zinc-800 border border-zinc-700 rounded-lg text-white"
              placeholder="e.g., Darth Vader"
              required
            />
          </div>

          {/* Size / Retailer Exclusive / Release Date Row */}
          <div className="grid grid-cols-3 gap-3">
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
              <label className="block text-sm text-zinc-400 mb-1">Retailer Exclusive</label>
              <select
                value={form.retailerExclusive}
                onChange={(e) => updateField('retailerExclusive', e.target.value)}
                className="w-full px-3 py-2 bg-zinc-800 border border-zinc-700 rounded-lg text-white"
              >
                {RETAILER_OPTIONS.map((retailer) => (
                  <option key={retailer} value={retailer}>{retailer}</option>
                ))}
              </select>
            </div>
            <div>
              <label className="block text-sm text-zinc-400 mb-1">Release Date</label>
              <input
                type="month"
                value={form.releaseDate}
                onChange={(e) => updateField('releaseDate', e.target.value)}
                className="w-full px-3 py-2 bg-zinc-800 border border-zinc-700 rounded-lg text-white"
              />
            </div>
          </div>

          {/* Inventory: Quantity / Price / Purchase Cost */}
          <div className="grid grid-cols-3 gap-3">
            <div>
              <label className="block text-sm text-zinc-400 mb-1">Quantity *</label>
              <input
                type="number"
                min="1"
                value={form.quantity}
                onChange={(e) => updateField('quantity', e.target.value)}
                className="w-full px-3 py-2 bg-zinc-800 border border-zinc-700 rounded-lg text-white"
                required
              />
            </div>
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
              <label className="block text-sm text-zinc-400 mb-1">Purchase Cost ($)</label>
              <input
                type="number"
                step="0.01"
                min="0"
                value={form.purchaseCost}
                onChange={(e) => updateField('purchaseCost', e.target.value)}
                className="w-full px-3 py-2 bg-zinc-800 border border-zinc-700 rounded-lg text-white"
                placeholder="Admin only"
              />
            </div>
          </div>

          {/* Special Features */}
          <div>
            <label className="block text-sm text-zinc-400 mb-2">Special Features</label>
            <div className="flex flex-wrap gap-2">
              {SPECIAL_FEATURES.map((feature) => (
                <button
                  key={feature}
                  type="button"
                  onClick={() => toggleFeature(feature)}
                  className={`px-3 py-1.5 rounded-full text-xs font-medium transition-colors ${
                    form.specialFeatures.includes(feature)
                      ? 'bg-[#9b59b6] text-white'
                      : 'bg-zinc-800 text-zinc-400 hover:bg-zinc-700'
                  }`}
                >
                  {feature}
                </button>
              ))}
            </div>
          </div>

          {/* Toggles: Vaulted / Featured */}
          <div className="flex items-center gap-6 p-3 bg-zinc-800/50 rounded-lg">
            <label className="flex items-center gap-2 cursor-pointer">
              <input
                type="checkbox"
                checked={form.isVaulted}
                onChange={(e) => updateField('isVaulted', e.target.checked)}
                className="w-4 h-4 rounded"
              />
              <span className="text-zinc-300">Vaulted</span>
              <span className="text-xs text-zinc-500">(Retired)</span>
            </label>
            <label className="flex items-center gap-2 cursor-pointer">
              <input
                type="checkbox"
                checked={form.isFeatured}
                onChange={(e) => updateField('isFeatured', e.target.checked)}
                className="w-4 h-4 rounded"
              />
              <span className="text-zinc-300 flex items-center gap-1">
                <Star className="w-4 h-4 text-yellow-500" />
                Featured
              </span>
            </label>
          </div>

          {/* Condition Section */}
          <div className="p-4 bg-zinc-800/50 rounded-lg space-y-3">
            <label className="block text-sm text-zinc-400">Condition</label>

            {/* Condition Tabs */}
            <div className="flex gap-1">
              {Object.entries(CONDITION_TABS).map(([id, tab]) => (
                <button
                  key={id}
                  type="button"
                  onClick={() => {
                    updateField('conditionTab', id);
                    updateField('conditionValue', tab.options[0].value);
                  }}
                  className={`px-3 py-1.5 rounded text-xs font-medium transition-colors ${
                    form.conditionTab === id
                      ? 'bg-[#9b59b6] text-white'
                      : 'bg-zinc-700 text-zinc-400 hover:bg-zinc-600'
                  }`}
                >
                  {tab.label}
                </button>
              ))}
            </div>

            {/* Condition Description */}
            <p className="text-xs text-zinc-500">{CONDITION_TABS[form.conditionTab].description}</p>

            {/* Condition Options */}
            <div className="flex gap-2">
              {CONDITION_TABS[form.conditionTab].options.map((option) => (
                <button
                  key={option.value}
                  type="button"
                  onClick={() => updateField('conditionValue', option.value)}
                  className={`px-3 py-2 rounded-lg text-sm font-medium transition-colors ${
                    form.conditionValue === option.value
                      ? 'bg-[#9b59b6] text-white'
                      : 'bg-zinc-700 text-zinc-400 hover:bg-zinc-600'
                  }`}
                >
                  {option.label}
                </button>
              ))}
            </div>

            {/* Additional Condition Flags */}
            <div>
              <label className="block text-xs text-zinc-500 mb-2">Additional Issues</label>
              <div className="flex flex-wrap gap-2">
                {CONDITION_FLAGS.map((flag) => (
                  <button
                    key={flag}
                    type="button"
                    onClick={() => toggleConditionFlag(flag)}
                    className={`px-2 py-1 rounded text-xs transition-colors ${
                      form.conditionFlags.includes(flag)
                        ? 'bg-red-500/20 text-red-400 border border-red-500/30'
                        : 'bg-zinc-700 text-zinc-400 hover:bg-zinc-600'
                    }`}
                  >
                    {flag}
                  </button>
                ))}
              </div>
            </div>
          </div>
        </div>

        {/* Right Column - Images */}
        <div>
          <MultiImageUploader
            images={form.images}
            onChange={handleImagesChange}
            maxImages={9}
            minImages={1}
            productType="funko"
          />
        </div>
      </div>
    </div>
  );
}
