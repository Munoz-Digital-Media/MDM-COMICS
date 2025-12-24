/**
 * CreateComicsForm - Comic book product creation form
 * Implements STORY_02: MDM Comics Create Comics Screen
 *
 * Features:
 * - Basic fields: UPC, ISBN, SKU, Publisher, Series, Volume, Issue, Variant, Cover Date
 * - Inventory: Quantity, Price, Purchase Cost (admin only)
 * - Grading: Raw condition selector OR Graded book fields (CGC, CBCS, PGX, EGS)
 * - Key Issue: Tabbed interface for key issue types
 * - Multi-image upload (1-9 images)
 * - Draft/Publish workflow
 */
import React, { useState, useCallback } from 'react';
import {
  Save, Send, Loader2, X, Plus, Star, Award, BookOpen,
  Upload, Trash2, GripVertical, Search
} from 'lucide-react';
import { adminAPI } from '../../../services/adminApi';
import { comicsAPI } from '../../../services/api';
import MultiImageUploader from '../../shared/MultiImageUploader';

// Raw book condition options (STORY_02 AC11)
const RAW_CONDITIONS = [
  { value: 'NM', label: 'Near Mint (NM)', description: 'Nearly perfect, minor defects only', gradeRange: '9.2-9.8' },
  { value: 'VF', label: 'Very Fine (VF)', description: 'Minor defects, excellent overall condition', gradeRange: '7.5-9.0' },
  { value: 'FN', label: 'Fine (FN)', description: 'Above average, some wear visible', gradeRange: '5.5-7.0' },
  { value: 'VG', label: 'Very Good (VG)', description: 'Significant wear, still desirable', gradeRange: '3.5-5.0' },
  { value: 'GD', label: 'Good (GD)', description: 'Major defects, complete and readable', gradeRange: '1.8-3.0' },
  { value: 'FR', label: 'Fair (FR)', description: 'Major damage, all story pages intact', gradeRange: '1.0-1.5' },
  { value: 'PR', label: 'Poor (PR)', description: 'Severe damage, may be missing pages', gradeRange: '0.5' },
];

// Grading companies (STORY_02 AC15)
const GRADING_COMPANIES = ['CGC', 'CBCS', 'PGX', 'EGS'];

// Key issue types with tabs (STORY_02 AC14)
const KEY_ISSUE_TABS = {
  appearance: {
    label: 'Appearance',
    options: ['1st Cameo', '1st Appearance', '1st Full Appearance', '1st Team Appearance', '1st Cover'],
  },
  origin: {
    label: 'Origin',
    options: ['Origin Issue', 'Origin Retold', 'Death of [CHARACTER]', 'Resurrection'],
  },
  milestones: {
    label: 'Milestones',
    options: ['1st Costume/Suit', 'New Identity Debut', 'Identity Reveal', 'Wedding Issue', 'Final Appearance'],
  },
  crossovers: {
    label: 'Crossovers',
    options: ['1st Meeting', '1st Battle', '1st Team-Up'],
  },
  creative: {
    label: 'Creative',
    options: ['1st Work by [NOTABLE_CREATOR]', 'Iconic/Classic Cover', 'Controversial/Banned Issue'],
  },
  collectibility: {
    label: 'Collectibility',
    options: ['Low Print Run', 'Newsstand Edition', 'Recalled/Error Issue', 'Milestone Issue Number', 'Final Issue'],
  },
};

const initialFormState = {
  // Basic fields
  upc: '',
  isbn: '',
  sku: '',
  publisher: '',
  series: '',
  volume: '',
  issue: '',
  variant: '',
  coverDate: '',
  // Inventory
  quantity: 1,
  price: '',
  purchaseCost: '',
  // Grading
  isGraded: false,
  condition: 'NM', // For raw books
  gradingCompany: 'CGC',
  grade: '',
  certificationNumber: '',
  // Key issue
  isKey: false,
  keyTypes: [],
  keyNotes: '',
  // Images
  images: [],
  // Status
  status: 'draft',
};

export default function CreateComicsForm() {
  const [form, setForm] = useState(initialFormState);
  const [saving, setSaving] = useState(false);
  const [message, setMessage] = useState(null);
  const [activeKeyTab, setActiveKeyTab] = useState('appearance');

  // Search state
  const [searchQuery, setSearchQuery] = useState('');
  const [searchResults, setSearchResults] = useState([]);
  const [searching, setSearching] = useState(false);

  // Update form field
  const updateField = (field, value) => {
    setForm(prev => ({ ...prev, [field]: value }));
  };

  // Toggle key issue type
  const toggleKeyType = (type) => {
    setForm(prev => ({
      ...prev,
      keyTypes: prev.keyTypes.includes(type)
        ? prev.keyTypes.filter(t => t !== type)
        : [...prev.keyTypes, type],
    }));
  };

  // Generate SKU based on fields (STORY_02 AC4)
  const generateSKU = useCallback(() => {
    const parts = ['MDM'];
    if (form.series) parts.push(form.series.replace(/[^a-zA-Z0-9]/g, '').substring(0, 10).toUpperCase());
    if (form.volume) parts.push(`V${form.volume}`);
    if (form.issue) parts.push(form.issue);
    if (form.variant) parts.push(form.variant.substring(0, 1).toUpperCase());
    return parts.join('-');
  }, [form.series, form.volume, form.issue, form.variant]);

  // Handle image changes from MultiImageUploader
  const handleImagesChange = (newImages) => {
    updateField('images', newImages);
  };

  // Search comics database
  const handleSearch = async () => {
    if (!searchQuery.trim()) return;
    setSearching(true);
    try {
      const result = await comicsAPI.search({
        series: searchQuery,
        page: 1,
      });
      setSearchResults(result.results || []);
    } catch (err) {
      setMessage({ type: 'error', text: 'Search failed: ' + err.message });
    } finally {
      setSearching(false);
    }
  };

  // Select a comic from search results
  const selectComic = async (comic) => {
    try {
      const details = await comicsAPI.getIssue(comic.id);
      setForm(prev => ({
        ...prev,
        series: details.series?.name || comic.series?.name || '',
        issue: details.number || comic.number || '',
        publisher: details.publisher?.name || details.series?.publisher?.name || '',
        volume: details.series?.volume || '',
        upc: details.upc || '',
        coverDate: details.cover_date ? details.cover_date.substring(0, 7) : '', // MM/YYYY
        images: details.image ? [{ url: details.image, isPrimary: true, order: 0 }] : [],
      }));
      setSearchResults([]);
      setSearchQuery('');
    } catch (err) {
      setMessage({ type: 'error', text: 'Failed to load comic details: ' + err.message });
    }
  };

  // Save draft
  const handleSaveDraft = async () => {
    setSaving(true);
    setMessage(null);
    try {
      // TODO: Implement draft save API
      await new Promise(resolve => setTimeout(resolve, 500)); // Simulated
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
    if (!form.series || !form.issue || !form.sku || !form.price || form.images.length === 0) {
      setMessage({ type: 'error', text: 'Please fill in required fields: Series, Issue, SKU, Price, and at least 1 image' });
      return;
    }

    if (form.isGraded && (!form.grade || !form.certificationNumber)) {
      setMessage({ type: 'error', text: 'Graded books require Grade and Certification Number' });
      return;
    }

    setSaving(true);
    setMessage(null);
    try {
      const productData = {
        name: `${form.series}${form.issue ? ' #' + form.issue : ''}${form.variant ? ' (' + form.variant + ')' : ''}`,
        sku: form.sku || generateSKU(),
        category: 'comics',
        price: parseFloat(form.price),
        stock: parseInt(form.quantity) || 1,
        description: '',
        upc: form.upc || null,
        publisher: form.publisher || null,
        issue_number: form.issue || null,
        // Grading fields
        is_graded: form.isGraded,
        grading_company: form.isGraded ? form.gradingCompany.toLowerCase() : null,
        cgc_grade: form.isGraded ? parseFloat(form.grade) : null,
        certification_number: form.isGraded ? form.certificationNumber : null,
        // Raw condition
        condition: !form.isGraded ? form.condition : null,
        // Key issue
        is_key: form.isKey,
        key_types: form.isKey ? form.keyTypes : null,
        key_notes: form.isKey ? form.keyNotes : null,
        // Images
        images: form.images.map(img => img.url),
        image_url: form.images.find(img => img.isPrimary)?.url || form.images[0]?.url || null,
        // Purchase cost (admin only)
        purchase_cost: form.purchaseCost ? parseFloat(form.purchaseCost) : null,
      };

      await adminAPI.createProduct(null, productData);
      setMessage({ type: 'success', text: 'Comic published successfully!' });
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
            <BookOpen className="w-6 h-6 text-[#e94560]" />
            Create Comic
          </h2>
          <p className="text-sm text-zinc-400 mt-1">Add a new comic book to your inventory</p>
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
            className="px-4 py-2 bg-[#e94560] text-white rounded-lg hover:bg-[#d63d56] disabled:opacity-50 flex items-center gap-2"
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
        <label className="block text-sm text-zinc-400 mb-2">Search Comics Database</label>
        <div className="flex gap-2">
          <input
            type="text"
            value={searchQuery}
            onChange={(e) => setSearchQuery(e.target.value)}
            onKeyPress={(e) => e.key === 'Enter' && handleSearch()}
            placeholder="Search by series name..."
            className="flex-1 px-4 py-2 bg-zinc-900 border border-zinc-700 rounded-lg text-white"
          />
          <button
            onClick={handleSearch}
            disabled={searching}
            className="px-4 py-2 bg-[#e94560] text-white rounded-lg hover:bg-[#d63d56] disabled:opacity-50 flex items-center gap-2"
          >
            {searching ? <Loader2 className="w-4 h-4 animate-spin" /> : <Search className="w-4 h-4" />}
            Search
          </button>
        </div>

        {/* Search Results */}
        {searchResults.length > 0 && (
          <div className="mt-3 grid grid-cols-4 gap-2 max-h-48 overflow-auto">
            {searchResults.slice(0, 8).map((comic) => (
              <div
                key={comic.id}
                onClick={() => selectComic(comic)}
                className="bg-zinc-900 rounded-lg p-2 cursor-pointer hover:bg-zinc-700 transition-colors"
              >
                <div className="aspect-[2/3] bg-zinc-800 rounded mb-1">
                  {comic.image && <img src={comic.image} alt="" className="w-full h-full object-contain rounded" />}
                </div>
                <p className="text-xs text-white truncate">{comic.series?.name}</p>
                <p className="text-xs text-zinc-400">#{comic.number}</p>
              </div>
            ))}
          </div>
        )}
      </div>

      <div className="grid md:grid-cols-2 gap-6">
        {/* Left Column - Basic Info */}
        <div className="space-y-4">
          {/* UPC / ISBN / SKU Row */}
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
              <label className="block text-sm text-zinc-400 mb-1">ISBN</label>
              <input
                type="text"
                value={form.isbn}
                onChange={(e) => updateField('isbn', e.target.value.replace(/\D/g, ''))}
                className="w-full px-3 py-2 bg-zinc-800 border border-zinc-700 rounded-lg text-white font-mono text-sm"
                placeholder="For TPBs"
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

          {/* Publisher / Series Row */}
          <div className="grid grid-cols-2 gap-3">
            <div>
              <label className="block text-sm text-zinc-400 mb-1">Publisher</label>
              <input
                type="text"
                value={form.publisher}
                onChange={(e) => updateField('publisher', e.target.value)}
                className="w-full px-3 py-2 bg-zinc-800 border border-zinc-700 rounded-lg text-white"
                placeholder="e.g., Marvel, DC"
              />
            </div>
            <div>
              <label className="block text-sm text-zinc-400 mb-1">Series *</label>
              <input
                type="text"
                value={form.series}
                onChange={(e) => updateField('series', e.target.value)}
                className="w-full px-3 py-2 bg-zinc-800 border border-zinc-700 rounded-lg text-white"
                placeholder="e.g., Amazing Spider-Man"
                required
              />
            </div>
          </div>

          {/* Volume / Issue / Variant / Cover Date Row */}
          <div className="grid grid-cols-4 gap-3">
            <div>
              <label className="block text-sm text-zinc-400 mb-1">Volume</label>
              <input
                type="text"
                value={form.volume}
                onChange={(e) => updateField('volume', e.target.value.replace(/\D/g, ''))}
                className="w-full px-3 py-2 bg-zinc-800 border border-zinc-700 rounded-lg text-white"
                placeholder="Vol"
              />
            </div>
            <div>
              <label className="block text-sm text-zinc-400 mb-1">Issue *</label>
              <input
                type="text"
                value={form.issue}
                onChange={(e) => updateField('issue', e.target.value)}
                className="w-full px-3 py-2 bg-zinc-800 border border-zinc-700 rounded-lg text-white"
                placeholder="#"
                required
              />
            </div>
            <div>
              <label className="block text-sm text-zinc-400 mb-1">Variant</label>
              <input
                type="text"
                value={form.variant}
                onChange={(e) => updateField('variant', e.target.value.replace(/[0-9]/g, ''))}
                className="w-full px-3 py-2 bg-zinc-800 border border-zinc-700 rounded-lg text-white"
                placeholder="A, B..."
              />
            </div>
            <div>
              <label className="block text-sm text-zinc-400 mb-1">Cover Date</label>
              <input
                type="month"
                value={form.coverDate}
                onChange={(e) => updateField('coverDate', e.target.value)}
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

          {/* Graded Toggle */}
          <div className="flex items-center gap-4 p-3 bg-zinc-800/50 rounded-lg">
            <label className="flex items-center gap-2 cursor-pointer">
              <input
                type="checkbox"
                checked={form.isGraded}
                onChange={(e) => updateField('isGraded', e.target.checked)}
                className="w-4 h-4 rounded"
              />
              <span className="text-zinc-300 flex items-center gap-1">
                <Award className="w-4 h-4 text-yellow-500" />
                Graded Book
              </span>
            </label>
            <label className="flex items-center gap-2 cursor-pointer">
              <input
                type="checkbox"
                checked={form.isKey}
                onChange={(e) => updateField('isKey', e.target.checked)}
                className="w-4 h-4 rounded"
              />
              <span className="text-zinc-300 flex items-center gap-1">
                <Star className="w-4 h-4 text-orange-500" />
                Key Issue
              </span>
            </label>
          </div>

          {/* Raw Condition (shown when NOT graded) */}
          {!form.isGraded && (
            <div>
              <label className="block text-sm text-zinc-400 mb-2">Condition</label>
              <div className="grid grid-cols-4 gap-2">
                {RAW_CONDITIONS.slice(0, 4).map((cond) => (
                  <button
                    key={cond.value}
                    type="button"
                    onClick={() => updateField('condition', cond.value)}
                    className={`px-3 py-2 rounded-lg text-sm font-medium transition-colors ${
                      form.condition === cond.value
                        ? 'bg-[#e94560] text-white'
                        : 'bg-zinc-800 text-zinc-400 hover:bg-zinc-700'
                    }`}
                    title={cond.description}
                  >
                    {cond.value}
                  </button>
                ))}
              </div>
              <div className="grid grid-cols-3 gap-2 mt-2">
                {RAW_CONDITIONS.slice(4).map((cond) => (
                  <button
                    key={cond.value}
                    type="button"
                    onClick={() => updateField('condition', cond.value)}
                    className={`px-3 py-2 rounded-lg text-sm font-medium transition-colors ${
                      form.condition === cond.value
                        ? 'bg-[#e94560] text-white'
                        : 'bg-zinc-800 text-zinc-400 hover:bg-zinc-700'
                    }`}
                    title={cond.description}
                  >
                    {cond.value}
                  </button>
                ))}
              </div>
            </div>
          )}

          {/* Graded Book Fields (shown when graded) */}
          {form.isGraded && (
            <div className="p-4 bg-yellow-500/10 border border-yellow-500/30 rounded-lg space-y-3">
              <h4 className="text-sm font-semibold text-yellow-400 flex items-center gap-2">
                <Award className="w-4 h-4" />
                Grading Information
              </h4>
              <div className="grid grid-cols-2 gap-3">
                <div>
                  <label className="block text-sm text-zinc-400 mb-1">Grading Company *</label>
                  <select
                    value={form.gradingCompany}
                    onChange={(e) => updateField('gradingCompany', e.target.value)}
                    className="w-full px-3 py-2 bg-zinc-800 border border-zinc-700 rounded-lg text-white"
                  >
                    {GRADING_COMPANIES.map((company) => (
                      <option key={company} value={company}>{company}</option>
                    ))}
                  </select>
                </div>
                <div>
                  <label className="block text-sm text-zinc-400 mb-1">Grade * (0.5-10.0)</label>
                  <input
                    type="number"
                    step="0.1"
                    min="0.5"
                    max="10.0"
                    value={form.grade}
                    onChange={(e) => updateField('grade', e.target.value)}
                    className="w-full px-3 py-2 bg-zinc-800 border border-zinc-700 rounded-lg text-white"
                    placeholder="9.8"
                  />
                </div>
              </div>
              <div>
                <label className="block text-sm text-zinc-400 mb-1">Certification Number *</label>
                <input
                  type="text"
                  value={form.certificationNumber}
                  onChange={(e) => updateField('certificationNumber', e.target.value)}
                  className="w-full px-3 py-2 bg-zinc-800 border border-zinc-700 rounded-lg text-white font-mono"
                  placeholder="e.g., 4375892006"
                />
              </div>
            </div>
          )}

          {/* Key Issue Section (shown when key) */}
          {form.isKey && (
            <div className="p-4 bg-orange-500/10 border border-orange-500/30 rounded-lg space-y-3">
              <h4 className="text-sm font-semibold text-orange-400 flex items-center gap-2">
                <Star className="w-4 h-4" />
                Key Issue Details
              </h4>

              {/* Key Type Tabs */}
              <div className="flex gap-1 flex-wrap">
                {Object.entries(KEY_ISSUE_TABS).map(([id, tab]) => {
                  const hasSelection = tab.options.some(opt => form.keyTypes.includes(opt));
                  return (
                    <button
                      key={id}
                      type="button"
                      onClick={() => setActiveKeyTab(id)}
                      className={`px-3 py-1.5 rounded text-xs font-medium transition-colors ${
                        activeKeyTab === id
                          ? 'bg-orange-500 text-white'
                          : hasSelection
                            ? 'bg-orange-500/30 text-orange-300'
                            : 'bg-zinc-800 text-zinc-400 hover:bg-zinc-700'
                      }`}
                    >
                      {tab.label}
                      {hasSelection && <span className="ml-1">*</span>}
                    </button>
                  );
                })}
              </div>

              {/* Key Type Options */}
              <div className="flex flex-wrap gap-2">
                {KEY_ISSUE_TABS[activeKeyTab].options.map((option) => (
                  <button
                    key={option}
                    type="button"
                    onClick={() => toggleKeyType(option)}
                    className={`px-3 py-1.5 rounded-full text-xs font-medium transition-colors flex items-center gap-1 ${
                      form.keyTypes.includes(option)
                        ? 'bg-orange-500 text-white'
                        : 'bg-zinc-800 text-zinc-400 hover:bg-zinc-700'
                    }`}
                  >
                    {option}
                    {form.keyTypes.includes(option) && (
                      <X className="w-3 h-3" onClick={(e) => { e.stopPropagation(); toggleKeyType(option); }} />
                    )}
                  </button>
                ))}
              </div>

              {/* Selected Key Types Display */}
              {form.keyTypes.length > 0 && (
                <div className="text-xs text-zinc-400">
                  Selected: {form.keyTypes.join(', ')}
                </div>
              )}

              {/* Key Notes */}
              <div>
                <label className="block text-sm text-zinc-400 mb-1">
                  Key Notes <span className="text-zinc-600">({form.keyNotes.length}/500)</span>
                </label>
                <textarea
                  value={form.keyNotes}
                  onChange={(e) => updateField('keyNotes', e.target.value.slice(0, 500))}
                  className="w-full px-3 py-2 bg-zinc-800 border border-zinc-700 rounded-lg text-white text-sm"
                  rows={2}
                  placeholder="Additional notes about why this is a key issue..."
                />
              </div>
            </div>
          )}
        </div>

        {/* Right Column - Images */}
        <div>
          <MultiImageUploader
            images={form.images}
            onChange={handleImagesChange}
            maxImages={9}
            minImages={1}
            productType="comic"
          />
        </div>
      </div>
    </div>
  );
}
