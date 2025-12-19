/**
 * ProductCreator - Search databases and create products
 * Consolidated search & create functionality for Catalog module
 */
import React, { useState, useEffect, useRef, useCallback } from 'react';
import {
  Search, Plus, Save, Package, Loader2, Upload, Camera, ScanLine,
  Tag, ChevronLeft, ChevronRight, X, BookOpen
} from 'lucide-react';
import { comicsAPI, funkosAPI } from '../../../services/api';
import { adminAPI } from '../../../services/adminApi';

const SEARCH_MODES = {
  COMICS: 'comics',
  FUNKOS: 'funkos',
  BCW_SUPPLIES: 'bcw_supplies',
};

export default function ProductCreator() {
  const [searchMode, setSearchMode] = useState(SEARCH_MODES.COMICS);
  const [showCreateForm, setShowCreateForm] = useState(false);

  // Comic search state
  const [searchParams, setSearchParams] = useState({ series: '', number: '', publisher: '', upc: '' });
  const [searchResults, setSearchResults] = useState([]);
  const [searchLoading, setSearchLoading] = useState(false);
  const [selectedComic, setSelectedComic] = useState(null);
  const [detailsLoading, setDetailsLoading] = useState(false);

  // Funko search state
  const [funkoSearchQuery, setFunkoSearchQuery] = useState('');
  const [funkoSeriesFilter, setFunkoSeriesFilter] = useState('');
  const [funkoResults, setFunkoResults] = useState([]);
  const [funkoLoading, setFunkoLoading] = useState(false);
  const [funkoPage, setFunkoPage] = useState(1);
  const [funkoTotalPages, setFunkoTotalPages] = useState(1);
  const [funkoTotal, setFunkoTotal] = useState(0);
  const [funkoStats, setFunkoStats] = useState(null);

  // BCW search state
  const [bcwSearchQuery, setBcwSearchQuery] = useState('');
  const [bcwCategoryFilter, setBcwCategoryFilter] = useState('');
  const [bcwResults, setBcwResults] = useState([]);
  const [bcwLoading, setBcwLoading] = useState(false);
  const [bcwPage, setBcwPage] = useState(1);
  const [bcwTotalPages, setBcwTotalPages] = useState(1);
  const [bcwTotal, setBcwTotal] = useState(0);
  const [bcwCategories, setBcwCategories] = useState([]);

  // Product form state
  const [productForm, setProductForm] = useState({
    sku: '', name: '', description: '', category: 'comics', subcategory: '',
    price: '', original_price: '', stock: 1, image_url: '',
    issue_number: '', publisher: '', year: '', upc: '', featured: false, tags: [],
    variant: '',
  });
  const [saving, setSaving] = useState(false);
  const [message, setMessage] = useState(null);

  // Load Funko stats on mount - v1.1: Fixed silent error swallowing
  useEffect(() => {
    funkosAPI.getStats()
      .then(setFunkoStats)
      .catch((err) => {
        console.error('[ProductCreator] Failed to load Funko stats:', err);
        // Stats are optional, don't block UI
      });
  }, []);

  // Load BCW categories when BCW mode is selected
  useEffect(() => {
    if (searchMode === SEARCH_MODES.BCW_SUPPLIES && bcwCategories.length === 0) {
      adminAPI.getBCWCategories()
        .then(data => setBcwCategories(data.categories || []))
        .catch(err => console.error('[ProductCreator] Failed to load BCW categories:', err));
    }
  }, [searchMode, bcwCategories.length]);

  // Comic search handler
  const handleComicSearch = async (e) => {
    e?.preventDefault();
    if (!searchParams.series && !searchParams.upc) return;

    setSearchLoading(true);
    try {
      const result = await comicsAPI.search({
        series: searchParams.series,
        number: searchParams.number,
        publisher: searchParams.publisher,
        upc: searchParams.upc,
        page: 1,
      });
      setSearchResults(result.results || []);
      // Check for API message (rate limit, timeout, etc.)
      if (result.message) {
        setMessage({ type: 'warning', text: result.message });
      } else if (result.results?.length === 0) {
        setMessage({ type: 'info', text: 'No results found' });
      }
    } catch (err) {
      setMessage({ type: 'error', text: 'Search failed: ' + err.message });
    } finally {
      setSearchLoading(false);
    }
  };

  // Funko search handler
  const handleFunkoSearch = async (pageNum = 1) => {
    if (!funkoSearchQuery.trim() && !funkoSeriesFilter.trim()) return;

    setFunkoLoading(true);
    try {
      const data = await funkosAPI.search({
        q: funkoSearchQuery || undefined,
        series: funkoSeriesFilter || undefined,
        page: pageNum,
        per_page: 20,
      });
      setFunkoResults(data.results || []);
      setFunkoTotalPages(data.pages || 1);
      setFunkoTotal(data.total || 0);
      setFunkoPage(pageNum);
    } catch (err) {
      setMessage({ type: 'error', text: 'Funko search failed: ' + err.message });
    } finally {
      setFunkoLoading(false);
    }
  };

  // BCW search handler
  const handleBCWSearch = async (pageNum = 1) => {
    if (!bcwSearchQuery.trim() && !bcwCategoryFilter) return;

    setBcwLoading(true);
    try {
      const data = await adminAPI.searchBCWCatalog({
        q: bcwSearchQuery || undefined,
        category: bcwCategoryFilter || undefined,
        page: pageNum,
        per_page: 20,
      });
      setBcwResults(data.results || []);
      setBcwTotalPages(data.pages || 1);
      setBcwTotal(data.total || 0);
      setBcwPage(pageNum);
    } catch (err) {
      setMessage({ type: 'error', text: 'BCW search failed: ' + err.message });
    } finally {
      setBcwLoading(false);
    }
  };

  // Build enhanced description from comic details
  const buildEnhancedDescription = (details) => {
    let desc = details.desc || '';
    const parts = [];

    if (details.variants?.length > 0) {
      const matchingVariant = details.variants.find(v => v.image === details.image);
      if (matchingVariant?.name) {
        parts.push('Variant: ' + matchingVariant.name);
      }
    } else if (details.variant) {
      parts.push('Variant: ' + details.variant);
    }

    if (details.credits?.length > 0) {
      const hasRole = (c, roleName) => Array.isArray(c.role)
        ? c.role.some(r => r.name?.toLowerCase().includes(roleName))
        : c.role?.toLowerCase?.().includes(roleName);

      const coverArtists = details.credits.filter(c => hasRole(c, 'cover'));
      if (coverArtists.length > 0) {
        const names = coverArtists.map(c => c.creator || c.creator?.name).filter(Boolean);
        if (names.length > 0) parts.push('Cover Art by ' + names.join(', '));
      }

      const writer = details.credits.find(c => hasRole(c, 'writer'));
      if (writer) parts.push('Written by ' + (writer.creator || writer.creator?.name));

      const artist = details.credits.find(c => hasRole(c, 'artist') && !hasRole(c, 'cover'));
      if (artist) parts.push('Art by ' + (artist.creator || artist.creator?.name));
    }

    if (parts.length > 0) {
      desc = desc ? desc + '\n\n' + parts.join(' | ') : parts.join(' | ');
    }
    return desc;
  };

  // Select a comic to create product
  const selectComic = async (comic) => {
    setSelectedComic(comic);
    setDetailsLoading(true);
    setIsBCWProduct(false);
    try {
      const details = await comicsAPI.getIssue(comic.id);
      const seriesName = details.series?.name || comic.series?.name || '';
      const issueNum = details.number || comic.number || '';
      const pubName = details.publisher?.name || details.series?.publisher?.name || '';
      const coverYear = details.cover_date ? new Date(details.cover_date).getFullYear() : '';
      const coverPrice = details.price || '';

      setProductForm({
        sku: 'COMIC-' + comic.id,
        name: seriesName + (issueNum ? ' #' + issueNum : ''),
        description: buildEnhancedDescription(details),
        category: 'comics',
        subcategory: pubName,
        price: '',
        original_price: coverPrice,
        stock: 1,
        image_url: details.image || comic.image || '',
        issue_number: issueNum,
        publisher: pubName,
        year: coverYear,
        upc: details.upc || '',
        featured: false,
        tags: [],
        variant: '',
      });
      setShowCreateForm(true);
    } catch (err) {
      setMessage({ type: 'error', text: 'Failed to load comic details: ' + err.message });
    } finally {
      setDetailsLoading(false);
    }
  };

  // Select a Funko to create product
  const selectFunko = (funko) => {
    setIsBCWProduct(false);
    const descParts = [];
    if (funko.license) descParts.push(`License: ${funko.license}`);
    if (funko.product_type) descParts.push(`Type: ${funko.product_type}`);
    if (funko.box_number) descParts.push(`Box #${funko.box_number}`);
    if (funko.category && funko.category !== funko.license) descParts.push(`Category: ${funko.category}`);
    if (funko.series?.length) descParts.push(`Series: ${funko.series.map(s => s.name).join(', ')}`);

    setProductForm({
      sku: funko.box_number ? `FUNKO-${funko.box_number}` : `FUNKO-${funko.id}`,
      name: funko.title,
      description: descParts.join('\n') || '',
      category: 'funko',
      subcategory: funko.license || funko.series?.[0]?.name || '',
      price: '',
      original_price: '',
      stock: 1,
      image_url: funko.image_url || '',
      issue_number: funko.box_number || '',
      publisher: funko.license || '',
      year: '',
      upc: '',
      featured: false,
      tags: funko.series?.map(s => s.name) || [],
      variant: '',
    });
    setShowCreateForm(true);
  };

  // Track if current product is from BCW (for using correct API)
  const [isBCWProduct, setIsBCWProduct] = useState(false);

  // Select a BCW product to create
  const selectBCWProduct = (bcwProduct) => {
    setIsBCWProduct(true);
    setProductForm({
      sku: bcwProduct.mdm_sku,
      name: bcwProduct.product_name,
      description: `BCW ${bcwProduct.product_name}. Professional-grade comic book storage and protection.`,
      category: 'supplies',
      subcategory: bcwProduct.bcw_category || '',
      price: bcwProduct.pricing?.our_price || bcwProduct.pricing?.bcw_msrp || '',
      original_price: bcwProduct.pricing?.bcw_msrp || '',
      stock: 0,
      image_url: bcwProduct.image_url || '',
      issue_number: '',
      publisher: 'BCW',
      year: '',
      upc: '',
      featured: false,
      tags: ['bcw', 'supplies'],
      variant: '',
    });
    setShowCreateForm(true);
  };

  // Create product
  const handleCreateProduct = async (e) => {
    e.preventDefault();
    if (!productForm.price || !productForm.name || !productForm.sku) {
      setMessage({ type: 'error', text: 'Name, SKU, and Price are required' });
      return;
    }

    setSaving(true);
    try {
      const price = parseFloat(productForm.price);

      // BCW products use dedicated activate endpoint (handles create/update)
      if (isBCWProduct) {
        await adminAPI.activateBCWProduct(productForm.sku, price);
        setMessage({ type: 'success', text: 'BCW product activated in catalog!' });
      } else {
        // Regular product creation for comics/funkos
        const fullName = productForm.variant
          ? `${productForm.name} (${productForm.variant})`
          : productForm.name;

        const data = {
          ...productForm,
          name: fullName,
          price: price,
          original_price: productForm.original_price ? parseFloat(productForm.original_price) : null,
          stock: parseInt(productForm.stock) || 0,
          year: productForm.year ? parseInt(productForm.year) : null,
          images: productForm.image_url ? [productForm.image_url] : [],
          // Clean up empty string fields
          upc: productForm.upc || null,
          issue_number: productForm.issue_number || null,
          publisher: productForm.publisher || null,
          subcategory: productForm.subcategory || null,
        };
        delete data.variant;

        await adminAPI.createProduct(null, data);
        setMessage({ type: 'success', text: 'Product created successfully!' });
      }

      // Reset form
      setProductForm({
        sku: '', name: '', description: '', category: 'comics', subcategory: '',
        price: '', original_price: '', stock: 1, image_url: '',
        issue_number: '', publisher: '', year: '', upc: '', featured: false, tags: [],
        variant: '',
      });
      setSelectedComic(null);
      setIsBCWProduct(false);
      setShowCreateForm(false);
    } catch (err) {
      setMessage({ type: 'error', text: 'Failed: ' + err.message });
    } finally {
      setSaving(false);
    }
  };

  // Clear search
  const clearSearch = () => {
    setSearchParams({ series: '', number: '', publisher: '', upc: '' });
    setSearchResults([]);
    setFunkoSearchQuery('');
    setFunkoSeriesFilter('');
    setFunkoResults([]);
    setBcwSearchQuery('');
    setBcwCategoryFilter('');
    setBcwResults([]);
    setMessage(null);
  };

  return (
    <div className="space-y-4">
      {/* Message */}
      {message && (
        <div className={`p-3 rounded-lg text-sm flex items-center justify-between ${
          message.type === 'error' ? 'bg-red-500/10 border border-red-500/20 text-red-400' :
          message.type === 'success' ? 'bg-green-500/10 border border-green-500/20 text-green-400' :
          message.type === 'warning' ? 'bg-yellow-500/10 border border-yellow-500/20 text-yellow-400' :
          'bg-blue-500/10 border border-blue-500/20 text-blue-400'
        }`}>
          {message.text}
          <button onClick={() => setMessage(null)} className="p-1 hover:bg-white/10 rounded">
            <X className="w-4 h-4" />
          </button>
        </div>
      )}

      {/* Create Form Modal */}
      {showCreateForm && (
        <div className="fixed inset-0 z-50 flex items-center justify-center">
          <div className="absolute inset-0 bg-black/60" onClick={() => setShowCreateForm(false)} />
          <div className="relative bg-zinc-900 border border-zinc-800 rounded-xl w-full max-w-4xl mx-4 max-h-[90vh] overflow-auto">
            <div className="sticky top-0 bg-zinc-900 border-b border-zinc-800 p-4 flex items-center justify-between">
              <h3 className="text-lg font-semibold text-white">Create Product</h3>
              <button onClick={() => setShowCreateForm(false)} className="p-1 hover:bg-zinc-800 rounded">
                <X className="w-5 h-5 text-zinc-400" />
              </button>
            </div>

            <form onSubmit={handleCreateProduct} className="p-6 grid md:grid-cols-2 gap-6">
              <div className="space-y-4">
                <div className="grid grid-cols-2 gap-3">
                  <div>
                    <label className="block text-sm text-zinc-400 mb-1">SKU *</label>
                    <input type="text" value={productForm.sku} onChange={(e) => setProductForm({ ...productForm, sku: e.target.value })} className="w-full px-3 py-2 bg-zinc-800 border border-zinc-700 rounded-lg text-white" required />
                  </div>
                  <div>
                    <label className="block text-sm text-zinc-400 mb-1">Category</label>
                    <select value={productForm.category} onChange={(e) => setProductForm({ ...productForm, category: e.target.value })} className="w-full px-3 py-2 bg-zinc-800 border border-zinc-700 rounded-lg text-white">
                      <option value="comics">Comics</option>
                      <option value="funko">Funko</option>
                      <option value="supplies">Supplies</option>
                    </select>
                  </div>
                </div>

                <div>
                  <label className="block text-sm text-zinc-400 mb-1">Name *</label>
                  <input type="text" value={productForm.name} onChange={(e) => setProductForm({ ...productForm, name: e.target.value })} className="w-full px-3 py-2 bg-zinc-800 border border-zinc-700 rounded-lg text-white" required />
                </div>

                <div>
                  <label className="block text-sm text-zinc-400 mb-1">Variant</label>
                  <input
                    type="text"
                    value={productForm.variant}
                    onChange={(e) => setProductForm({ ...productForm, variant: e.target.value.replace(/[0-9]/g, '') })}
                    placeholder="e.g., Newsstand, Gold Foil Cover"
                    className="w-full px-3 py-2 bg-zinc-800 border border-zinc-700 rounded-lg text-white placeholder-zinc-600"
                  />
                </div>

                <div>
                  <label className="block text-sm text-zinc-400 mb-1">Description</label>
                  <textarea value={productForm.description} onChange={(e) => setProductForm({ ...productForm, description: e.target.value })} rows={4} className="w-full px-3 py-2 bg-zinc-800 border border-zinc-700 rounded-lg text-white" />
                </div>

                <div className="grid grid-cols-3 gap-3">
                  <div>
                    <label className="block text-sm text-zinc-400 mb-1">Price *</label>
                    <input type="number" step="0.01" min="0.01" value={productForm.price} onChange={(e) => setProductForm({ ...productForm, price: e.target.value })} className="w-full px-3 py-2 bg-zinc-800 border border-zinc-700 rounded-lg text-white" required />
                  </div>
                  <div>
                    <label className="block text-sm text-zinc-400 mb-1">Original Price</label>
                    <input type="number" step="0.01" min="0" value={productForm.original_price} onChange={(e) => setProductForm({ ...productForm, original_price: e.target.value })} className="w-full px-3 py-2 bg-zinc-800 border border-zinc-700 rounded-lg text-white" />
                  </div>
                  <div>
                    <label className="block text-sm text-zinc-400 mb-1">Stock</label>
                    <input type="number" min="0" value={productForm.stock} onChange={(e) => setProductForm({ ...productForm, stock: e.target.value })} className="w-full px-3 py-2 bg-zinc-800 border border-zinc-700 rounded-lg text-white" />
                  </div>
                </div>

                <div className="grid grid-cols-3 gap-3">
                  <div>
                    <label className="block text-sm text-zinc-400 mb-1">Publisher</label>
                    <input type="text" value={productForm.publisher} onChange={(e) => setProductForm({ ...productForm, publisher: e.target.value })} className="w-full px-3 py-2 bg-zinc-800 border border-zinc-700 rounded-lg text-white" />
                  </div>
                  <div>
                    <label className="block text-sm text-zinc-400 mb-1">Issue #</label>
                    <input type="text" value={productForm.issue_number} onChange={(e) => setProductForm({ ...productForm, issue_number: e.target.value })} className="w-full px-3 py-2 bg-zinc-800 border border-zinc-700 rounded-lg text-white" />
                  </div>
                  <div>
                    <label className="block text-sm text-zinc-400 mb-1">Year</label>
                    <input type="number" min="1900" max="2099" value={productForm.year} onChange={(e) => setProductForm({ ...productForm, year: e.target.value })} className="w-full px-3 py-2 bg-zinc-800 border border-zinc-700 rounded-lg text-white" />
                  </div>
                </div>

                <div>
                  <label className="block text-sm text-zinc-400 mb-1">UPC Barcode</label>
                  <input type="text" value={productForm.upc} onChange={(e) => setProductForm({ ...productForm, upc: e.target.value.replace(/[^0-9]/g, '') })} className="w-full px-3 py-2 bg-zinc-800 border border-zinc-700 rounded-lg text-white font-mono" />
                </div>

                <div>
                  <label className="block text-sm text-zinc-400 mb-1">Image URL</label>
                  <input type="url" value={productForm.image_url} onChange={(e) => setProductForm({ ...productForm, image_url: e.target.value })} className="w-full px-3 py-2 bg-zinc-800 border border-zinc-700 rounded-lg text-white" />
                </div>

                <label className="flex items-center gap-2 cursor-pointer">
                  <input type="checkbox" checked={productForm.featured} onChange={(e) => setProductForm({ ...productForm, featured: e.target.checked })} className="w-4 h-4 rounded" />
                  <span className="text-zinc-300">Featured Product</span>
                </label>

                <button type="submit" disabled={saving} className="w-full py-3 bg-orange-500 text-white rounded-lg font-bold hover:bg-orange-600 disabled:opacity-50 flex items-center justify-center gap-2">
                  {saving ? <Loader2 className="w-5 h-5 animate-spin" /> : <><Save className="w-5 h-5" />Create Product</>}
                </button>
              </div>

              {/* Preview */}
              <div className="bg-zinc-800 rounded-xl p-4">
                <h4 className="font-medium text-zinc-400 mb-3">Preview</h4>
                <div className="bg-zinc-900 rounded-lg overflow-hidden">
                  <div className="h-48 bg-zinc-700">
                    {productForm.image_url && <img src={productForm.image_url} alt="" className="w-full h-full object-contain" onError={(e) => e.target.style.display = 'none'} />}
                  </div>
                  <div className="p-4">
                    <p className="text-xs text-orange-500 mb-1">{productForm.subcategory || productForm.publisher}</p>
                    <h4 className="font-bold text-white mb-2">{productForm.name ? (productForm.variant ? `${productForm.name} (${productForm.variant})` : productForm.name) : 'Product Name'}</h4>
                    <p className="text-sm text-zinc-500 mb-3 line-clamp-2">{productForm.description || 'Description...'}</p>
                    <div className="flex items-center justify-between">
                      <div>
                        <span className="text-xl font-bold text-white">${productForm.price || '0.00'}</span>
                        {productForm.original_price && <span className="ml-2 text-sm text-zinc-500 line-through">${productForm.original_price}</span>}
                      </div>
                      <span className="text-sm text-zinc-400">Stock: {productForm.stock}</span>
                    </div>
                  </div>
                </div>
              </div>
            </form>
          </div>
        </div>
      )}

      {/* Search Mode Toggle */}
      <div className="flex gap-2">
        <button
          onClick={() => setSearchMode(SEARCH_MODES.COMICS)}
          className={`px-4 py-2 rounded-lg font-medium flex items-center gap-2 transition-colors ${
            searchMode === SEARCH_MODES.COMICS
              ? 'bg-orange-500/20 text-orange-400 border border-orange-500/30'
              : 'bg-zinc-800 text-zinc-400 hover:text-white'
          }`}
        >
          <BookOpen className="w-4 h-4" />
          Comics
        </button>
        <button
          onClick={() => setSearchMode(SEARCH_MODES.FUNKOS)}
          className={`px-4 py-2 rounded-lg font-medium flex items-center gap-2 transition-colors ${
            searchMode === SEARCH_MODES.FUNKOS
              ? 'bg-purple-500/20 text-purple-400 border border-purple-500/30'
              : 'bg-zinc-800 text-zinc-400 hover:text-white'
          }`}
        >
          <Package className="w-4 h-4" />
          Funkos
        </button>
        <button
          onClick={() => setSearchMode(SEARCH_MODES.BCW_SUPPLIES)}
          className={`px-4 py-2 rounded-lg font-medium flex items-center gap-2 transition-colors ${
            searchMode === SEARCH_MODES.BCW_SUPPLIES
              ? 'bg-green-500/20 text-green-400 border border-green-500/30'
              : 'bg-zinc-800 text-zinc-400 hover:text-white'
          }`}
        >
          <Tag className="w-4 h-4" />
          BCW Supplies
        </button>

        {(searchResults.length > 0 || funkoResults.length > 0 || bcwResults.length > 0) && (
          <button
            onClick={clearSearch}
            className="ml-auto px-3 py-2 text-sm text-zinc-400 hover:text-white hover:bg-zinc-800 rounded-lg flex items-center gap-2"
          >
            <X className="w-4 h-4" />
            Clear
          </button>
        )}
      </div>

      {/* Comic Search */}
      {searchMode === SEARCH_MODES.COMICS && (
        <div className="space-y-4">
          {/* UPC Search */}
          <div className="flex items-center gap-2 mb-2">
            <ScanLine className="w-4 h-4 text-zinc-400" />
            <span className="text-sm text-zinc-400">Barcode Search</span>
          </div>
          <form onSubmit={handleComicSearch} className="flex gap-3">
            <input
              type="text"
              placeholder="Enter UPC barcode..."
              value={searchParams.upc}
              onChange={(e) => setSearchParams({ ...searchParams, upc: e.target.value.replace(/\D/g, '') })}
              className="flex-1 px-4 py-2 bg-zinc-800 border border-zinc-700 rounded-lg text-white font-mono"
            />
            <button type="submit" disabled={searchLoading || !searchParams.upc} className="px-6 py-2 bg-orange-500 text-white rounded-lg font-medium hover:bg-orange-600 disabled:opacity-50">
              {searchLoading ? <Loader2 className="w-5 h-5 animate-spin" /> : 'Scan'}
            </button>
          </form>

          <div className="flex items-center gap-3">
            <div className="flex-1 border-t border-zinc-700"></div>
            <span className="text-xs text-zinc-500">OR</span>
            <div className="flex-1 border-t border-zinc-700"></div>
          </div>

          {/* Text Search */}
          <form onSubmit={handleComicSearch} className="flex gap-3">
            <input type="text" placeholder="Series name..." value={searchParams.series} onChange={(e) => setSearchParams({ ...searchParams, series: e.target.value })} className="flex-1 px-4 py-2 bg-zinc-800 border border-zinc-700 rounded-lg text-white" />
            <input type="text" placeholder="Issue #" value={searchParams.number} onChange={(e) => setSearchParams({ ...searchParams, number: e.target.value })} className="w-24 px-4 py-2 bg-zinc-800 border border-zinc-700 rounded-lg text-white" />
            <input type="text" placeholder="Publisher" value={searchParams.publisher} onChange={(e) => setSearchParams({ ...searchParams, publisher: e.target.value })} className="w-32 px-4 py-2 bg-zinc-800 border border-zinc-700 rounded-lg text-white" />
            <button type="submit" disabled={searchLoading || (!searchParams.series && !searchParams.upc)} className="px-6 py-2 bg-orange-500 text-white rounded-lg font-medium hover:bg-orange-600 disabled:opacity-50">
              {searchLoading ? <Loader2 className="w-5 h-5 animate-spin" /> : 'Search'}
            </button>
          </form>

          {/* Results */}
          {searchResults.length > 0 && (
            <div className="grid grid-cols-2 md:grid-cols-4 lg:grid-cols-5 gap-3">
              {searchResults.map((comic) => (
                <div
                  key={comic.id}
                  onClick={() => selectComic(comic)}
                  className="bg-zinc-800 rounded-lg overflow-hidden cursor-pointer hover:ring-2 hover:ring-orange-500 transition-all group"
                >
                  <div className="aspect-[2/3] bg-zinc-700 relative">
                    {comic.image && <img src={comic.image} alt="" className="w-full h-full object-cover" />}
                    <div className="absolute inset-0 bg-black/70 opacity-0 group-hover:opacity-100 transition-opacity flex items-center justify-center">
                      <span className="px-3 py-1.5 bg-orange-500 text-white rounded-lg text-sm font-medium flex items-center gap-1">
                        <Plus className="w-4 h-4" />
                        Create
                      </span>
                    </div>
                  </div>
                  <div className="p-2">
                    <p className="text-xs text-orange-500 truncate">{comic.series?.name}</p>
                    <p className="text-sm font-medium text-white truncate">#{comic.number}</p>
                    <p className="text-xs text-zinc-500">{comic.cover_date}</p>
                  </div>
                </div>
              ))}
            </div>
          )}

          {searchResults.length === 0 && !searchLoading && (
            <div className="text-center py-12 text-zinc-500">
              <BookOpen className="w-12 h-12 mx-auto mb-3 text-zinc-700" />
              <p>Search for comics by series name, issue number, or UPC barcode</p>
            </div>
          )}
        </div>
      )}

      {/* Funko Search */}
      {searchMode === SEARCH_MODES.FUNKOS && (
        <div className="space-y-4">
          <p className="text-sm text-zinc-500">
            Search {funkoStats ? `${funkoStats.total_funkos.toLocaleString()} Funkos` : 'the database'}
          </p>

          <div className="flex gap-3">
            <input
              type="text"
              value={funkoSearchQuery}
              onChange={(e) => setFunkoSearchQuery(e.target.value)}
              onKeyPress={(e) => e.key === 'Enter' && handleFunkoSearch(1)}
              placeholder="Search by title (e.g., Spider-Man, Darth Vader)"
              className="flex-1 px-4 py-2 bg-zinc-800 border border-zinc-700 rounded-lg text-white"
            />
            <input
              type="text"
              value={funkoSeriesFilter}
              onChange={(e) => setFunkoSeriesFilter(e.target.value)}
              onKeyPress={(e) => e.key === 'Enter' && handleFunkoSearch(1)}
              placeholder="Series (optional)"
              className="w-48 px-4 py-2 bg-zinc-800 border border-zinc-700 rounded-lg text-white"
            />
            <button
              onClick={() => handleFunkoSearch(1)}
              disabled={funkoLoading || (!funkoSearchQuery.trim() && !funkoSeriesFilter.trim())}
              className="px-6 py-2 bg-purple-500 text-white rounded-lg font-medium hover:bg-purple-600 disabled:opacity-50"
            >
              {funkoLoading ? <Loader2 className="w-5 h-5 animate-spin" /> : 'Search'}
            </button>
          </div>

          {/* Results */}
          {funkoResults.length > 0 && (
            <>
              <p className="text-sm text-zinc-500">
                Showing {funkoResults.length} of {funkoTotal.toLocaleString()} results
              </p>

              <div className="grid grid-cols-2 md:grid-cols-4 lg:grid-cols-5 gap-4">
                {funkoResults.map((funko) => (
                  <div
                    key={funko.id}
                    onClick={() => selectFunko(funko)}
                    className="bg-zinc-800 rounded-xl border border-zinc-700 overflow-hidden cursor-pointer hover:border-purple-500 transition-all group"
                  >
                    <div className="aspect-square bg-zinc-900 relative">
                      {funko.image_url ? (
                        <img src={funko.image_url} alt={funko.title} className="w-full h-full object-contain group-hover:scale-105 transition-transform" />
                      ) : (
                        <div className="w-full h-full flex items-center justify-center">
                          <Package className="w-12 h-12 text-zinc-700" />
                        </div>
                      )}
                      <div className="absolute inset-0 bg-black/70 opacity-0 group-hover:opacity-100 transition-opacity flex items-center justify-center">
                        <span className="px-3 py-1.5 bg-purple-500 text-white rounded-lg text-sm font-medium flex items-center gap-1">
                          <Plus className="w-4 h-4" />
                          Create
                        </span>
                      </div>
                    </div>
                    <div className="p-3">
                      <h4 className="text-white font-bold text-sm line-clamp-2 mb-1">{funko.title}</h4>
                      <div className="text-xs text-zinc-500 space-y-0.5">
                        {funko.box_number && <p><span className="text-purple-400">#{funko.box_number}</span></p>}
                        {funko.license && <p>{funko.license}</p>}
                      </div>
                    </div>
                  </div>
                ))}
              </div>

              {/* Pagination */}
              {funkoTotalPages > 1 && (
                <div className="flex items-center justify-center gap-4">
                  <button
                    onClick={() => handleFunkoSearch(funkoPage - 1)}
                    disabled={funkoPage <= 1 || funkoLoading}
                    className="p-2 bg-zinc-800 rounded-lg hover:bg-zinc-700 disabled:opacity-50"
                  >
                    <ChevronLeft className="w-5 h-5 text-zinc-400" />
                  </button>
                  <span className="text-zinc-400">Page {funkoPage} of {funkoTotalPages}</span>
                  <button
                    onClick={() => handleFunkoSearch(funkoPage + 1)}
                    disabled={funkoPage >= funkoTotalPages || funkoLoading}
                    className="p-2 bg-zinc-800 rounded-lg hover:bg-zinc-700 disabled:opacity-50"
                  >
                    <ChevronRight className="w-5 h-5 text-zinc-400" />
                  </button>
                </div>
              )}
            </>
          )}

          {funkoResults.length === 0 && !funkoLoading && (
            <div className="text-center py-12 text-zinc-500">
              <Package className="w-12 h-12 mx-auto mb-3 text-zinc-700" />
              <p>Search for Funko POPs by name or series</p>
            </div>
          )}
        </div>
      )}

      {/* BCW Supplies Search */}
      {searchMode === SEARCH_MODES.BCW_SUPPLIES && (
        <div className="space-y-4">
          <p className="text-sm text-zinc-500">
            Search BCW product catalog for storage and protection supplies
          </p>

          <div className="flex gap-3">
            <input
              type="text"
              value={bcwSearchQuery}
              onChange={(e) => setBcwSearchQuery(e.target.value)}
              onKeyPress={(e) => e.key === 'Enter' && handleBCWSearch(1)}
              placeholder="Search by product name or SKU..."
              className="flex-1 px-4 py-2 bg-zinc-800 border border-zinc-700 rounded-lg text-white"
            />
            <select
              value={bcwCategoryFilter}
              onChange={(e) => setBcwCategoryFilter(e.target.value)}
              className="w-48 px-4 py-2 bg-zinc-800 border border-zinc-700 rounded-lg text-white"
            >
              <option value="">All Categories</option>
              {bcwCategories.map((cat) => (
                <option key={cat.name} value={cat.name}>
                  {cat.name} ({cat.count})
                </option>
              ))}
            </select>
            <button
              onClick={() => handleBCWSearch(1)}
              disabled={bcwLoading || (!bcwSearchQuery.trim() && !bcwCategoryFilter)}
              className="px-6 py-2 bg-green-500 text-white rounded-lg font-medium hover:bg-green-600 disabled:opacity-50"
            >
              {bcwLoading ? <Loader2 className="w-5 h-5 animate-spin" /> : 'Search'}
            </button>
          </div>

          {/* Results */}
          {bcwResults.length > 0 && (
            <>
              <p className="text-sm text-zinc-500">
                Showing {bcwResults.length} of {bcwTotal.toLocaleString()} results
              </p>

              <div className="grid grid-cols-2 md:grid-cols-4 lg:grid-cols-5 gap-4">
                {bcwResults.map((product) => (
                  <div
                    key={product.mapping_id}
                    onClick={() => selectBCWProduct(product)}
                    className="bg-zinc-800 rounded-xl border border-zinc-700 overflow-hidden cursor-pointer hover:border-green-500 transition-all group"
                  >
                    <div className="aspect-square bg-zinc-900 relative">
                      {product.image_url ? (
                        <img src={product.image_url} alt={product.product_name} className="w-full h-full object-contain group-hover:scale-105 transition-transform" />
                      ) : (
                        <div className="w-full h-full flex items-center justify-center">
                          <Tag className="w-12 h-12 text-zinc-700" />
                        </div>
                      )}
                      {product.in_catalog && (
                        <div className="absolute top-2 right-2 px-2 py-0.5 bg-blue-500/90 text-white text-xs rounded-full">
                          In Catalog
                        </div>
                      )}
                      <div className="absolute inset-0 bg-black/70 opacity-0 group-hover:opacity-100 transition-opacity flex items-center justify-center">
                        <span className="px-3 py-1.5 bg-green-500 text-white rounded-lg text-sm font-medium flex items-center gap-1">
                          <Plus className="w-4 h-4" />
                          {product.in_catalog ? 'Edit' : 'Create'}
                        </span>
                      </div>
                    </div>
                    <div className="p-3">
                      <h4 className="text-white font-bold text-sm line-clamp-2 mb-1">{product.product_name}</h4>
                      <div className="text-xs text-zinc-500 space-y-0.5">
                        <p><span className="text-green-400">{product.mdm_sku}</span></p>
                        {product.bcw_category && <p>{product.bcw_category}</p>}
                        {product.pricing?.bcw_msrp && (
                          <p className="text-zinc-400">MSRP: ${product.pricing.bcw_msrp.toFixed(2)}</p>
                        )}
                      </div>
                    </div>
                  </div>
                ))}
              </div>

              {/* Pagination */}
              {bcwTotalPages > 1 && (
                <div className="flex items-center justify-center gap-4">
                  <button
                    onClick={() => handleBCWSearch(bcwPage - 1)}
                    disabled={bcwPage <= 1 || bcwLoading}
                    className="p-2 bg-zinc-800 rounded-lg hover:bg-zinc-700 disabled:opacity-50"
                  >
                    <ChevronLeft className="w-5 h-5 text-zinc-400" />
                  </button>
                  <span className="text-zinc-400">Page {bcwPage} of {bcwTotalPages}</span>
                  <button
                    onClick={() => handleBCWSearch(bcwPage + 1)}
                    disabled={bcwPage >= bcwTotalPages || bcwLoading}
                    className="p-2 bg-zinc-800 rounded-lg hover:bg-zinc-700 disabled:opacity-50"
                  >
                    <ChevronRight className="w-5 h-5 text-zinc-400" />
                  </button>
                </div>
              )}
            </>
          )}

          {bcwResults.length === 0 && !bcwLoading && (
            <div className="text-center py-12 text-zinc-500">
              <Tag className="w-12 h-12 mx-auto mb-3 text-zinc-700" />
              <p>Search BCW supplies catalog by name, SKU, or category</p>
            </div>
          )}
        </div>
      )}

      {/* Loading overlay */}
      {detailsLoading && (
        <div className="fixed inset-0 bg-black/50 flex items-center justify-center z-50">
          <Loader2 className="w-8 h-8 text-orange-500 animate-spin" />
        </div>
      )}
    </div>
  );
}
