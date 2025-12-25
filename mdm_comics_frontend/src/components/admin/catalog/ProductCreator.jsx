/**
 * ProductCreator - Search databases and create products
 * Consolidated search & create functionality for Catalog module
 */
import React, { useEffect, useReducer, useCallback } from 'react';
import {
  Search, Plus, Save, Package, Loader2, Upload, Camera, ScanLine,
  Tag, ChevronLeft, ChevronRight, X, BookOpen, Box
} from 'lucide-react';
import { comicsAPI, funkosAPI } from '../../../services/api';
import { adminAPI } from '../../../services/adminApi';
import { initialState, reducer } from './productCreatorReducer';

const SEARCH_MODES = {
  COMICS: 'comics',
  FUNKOS: 'funkos',
  BCW_SUPPLIES: 'bcw_supplies',
};

export default function ProductCreator() {
  const [state, dispatch] = useReducer(reducer, initialState);
  const { searchMode, showCreateForm, saving, message, detailsLoading, comics, funkos, bcw, form } = state;

  // Load stats on mount
  useEffect(() => {
    funkosAPI.getStats()
      .then(stats => dispatch({ type: 'SET_FUNKO_STATS', payload: stats }))
      .catch(console.error);
  }, []);

  // Load BCW categories
  useEffect(() => {
    if (searchMode === SEARCH_MODES.BCW_SUPPLIES && bcw.categories.length === 0) {
      adminAPI.getBCWCategories()
        .then(data => dispatch({ type: 'SET_BCW_CATEGORIES', payload: data.categories || [] }))
        .catch(console.error);
    }
  }, [searchMode, bcw.categories.length]);

  // Handlers
  const handleComicSearch = async (e) => {
    e?.preventDefault();
    if (!Object.values(comics.params).some(val => val && val.trim())) {
      dispatch({ type: 'SET_MESSAGE', payload: { type: 'warning', text: 'Enter search criteria' } });
      return;
    }

    dispatch({ type: 'SET_COMIC_LOADING', payload: true });
    dispatch({ type: 'SET_MESSAGE', payload: null });

    try {
      const result = await comicsAPI.search({ ...comics.params, page: 1 });
      dispatch({ type: 'SET_COMIC_RESULTS', payload: result.results || [] });
      if (result.results?.length === 0) {
        dispatch({ type: 'SET_MESSAGE', payload: { type: 'info', text: 'No results found' } });
      }
    } catch (err) {
      dispatch({ type: 'SET_MESSAGE', payload: { type: 'error', text: err.message } });
      dispatch({ type: 'SET_COMIC_LOADING', payload: false });
    }
  };

  const handleFunkoSearch = async (pageNum = 1) => {
    dispatch({ type: 'UPDATE_FUNKO_SEARCH', payload: { loading: true } });
    try {
      const data = await funkosAPI.search({
        q: funkos.query || undefined,
        series: funkos.seriesFilter || undefined,
        page: pageNum,
        per_page: 20,
      });
      dispatch({ 
        type: 'SET_FUNKO_RESULTS', 
        payload: { results: data.results, page: pageNum, total: data.total, totalPages: data.pages } 
      });
    } catch (err) {
      dispatch({ type: 'SET_MESSAGE', payload: { type: 'error', text: err.message } });
      dispatch({ type: 'UPDATE_FUNKO_SEARCH', payload: { loading: false } });
    }
  };

  const handleBCWSearch = async (pageNum = 1) => {
    dispatch({ type: 'UPDATE_BCW_SEARCH', payload: { loading: true } });
    try {
      const data = await adminAPI.searchBCWCatalog({
        q: bcw.query || undefined,
        category: bcw.categoryFilter || undefined,
        page: pageNum,
        per_page: 20,
      });
      dispatch({ 
        type: 'SET_BCW_RESULTS', 
        payload: { results: data.results, page: pageNum, total: data.total, totalPages: data.pages } 
      });
    } catch (err) {
      dispatch({ type: 'SET_MESSAGE', payload: { type: 'error', text: err.message } });
      dispatch({ type: 'UPDATE_BCW_SEARCH', payload: { loading: false } });
    }
  };

  // Selection Handlers
  const selectComic = async (comic) => {
    dispatch({ type: 'SET_DETAILS_LOADING', payload: true });
    try {
      const details = await comicsAPI.getIssue(comic.id);
      dispatch({ 
        type: 'UPDATE_FORM', 
        payload: {
          sku: 'COMIC-' + comic.id,
          name: `${details.series?.name || ''} #${details.number || ''}`.trim(),
          description: details.desc || '',
          category: 'comics',
          subcategory: details.series?.publisher?.name || '',
          price: '',
          original_price: details.price,
          stock: 1,
          image_url: details.image || '',
          issue_number: details.number,
          publisher: details.series?.publisher?.name,
          year: details.cover_date ? new Date(details.cover_date).getFullYear() : '',
          upc: details.upc || '',
        }
      });
      dispatch({ type: 'TOGGLE_CREATE_FORM', payload: true });
    } catch (err) {
      dispatch({ type: 'SET_MESSAGE', payload: { type: 'error', text: err.message } });
    } finally {
      dispatch({ type: 'SET_DETAILS_LOADING', payload: false });
    }
  };

  const selectBCWProduct = (product) => {
    dispatch({
      type: 'UPDATE_FORM',
      payload: {
        sku: product.mdm_sku,
        name: product.product_name,
        description: `BCW ${product.product_name}. Professional-grade comic book storage.`,
        category: 'supplies',
        subcategory: product.bcw_category || '',
        price: product.pricing?.our_price || product.pricing?.bcw_msrp || '',
        original_price: product.pricing?.bcw_msrp || '',
        stock: 0,
        image_url: product.image_url || '',
        publisher: 'BCW',
        tags: ['bcw', 'supplies'],
        // Case Intelligence from new DB columns
        case_quantity: product.case_quantity || '',
        case_weight: product.weight || '',
        case_dimensions: product.dimensions || '',
        upc: product.upc || ''
      }
    });
    dispatch({ type: 'TOGGLE_CREATE_FORM', payload: true });
  };

  const handleCreateProduct = async (e) => {
    e.preventDefault();
    if (!form.price || !form.name || !form.sku) {
      dispatch({ type: 'SET_MESSAGE', payload: { type: 'error', text: 'Required fields missing' } });
      return;
    }

    dispatch({ type: 'SET_SAVING', payload: true });
    try {
      // Logic split for BCW vs Regular
      if (form.sku.startsWith('BCW') || form.tags.includes('bcw')) {
        await adminAPI.activateBCWProduct(form.sku, parseFloat(form.price));
      } else {
        await adminAPI.createProduct(null, { ...form, price: parseFloat(form.price) });
      }
      
      dispatch({ type: 'SET_MESSAGE', payload: { type: 'success', text: 'Product created!' } });
      dispatch({ type: 'RESET_FORM' });
    } catch (err) {
      dispatch({ type: 'SET_MESSAGE', payload: { type: 'error', text: err.message } });
    } finally {
      dispatch({ type: 'SET_SAVING', payload: false });
    }
  };

  return (
    <div className="space-y-4">
      {/* Messages */}
      {message && (
        <div className={`p-3 rounded-lg text-sm flex justify-between ${
          message.type === 'error' ? 'bg-red-500/10 text-red-400' : 'bg-blue-500/10 text-blue-400'
        }`}>
          {message.text}
          <button onClick={() => dispatch({ type: 'SET_MESSAGE', payload: null })}><X className="w-4 h-4"/></button>
        </div>
      )}

      {/* Modal Form */}
      {showCreateForm && (
        <div className="fixed inset-0 z-50 flex items-center justify-center">
          <div className="absolute inset-0 bg-black/60" onClick={() => dispatch({ type: 'TOGGLE_CREATE_FORM', payload: false })} />
          <div className="relative bg-zinc-900 border border-zinc-800 rounded-xl w-full max-w-4xl max-h-[90vh] overflow-auto">
            <div className="sticky top-0 bg-zinc-900 border-b border-zinc-800 p-4 flex justify-between">
              <h3 className="text-lg font-semibold text-white">Create Product</h3>
              <button onClick={() => dispatch({ type: 'TOGGLE_CREATE_FORM', payload: false })}><X className="w-5 h-5 text-zinc-400"/></button>
            </div>

            <form onSubmit={handleCreateProduct} className="p-6 grid md:grid-cols-2 gap-6">
              <div className="space-y-4">
                <div className="grid grid-cols-2 gap-3">
                  <div>
                    <label className="block text-sm text-zinc-400 mb-1">SKU</label>
                    <input type="text" value={form.sku} onChange={e => dispatch({ type: 'UPDATE_FORM', payload: { sku: e.target.value } })} className="w-full px-3 py-2 bg-zinc-800 border-zinc-700 rounded-lg text-white" />
                  </div>
                  <div>
                    <label className="block text-sm text-zinc-400 mb-1">Category</label>
                    <select value={form.category} onChange={e => dispatch({ type: 'UPDATE_FORM', payload: { category: e.target.value } })} className="w-full px-3 py-2 bg-zinc-800 border-zinc-700 rounded-lg text-white">
                      <option value="comics">Comics</option>
                      <option value="supplies">Supplies</option>
                      <option value="funko">Funko</option>
                    </select>
                  </div>
                </div>

                {/* Case Intelligence Section */}
                {(form.category === 'supplies' || form.sku.startsWith('BCW')) && (
                  <div className="p-3 bg-blue-500/10 border border-blue-500/30 rounded-lg space-y-2">
                    <h4 className="text-sm font-semibold text-blue-400 flex items-center gap-2">
                      <Box className="w-4 h-4" /> Case Intelligence
                    </h4>
                    <div className="grid grid-cols-3 gap-2">
                      <div>
                        <label className="text-xs text-zinc-400">Qty/Case</label>
                        <input type="number" value={form.case_quantity} disabled className="w-full px-2 py-1 bg-zinc-900 border-zinc-700 rounded text-zinc-300 text-sm" />
                      </div>
                      <div>
                        <label className="text-xs text-zinc-400">Weight (lbs)</label>
                        <input type="text" value={form.case_weight} disabled className="w-full px-2 py-1 bg-zinc-900 border-zinc-700 rounded text-zinc-300 text-sm" />
                      </div>
                      <div>
                        <label className="text-xs text-zinc-400">Dims</label>
                        <input type="text" value={form.case_dimensions} disabled className="w-full px-2 py-1 bg-zinc-900 border-zinc-700 rounded text-zinc-300 text-sm" />
                      </div>
                    </div>
                  </div>
                )}

                {/* Rest of form fields... (Name, Desc, Price) */}
                <div>
                  <label className="block text-sm text-zinc-400 mb-1">Name</label>
                  <input type="text" value={form.name} onChange={e => dispatch({ type: 'UPDATE_FORM', payload: { name: e.target.value } })} className="w-full px-3 py-2 bg-zinc-800 border-zinc-700 rounded-lg text-white" />
                </div>
                
                <div className="grid grid-cols-2 gap-3">
                  <div>
                    <label className="block text-sm text-zinc-400 mb-1">Price</label>
                    <input type="number" step="0.01" value={form.price} onChange={e => dispatch({ type: 'UPDATE_FORM', payload: { price: e.target.value } })} className="w-full px-3 py-2 bg-zinc-800 border-zinc-700 rounded-lg text-white" />
                  </div>
                  <div>
                    <label className="block text-sm text-zinc-400 mb-1">Stock</label>
                    <input type="number" value={form.stock} onChange={e => dispatch({ type: 'UPDATE_FORM', payload: { stock: e.target.value } })} className="w-full px-3 py-2 bg-zinc-800 border-zinc-700 rounded-lg text-white" />
                  </div>
                </div>

                <button type="submit" disabled={saving} className="w-full py-3 bg-orange-500 text-white rounded-lg font-bold hover:bg-orange-600 disabled:opacity-50 flex items-center justify-center gap-2">
                  {saving ? <Loader2 className="w-5 h-5 animate-spin" /> : <><Save className="w-5 h-5" /> Save Product</>}
                </button>
              </div>

              {/* Preview Side */}
              <div className="bg-zinc-800 rounded-xl p-4 h-fit">
                <div className="aspect-square bg-zinc-900 rounded-lg overflow-hidden mb-4">
                  {form.image_url ? (
                    <img src={form.image_url} alt="Preview" className="w-full h-full object-contain" />
                  ) : (
                    <div className="w-full h-full flex items-center justify-center text-zinc-600"><Package className="w-12 h-12"/></div>
                  )}
                </div>
                <h3 className="font-bold text-white text-lg mb-1">{form.name || 'Product Name'}</h3>
                <p className="text-orange-500 font-mono text-sm mb-4">{form.sku}</p>
                <div className="flex justify-between items-end">
                  <span className="text-2xl font-bold text-white">${form.price || '0.00'}</span>
                  {form.case_quantity && (
                    <span className="text-xs px-2 py-1 bg-blue-500/20 text-blue-400 rounded">
                      Case of {form.case_quantity}
                    </span>
                  )}
                </div>
              </div>
            </form>
          </div>
        </div>
      )}

      {/* Mode Switcher */}
      <div className="flex gap-2 mb-4">
        {Object.values(SEARCH_MODES).map(mode => (
          <button
            key={mode}
            onClick={() => dispatch({ type: 'SET_SEARCH_MODE', payload: mode })}
            className={`px-4 py-2 rounded-lg capitalize ${searchMode === mode ? 'bg-orange-500 text-white' : 'bg-zinc-800 text-zinc-400'}`}
          >
            {mode.replace('_', ' ')}
          </button>
        ))}
        <button onClick={() => dispatch({ type: 'CLEAR_ALL' })} className="ml-auto px-3 text-sm text-zinc-500 hover:text-white">Reset</button>
      </div>

      {/* Search Interfaces */}
      {searchMode === SEARCH_MODES.COMICS && (
        <div className="space-y-4">
          <div className="flex gap-2">
            <input 
              placeholder="UPC / Barcode" 
              value={comics.params.upc}
              onChange={e => dispatch({ type: 'UPDATE_COMIC_PARAMS', payload: { upc: e.target.value } })}
              className="flex-1 px-4 py-2 bg-zinc-800 border-zinc-700 rounded-lg text-white"
            />
            <button onClick={handleComicSearch} disabled={searchLoading} className="px-6 bg-orange-500 text-white rounded-lg">
              {searchLoading ? <Loader2 className="w-4 h-4 animate-spin"/> : 'Search'}
            </button>
          </div>
          {/* Results Grid */}
          <div className="grid grid-cols-2 md:grid-cols-5 gap-4">
            {comics.results.map(comic => (
              <div key={comic.id} onClick={() => selectComic(comic)} className="bg-zinc-800 p-2 rounded-lg cursor-pointer hover:ring-2 ring-orange-500">
                <img src={comic.image || '/assets/no-cover.png'} className="w-full aspect-[2/3] object-cover rounded mb-2"/>
                <p className="text-sm font-medium text-white truncate">{comic.series?.name}</p>
                <p className="text-xs text-zinc-500">#{comic.number}</p>
              </div>
            ))}
          </div>
        </div>
      )}

      {searchMode === SEARCH_MODES.BCW_SUPPLIES && (
        <div className="space-y-4">
          <div className="flex gap-2">
            <input 
              placeholder="Search BCW Catalog..." 
              value={bcw.query}
              onChange={e => dispatch({ type: 'UPDATE_BCW_SEARCH', payload: { query: e.target.value } })}
              className="flex-1 px-4 py-2 bg-zinc-800 border-zinc-700 rounded-lg text-white"
            />
            <button onClick={() => handleBCWSearch(1)} disabled={bcw.loading} className="px-6 bg-green-500 text-white rounded-lg">
              {bcw.loading ? <Loader2 className="w-4 h-4 animate-spin"/> : 'Search'}
            </button>
          </div>
          <div className="grid grid-cols-2 md:grid-cols-5 gap-4">
            {bcw.results.map(prod => (
              <div key={prod.mapping_id} onClick={() => selectBCWProduct(prod)} className="bg-zinc-800 p-2 rounded-lg cursor-pointer hover:ring-2 ring-green-500 relative">
                {prod.in_catalog && <span className="absolute top-2 right-2 bg-blue-500 text-white text-[10px] px-2 py-0.5 rounded">Active</span>}
                <img src={prod.image_url} className="w-full aspect-square object-contain bg-zinc-900 rounded mb-2"/>
                <p className="text-sm font-medium text-white line-clamp-2">{prod.product_name}</p>
                <p className="text-xs text-green-400 font-mono mt-1">{prod.mdm_sku}</p>
              </div>
            ))}
          </div>
        </div>
      )}
    </div>
  );
}
