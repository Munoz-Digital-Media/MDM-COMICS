/**
 * ProductEditModal - Full product editor for all fields
 * Admin Console v1.5.0 - Added image upload/remove/gallery management
 */
import React, { useState, useEffect, useRef } from 'react';
import { X, Save, Loader2, Image as ImageIcon, AlertTriangle, Upload, Trash2, GripVertical, Plus, ArrowDown, Star } from 'lucide-react';
import { adminAPI } from '../../../services/adminApi';

const FEATURED_LIMIT = 5; // Max featured products per category section on homepage

// Fraction options for dimension inputs (in eighths)
const FRACTIONS = [
  { value: 0, label: '0', display: '' },
  { value: 0.125, label: '⅛', display: '⅛' },
  { value: 0.25, label: '¼', display: '¼' },
  { value: 0.375, label: '⅜', display: '⅜' },
  { value: 0.5, label: '½', display: '½' },
  { value: 0.625, label: '⅝', display: '⅝' },
  { value: 0.75, label: '¾', display: '¾' },
  { value: 0.875, label: '⅞', display: '⅞' },
];

// Convert decimal to whole + fraction parts
const decimalToFraction = (decimal) => {
  if (!decimal && decimal !== 0) return { whole: '', fraction: 0 };
  const num = parseFloat(decimal);
  const whole = Math.floor(num);
  const frac = num - whole;
  // Find closest fraction
  const closest = FRACTIONS.reduce((prev, curr) =>
    Math.abs(curr.value - frac) < Math.abs(prev.value - frac) ? curr : prev
  );
  return { whole: whole || '', fraction: closest.value };
};

// Convert whole + fraction to decimal
const fractionToDecimal = (whole, fraction) => {
  const w = parseInt(whole) || 0;
  const f = parseFloat(fraction) || 0;
  return w + f || '';
};

// Fractional dimension input component
const FractionInput = ({ value, onChange, placeholder }) => {
  const { whole, fraction } = decimalToFraction(value);

  const handleWholeChange = (e) => {
    const newWhole = e.target.value.replace(/[^0-9]/g, '');
    onChange(fractionToDecimal(newWhole, fraction));
  };

  const handleFractionChange = (e) => {
    onChange(fractionToDecimal(whole, e.target.value));
  };

  return (
    <div className="flex gap-1">
      <input
        type="text"
        inputMode="numeric"
        value={whole}
        onChange={handleWholeChange}
        placeholder={placeholder}
        className="w-12 px-2 py-1.5 bg-zinc-800 border border-zinc-700 rounded text-white text-sm text-center focus:border-orange-500 focus:outline-none"
      />
      <select
        value={fraction}
        onChange={handleFractionChange}
        className="w-14 px-1 py-1.5 bg-zinc-800 border border-zinc-700 rounded text-white text-sm focus:border-orange-500 focus:outline-none"
      >
        {FRACTIONS.map(f => (
          <option key={f.value} value={f.value}>{f.label}</option>
        ))}
      </select>
    </div>
  );
};

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
    images: [],
    tags: [],
    featured: false,
    upc: '',
    isbn: '',
    issue_number: '',
    publisher: '',
    year: '',
    // Dimensions for supplies
    interior_width: '',
    interior_height: '',
    interior_length: '',
    exterior_width: '',
    exterior_height: '',
    exterior_length: '',
    // Physical properties
    weight: '',
    material: '',
    // Grading fields
    grading_company: '',
    certification_number: '',
    cgc_grade: '',
    grade_label: '',
    is_graded: false,
  });
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);
  const [tagInput, setTagInput] = useState('');
  const [featuredCount, setFeaturedCount] = useState(0);
  const [featuredLimitReached, setFeaturedLimitReached] = useState(false);

  // Image management state
  const [uploadingPrimary, setUploadingPrimary] = useState(false);
  const [uploadingGallery, setUploadingGallery] = useState(false);
  const [draggedIndex, setDraggedIndex] = useState(null);
  const [dragOverIndex, setDragOverIndex] = useState(null);

  const primaryFileRef = useRef(null);
  const galleryFileRef = useRef(null);

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
        images: product.images || [],
        tags: product.tags || [],
        featured: product.featured || false,
        upc: product.upc || '',
        isbn: product.isbn || '',
        issue_number: product.issue_number || '',
        publisher: product.publisher || '',
        year: product.year?.toString() || '',
        interior_width: product.interior_width?.toString() || '',
        interior_height: product.interior_height?.toString() || '',
        interior_length: product.interior_length?.toString() || '',
        exterior_width: product.exterior_width?.toString() || '',
        exterior_height: product.exterior_height?.toString() || '',
        exterior_length: product.exterior_length?.toString() || '',
        weight: product.weight || '',
        material: product.material || '',
        // Grading fields
        grading_company: product.grading_company || '',
        certification_number: product.certification_number || '',
        cgc_grade: product.cgc_grade?.toString() || '',
        grade_label: product.grade_label || '',
        is_graded: product.is_graded || false,
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
  // Primary image upload
  const handlePrimaryUpload = async (e) => {
    const file = e.target.files?.[0];
    if (!file) return;
    setUploadingPrimary(true);
    setError(null);
    try {
      const result = await adminAPI.uploadProductImage(product.id, file);
      setForm({ ...form, image_url: result.image_url });
    } catch (err) {
      setError('Failed to upload image: ' + err.message);
    } finally {
      setUploadingPrimary(false);
      if (primaryFileRef.current) primaryFileRef.current.value = '';
    }
  };

  // Remove primary image
  const handleRemovePrimary = async () => {
    if (!window.confirm('Remove primary image from this product?')) return;
    setUploadingPrimary(true);
    setError(null);
    try {
      await adminAPI.removeProductImage(product.id);
      setForm({ ...form, image_url: '' });
    } catch (err) {
      setError('Failed to remove image: ' + err.message);
    } finally {
      setUploadingPrimary(false);
    }
  };

  // Gallery image upload
  const handleGalleryUpload = async (e) => {
    const file = e.target.files?.[0];
    if (!file) return;
    setUploadingGallery(true);
    setError(null);
    try {
      const result = await adminAPI.uploadGalleryImage(product.id, file);
      setForm({ ...form, images: [...form.images, result.image_url] });
    } catch (err) {
      setError('Failed to upload gallery image: ' + err.message);
    } finally {
      setUploadingGallery(false);
      if (galleryFileRef.current) galleryFileRef.current.value = '';
    }
  };

  // Remove gallery image
  const handleRemoveGallery = async (index) => {
    if (!window.confirm('Remove this image from the gallery?')) return;
    setError(null);
    try {
      await adminAPI.removeGalleryImage(product.id, index);
      const newImages = [...form.images];
      newImages.splice(index, 1);
      setForm({ ...form, images: newImages });
    } catch (err) {
      setError('Failed to remove gallery image: ' + err.message);
    }
  };

  // Drag and drop for gallery reordering
  const handleDragStart = (index) => setDraggedIndex(index);
  const handleDragOver = (e, index) => { e.preventDefault(); setDragOverIndex(index); };
  const handleDragEnd = async () => {
    if (draggedIndex === null || dragOverIndex === null || draggedIndex === dragOverIndex) {
      setDraggedIndex(null);
      setDragOverIndex(null);
      return;
    }
    // Save current state in case we need to revert
    const previousImages = [...form.images];
    const newOrder = form.images.map((_, i) => i);
    const [removed] = newOrder.splice(draggedIndex, 1);
    newOrder.splice(dragOverIndex, 0, removed);
    const newImages = newOrder.map(i => form.images[i]);
    setForm({ ...form, images: newImages });
    try {
      await adminAPI.reorderGalleryImages(product.id, newOrder);
    } catch (err) {
      setError('Failed to reorder gallery: ' + err.message);
      // Revert to previous state (not original product.images which may be stale)
      setForm({ ...form, images: previousImages });
    }
    setDraggedIndex(null);
    setDragOverIndex(null);
  };

  // Demote primary image to gallery
  const handleDemotePrimary = async () => {
    if (!form.image_url) return;
    if (!window.confirm('Move primary image to gallery?')) return;
    setUploadingPrimary(true);
    setError(null);
    try {
      const result = await adminAPI.demotePrimaryImage(product.id);
      setForm({ ...form, image_url: '', images: [result.demoted_url, ...form.images] });
    } catch (err) {
      setError('Failed to demote image: ' + err.message);
    } finally {
      setUploadingPrimary(false);
    }
  };

  // Promote gallery image to primary
  const handlePromoteGallery = async (index) => {
    if (!window.confirm('Set this image as the primary image?')) return;
    setError(null);
    try {
      const result = await adminAPI.promoteGalleryImage(product.id, index);
      // Swap: gallery image becomes primary, old primary goes to gallery at same position
      const newImages = [...form.images];
      newImages.splice(index, 1);
      if (result.previous_primary) {
        newImages.splice(index, 0, result.previous_primary);
      }
      setForm({ ...form, image_url: result.promoted_url, images: newImages });
    } catch (err) {
      setError('Failed to promote image: ' + err.message);
    }
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
        interior_width: form.interior_width ? parseFloat(form.interior_width) : null,
        interior_height: form.interior_height ? parseFloat(form.interior_height) : null,
        interior_length: form.interior_length ? parseFloat(form.interior_length) : null,
        exterior_width: form.exterior_width ? parseFloat(form.exterior_width) : null,
        exterior_height: form.exterior_height ? parseFloat(form.exterior_height) : null,
        exterior_length: form.exterior_length ? parseFloat(form.exterior_length) : null,
        weight: form.weight || null,
        material: form.material || null,
        // Grading fields - only include if category is graded
        grading_company: form.category === 'graded' ? (form.grading_company || null) : null,
        certification_number: form.category === 'graded' ? (form.certification_number || null) : null,
        cgc_grade: form.category === 'graded' && form.cgc_grade ? parseFloat(form.cgc_grade) : null,
        grade_label: form.category === 'graded' ? (form.grade_label || null) : null,
        is_graded: form.category === 'graded',
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
                    onChange={(e) => setForm({ ...form, category: e.target.value, is_graded: e.target.value === 'graded' })}
                    className="w-full px-3 py-2 bg-zinc-800 border border-zinc-700 rounded-lg text-white focus:border-orange-500 focus:outline-none"
                  >
                    <option value="bagged-boarded">Bagged & Boarded</option>
                    <option value="graded">Graded</option>
                    <option value="funko">Funko</option>
                    <option value="supplies">Supplies</option>
                  </select>
                </div>
              </div>

              {/* Grading Section - only shown when category is "graded" */}
              {form.category === 'graded' && (
                <div className="p-4 bg-yellow-500/10 border border-yellow-500/30 rounded-lg space-y-3">
                  <h4 className="text-sm font-semibold text-yellow-400 flex items-center gap-2">
                    🏆 Grading Information
                  </h4>
                  <div className="grid grid-cols-2 gap-3">
                    <div>
                      <label className="block text-sm text-zinc-400 mb-1">Grading Company</label>
                      <select
                        value={form.grading_company}
                        onChange={(e) => setForm({ ...form, grading_company: e.target.value })}
                        className="w-full px-3 py-2 bg-zinc-800 border border-zinc-700 rounded-lg text-white focus:border-orange-500 focus:outline-none"
                      >
                        <option value="">Select...</option>
                        <option value="cgc">CGC</option>
                        {/* Future: CBCS, PGX */}
                      </select>
                    </div>
                    <div>
                      <label className="block text-sm text-zinc-400 mb-1">Certification #</label>
                      <input
                        type="text"
                        value={form.certification_number}
                        onChange={(e) => setForm({ ...form, certification_number: e.target.value })}
                        placeholder="e.g., 4375892006"
                        className="w-full px-3 py-2 bg-zinc-800 border border-zinc-700 rounded-lg text-white font-mono focus:border-orange-500 focus:outline-none"
                      />
                    </div>
                  </div>
                  <div className="grid grid-cols-2 gap-3">
                    <div>
                      <label className="block text-sm text-zinc-400 mb-1">Grade</label>
                      <input
                        type="number"
                        step="0.1"
                        min="0.5"
                        max="10.0"
                        value={form.cgc_grade}
                        onChange={(e) => setForm({ ...form, cgc_grade: e.target.value })}
                        placeholder="9.8"
                        className="w-full px-3 py-2 bg-zinc-800 border border-zinc-700 rounded-lg text-white focus:border-orange-500 focus:outline-none"
                      />
                    </div>
                    <div>
                      <label className="block text-sm text-zinc-400 mb-1">Label Type</label>
                      <select
                        value={form.grade_label}
                        onChange={(e) => setForm({ ...form, grade_label: e.target.value })}
                        className="w-full px-3 py-2 bg-zinc-800 border border-zinc-700 rounded-lg text-white focus:border-orange-500 focus:outline-none"
                      >
                        <option value="">Select...</option>
                        <option value="universal">Universal (Blue)</option>
                        <option value="signature">Signature Series (Yellow)</option>
                        <option value="qualified">Qualified (Green)</option>
                        <option value="restored">Restored (Purple)</option>
                        <option value="conserved">Conserved (Purple)</option>
                      </select>
                    </div>
                  </div>
                </div>
              )}

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

              {/* Dimensions - for supplies and supply-related categories */}
              {(form.category === 'supplies' ||
                form.category?.toLowerCase().includes('bag') ||
                form.category?.toLowerCase().includes('board') ||
                form.category?.toLowerCase().includes('bin') ||
                form.category?.toLowerCase().includes('divider') ||
                form.category?.toLowerCase().includes('case') ||
                form.category?.toLowerCase().includes('box') ||
                form.category?.toLowerCase().includes('sleeve') ||
                form.category?.toLowerCase().includes('toploader') ||
                form.category?.toLowerCase().includes('partition')) && (
                <div className="space-y-3 p-3 bg-zinc-800/50 rounded-lg border border-zinc-700">
                  <p className="text-sm font-medium text-zinc-300">Dimensions (inches)</p>

                  {/* Interior Dimensions */}
                  <div>
                    <label className="block text-xs text-zinc-500 mb-1">Interior (W × H × L)</label>
                    <div className="flex items-center gap-2">
                      <FractionInput
                        value={form.interior_width}
                        onChange={(v) => setForm({ ...form, interior_width: v })}
                        placeholder="W"
                      />
                      <span className="text-zinc-500">×</span>
                      <FractionInput
                        value={form.interior_height}
                        onChange={(v) => setForm({ ...form, interior_height: v })}
                        placeholder="H"
                      />
                      <span className="text-zinc-500">×</span>
                      <FractionInput
                        value={form.interior_length}
                        onChange={(v) => setForm({ ...form, interior_length: v })}
                        placeholder="L"
                      />
                    </div>
                  </div>

                  {/* Exterior Dimensions */}
                  <div>
                    <label className="block text-xs text-zinc-500 mb-1">Exterior (W × H × L)</label>
                    <div className="flex items-center gap-2">
                      <FractionInput
                        value={form.exterior_width}
                        onChange={(v) => setForm({ ...form, exterior_width: v })}
                        placeholder="W"
                      />
                      <span className="text-zinc-500">×</span>
                      <FractionInput
                        value={form.exterior_height}
                        onChange={(v) => setForm({ ...form, exterior_height: v })}
                        placeholder="H"
                      />
                      <span className="text-zinc-500">×</span>
                      <FractionInput
                        value={form.exterior_length}
                        onChange={(v) => setForm({ ...form, exterior_length: v })}
                        placeholder="L"
                      />
                    </div>
                  </div>

                  {/* Weight & Material */}
                  <div className="grid grid-cols-2 gap-3 pt-2 border-t border-zinc-700">
                    <div>
                      <label className="block text-xs text-zinc-500 mb-1">Weight</label>
                      <input
                        type="text"
                        value={form.weight}
                        onChange={(e) => setForm({ ...form, weight: e.target.value })}
                        placeholder="e.g., 2.5 lbs"
                        className="w-full px-2 py-1.5 bg-zinc-800 border border-zinc-700 rounded text-white text-sm focus:border-orange-500 focus:outline-none"
                      />
                    </div>
                    <div>
                      <label className="block text-xs text-zinc-500 mb-1">Material</label>
                      <input
                        type="text"
                        value={form.material}
                        onChange={(e) => setForm({ ...form, material: e.target.value })}
                        placeholder="e.g., Polypropylene"
                        className="w-full px-2 py-1.5 bg-zinc-800 border border-zinc-700 rounded text-white text-sm focus:border-orange-500 focus:outline-none"
                      />
                    </div>
                  </div>
                </div>
              )}

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

            {/* Right Column - Images & Preview */}
            <div className="space-y-4">
              {/* Primary Image Section */}
              <div className="bg-zinc-800 rounded-xl p-4">
                <h4 className="font-medium text-zinc-300 mb-3 flex items-center gap-2">
                  <ImageIcon className="w-4 h-4" />
                  Primary Image
                </h4>
                <div className="flex gap-4">
                  <div className="w-32 h-40 bg-zinc-900 rounded-lg overflow-hidden flex-shrink-0">
                    {form.image_url ? (
                      <img src={form.image_url} alt="Primary" className="w-full h-full object-contain" onError={(e) => { e.target.style.display = 'none'; }} />
                    ) : (
                      <div className="w-full h-full flex items-center justify-center"><ImageIcon className="w-8 h-8 text-zinc-700" /></div>
                    )}
                  </div>
                  <div className="flex-1 space-y-3">
                    <div>
                      <label className="block text-xs text-zinc-500 mb-1">Image URL</label>
                      <input type="url" value={form.image_url} onChange={(e) => setForm({ ...form, image_url: e.target.value })} placeholder="https://..." className="w-full px-3 py-1.5 bg-zinc-900 border border-zinc-700 rounded text-white text-sm placeholder-zinc-600 focus:border-orange-500 focus:outline-none" />
                    </div>
                    <div className="flex gap-2">
                      <input ref={primaryFileRef} type="file" accept="image/png,image/jpeg,image/gif,image/webp" onChange={handlePrimaryUpload} className="hidden" />
                      <button type="button" onClick={() => primaryFileRef.current?.click()} disabled={uploadingPrimary} className="flex-1 flex items-center justify-center gap-2 px-3 py-2 bg-zinc-700 text-zinc-300 rounded hover:bg-zinc-600 disabled:opacity-50 text-sm">
                        {uploadingPrimary ? <Loader2 className="w-4 h-4 animate-spin" /> : <Upload className="w-4 h-4" />} Upload
                      </button>
                      {form.image_url && (
                        <>
                          <button type="button" onClick={handleDemotePrimary} disabled={uploadingPrimary} className="flex items-center justify-center gap-2 px-3 py-2 bg-zinc-600 text-zinc-300 rounded hover:bg-zinc-500 disabled:opacity-50 text-sm" title="Move to gallery">
                            <ArrowDown className="w-4 h-4" />
                          </button>
                          <button type="button" onClick={handleRemovePrimary} disabled={uploadingPrimary} className="flex items-center justify-center gap-2 px-3 py-2 bg-red-500/20 text-red-400 rounded hover:bg-red-500/30 disabled:opacity-50 text-sm" title="Remove image">
                            <Trash2 className="w-4 h-4" />
                          </button>
                        </>
                      )}
                    </div>
                  </div>
                </div>
              </div>

              {/* Gallery Section */}
              <div className="bg-zinc-800 rounded-xl p-4">
                <h4 className="font-medium text-zinc-300 mb-3 flex items-center gap-2">
                  <ImageIcon className="w-4 h-4" />Gallery Images <span className="text-xs text-zinc-500">({form.images.length})</span>
                </h4>
                <div className="grid grid-cols-4 gap-2 mb-3">
                  {form.images.map((url, index) => (
                    <div key={index} draggable onDragStart={() => handleDragStart(index)} onDragOver={(e) => handleDragOver(e, index)} onDragEnd={handleDragEnd}
                      className={`relative group aspect-square bg-zinc-900 rounded overflow-hidden cursor-grab ${draggedIndex === index ? 'opacity-50' : ''} ${dragOverIndex === index ? 'ring-2 ring-orange-500' : ''}`}>
                      <img src={url} alt={`Gallery ${index + 1}`} className="w-full h-full object-contain" onError={(e) => { e.target.src = ''; }} />
                      <div className="absolute top-1 left-1 p-1 bg-black/50 rounded opacity-0 group-hover:opacity-100 transition-opacity"><GripVertical className="w-3 h-3 text-white" /></div>
                      <button type="button" onClick={() => handlePromoteGallery(index)} className="absolute bottom-1 left-1 p-1 bg-orange-500/80 rounded opacity-0 group-hover:opacity-100 transition-opacity hover:bg-orange-500" title="Set as primary"><Star className="w-3 h-3 text-white" /></button>
                      <button type="button" onClick={() => handleRemoveGallery(index)} className="absolute top-1 right-1 p-1 bg-red-500/80 rounded opacity-0 group-hover:opacity-100 transition-opacity hover:bg-red-500" title="Remove"><X className="w-3 h-3 text-white" /></button>
                    </div>
                  ))}
                  <input ref={galleryFileRef} type="file" accept="image/png,image/jpeg,image/gif,image/webp" onChange={handleGalleryUpload} className="hidden" />
                  <button type="button" onClick={() => galleryFileRef.current?.click()} disabled={uploadingGallery} className="aspect-square bg-zinc-900 rounded border-2 border-dashed border-zinc-700 flex items-center justify-center hover:border-orange-500 hover:bg-zinc-800 transition-colors disabled:opacity-50">
                    {uploadingGallery ? <Loader2 className="w-5 h-5 text-zinc-500 animate-spin" /> : <Plus className="w-5 h-5 text-zinc-500" />}
                  </button>
                </div>
                <p className="text-xs text-zinc-500">Drag to reorder. Click + to add. Max 10MB per image.</p>
              </div>

              {/* Tags */}
              <div className="bg-zinc-800 rounded-xl p-4">
                <label className="block text-sm text-zinc-300 mb-2">Tags</label>
                <div className="flex gap-2 mb-2 flex-wrap">
                  {form.tags.map(tag => (
                    <span key={tag} className="px-2 py-1 bg-orange-500/20 text-orange-400 text-xs rounded-full flex items-center gap-1">
                      {tag}
                      <button type="button" onClick={() => removeTag(tag)} className="hover:text-white"><X className="w-3 h-3" /></button>
                    </span>
                  ))}
                </div>
                <div className="flex gap-2">
                  <input type="text" value={tagInput} onChange={(e) => setTagInput(e.target.value)} onKeyPress={(e) => e.key === 'Enter' && (e.preventDefault(), addTag())} placeholder="Add tag..." className="flex-1 px-3 py-2 bg-zinc-900 border border-zinc-700 rounded-lg text-white placeholder-zinc-600 text-sm focus:border-orange-500 focus:outline-none" />
                  <button type="button" onClick={addTag} className="px-3 py-2 bg-zinc-700 text-zinc-300 rounded-lg hover:bg-zinc-600 text-sm">Add</button>
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
