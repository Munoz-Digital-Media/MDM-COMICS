/**
 * BrandAssets - Admin UI for Brand Asset Management
 *
 * v1.0.0: Upload, manage, and version brand assets (logos, banners, etc.)
 * Assets stored in S3, URLs tracked in database.
 */
import React, { useState, useEffect, useRef, useCallback } from 'react';
import {
  Upload, Image, Trash2, RotateCcw, History, ChevronDown,
  AlertCircle, Check, X, Loader2, ExternalLink, Settings2
} from 'lucide-react';
import { adminAPI } from '../../../services/adminApi';

const ASSET_TYPES = [
  { id: 'logo', label: 'Logo' },
  { id: 'banner', label: 'Banner' },
  { id: 'icon', label: 'Icon' },
  { id: 'favicon', label: 'Favicon' },
  { id: 'social', label: 'Social Media' },
];

const SETTING_KEYS = [
  { key: 'rack_factor_logo_url', label: 'The Rack Factor Logo' },
  { key: 'site_logo_url', label: 'Site Logo' },
  { key: 'site_logo_dark_url', label: 'Site Logo (Dark)' },
  { key: 'favicon_url', label: 'Favicon' },
  { key: 'email_header_logo_url', label: 'Email Header Logo' },
  { key: 'og_default_image_url', label: 'OG Default Image' },
];

function formatBytes(bytes) {
  if (bytes === 0) return '0 Bytes';
  const k = 1024;
  const sizes = ['Bytes', 'KB', 'MB'];
  const i = Math.floor(Math.log(bytes) / Math.log(k));
  return parseFloat((bytes / Math.pow(k, i)).toFixed(1)) + ' ' + sizes[i];
}

function formatDate(dateStr) {
  if (!dateStr) return '-';
  return new Date(dateStr).toLocaleDateString('en-US', {
    month: 'short',
    day: 'numeric',
    year: 'numeric',
    hour: '2-digit',
    minute: '2-digit',
  });
}

export default function BrandAssets() {
  const [assets, setAssets] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [selectedType, setSelectedType] = useState('');
  const [showUpload, setShowUpload] = useState(false);
  const [selectedAsset, setSelectedAsset] = useState(null);
  const [uploadProgress, setUploadProgress] = useState(false);
  const fileInputRef = useRef(null);

  // Upload form state
  const [uploadName, setUploadName] = useState('');
  const [uploadType, setUploadType] = useState('logo');
  const [uploadSettingKey, setUploadSettingKey] = useState('');
  const [uploadFile, setUploadFile] = useState(null);
  const [uploadPreview, setUploadPreview] = useState(null);

  const loadAssets = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const data = await adminAPI.getAssets({ assetType: selectedType || undefined });
      setAssets(data.items || []);
    } catch (err) {
      setError(err.message);
    } finally {
      setLoading(false);
    }
  }, [selectedType]);

  useEffect(() => {
    loadAssets();
  }, [loadAssets]);

  const handleFileChange = (e) => {
    const file = e.target.files?.[0];
    if (!file) return;

    setUploadFile(file);

    // Generate preview
    const reader = new FileReader();
    reader.onload = (e) => setUploadPreview(e.target.result);
    reader.readAsDataURL(file);

    // Auto-fill name if empty
    if (!uploadName) {
      const name = file.name.replace(/\.[^/.]+$/, '').replace(/[-_]/g, ' ');
      setUploadName(name.charAt(0).toUpperCase() + name.slice(1));
    }
  };

  const handleUpload = async () => {
    if (!uploadFile || !uploadName) return;

    setUploadProgress(true);
    try {
      await adminAPI.uploadAsset(
        uploadFile,
        uploadName,
        uploadType,
        uploadSettingKey || null
      );

      // Reset form
      setUploadFile(null);
      setUploadPreview(null);
      setUploadName('');
      setUploadSettingKey('');
      setShowUpload(false);

      // Reload assets
      await loadAssets();
    } catch (err) {
      setError(err.message);
    } finally {
      setUploadProgress(false);
    }
  };

  const handleDelete = async (assetId) => {
    if (!confirm('Delete this asset? It can be restored later.')) return;

    try {
      await adminAPI.deleteAsset(assetId);
      await loadAssets();
      setSelectedAsset(null);
    } catch (err) {
      setError(err.message);
    }
  };

  const handleRestore = async (assetId) => {
    try {
      await adminAPI.restoreAsset(assetId);
      await loadAssets();
    } catch (err) {
      setError(err.message);
    }
  };

  const loadAssetDetails = async (assetId) => {
    try {
      const data = await adminAPI.getAsset(assetId);
      setSelectedAsset(data);
    } catch (err) {
      setError(err.message);
    }
  };

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div>
          <h2 className="text-xl font-bold text-white">Brand Assets</h2>
          <p className="text-sm text-zinc-400 mt-1">
            Manage logos, banners, and other brand assets
          </p>
        </div>
        <button
          onClick={() => setShowUpload(true)}
          className="flex items-center gap-2 px-4 py-2 bg-red-500 text-white rounded-lg hover:bg-red-600 transition-colors"
        >
          <Upload className="w-4 h-4" />
          Upload Asset
        </button>
      </div>

      {/* Error display */}
      {error && (
        <div className="p-4 bg-red-500/10 border border-red-500/30 rounded-lg flex items-center gap-3">
          <AlertCircle className="w-5 h-5 text-red-400 flex-shrink-0" />
          <span className="text-red-400">{error}</span>
          <button onClick={() => setError(null)} className="ml-auto text-red-400 hover:text-red-300">
            <X className="w-4 h-4" />
          </button>
        </div>
      )}

      {/* Filter */}
      <div className="flex items-center gap-3">
        <span className="text-sm text-zinc-400">Filter:</span>
        <select
          value={selectedType}
          onChange={(e) => setSelectedType(e.target.value)}
          className="bg-zinc-800 border border-zinc-700 rounded-lg px-3 py-1.5 text-white text-sm"
        >
          <option value="">All Types</option>
          {ASSET_TYPES.map(type => (
            <option key={type.id} value={type.id}>{type.label}</option>
          ))}
        </select>
      </div>

      {/* Assets Grid */}
      {loading ? (
        <div className="flex items-center justify-center py-12">
          <Loader2 className="w-8 h-8 text-zinc-500 animate-spin" />
        </div>
      ) : assets.length === 0 ? (
        <div className="text-center py-12 text-zinc-500">
          No assets found. Upload your first brand asset!
        </div>
      ) : (
        <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-4 gap-4">
          {assets.map(asset => (
            <div
              key={asset.id}
              onClick={() => loadAssetDetails(asset.id)}
              className={`
                relative bg-zinc-800/50 border rounded-lg overflow-hidden cursor-pointer
                transition-all hover:border-red-500/50
                ${asset.is_deleted ? 'opacity-50 border-zinc-700' : 'border-zinc-700'}
                ${selectedAsset?.id === asset.id ? 'ring-2 ring-red-500' : ''}
              `}
            >
              {/* Image preview */}
              <div className="aspect-square bg-zinc-900 flex items-center justify-center p-4">
                <img
                  src={asset.url}
                  alt={asset.name}
                  className="max-w-full max-h-full object-contain"
                  onError={(e) => {
                    e.target.onerror = null;
                    e.target.src = 'data:image/svg+xml,<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="gray" stroke-width="2"><rect x="3" y="3" width="18" height="18" rx="2"/><circle cx="8.5" cy="8.5" r="1.5"/><path d="m21 15-5-5L5 21"/></svg>';
                  }}
                />
              </div>

              {/* Info */}
              <div className="p-3">
                <h3 className="font-medium text-white truncate">{asset.name}</h3>
                <div className="flex items-center justify-between mt-1 text-xs text-zinc-500">
                  <span className="capitalize">{asset.asset_type}</span>
                  <span>v{asset.version}</span>
                </div>
              </div>

              {/* Deleted badge */}
              {asset.is_deleted && (
                <div className="absolute top-2 right-2 px-2 py-0.5 bg-red-500/20 text-red-400 text-xs rounded">
                  Deleted
                </div>
              )}
            </div>
          ))}
        </div>
      )}

      {/* Upload Modal */}
      {showUpload && (
        <div className="fixed inset-0 bg-black/70 z-50 flex items-center justify-center p-4">
          <div className="bg-zinc-900 border border-zinc-700 rounded-xl w-full max-w-lg">
            <div className="flex items-center justify-between p-4 border-b border-zinc-700">
              <h3 className="text-lg font-semibold text-white">Upload Brand Asset</h3>
              <button
                onClick={() => setShowUpload(false)}
                className="p-1 hover:bg-zinc-800 rounded-lg"
              >
                <X className="w-5 h-5 text-zinc-400" />
              </button>
            </div>

            <div className="p-4 space-y-4">
              {/* File drop zone */}
              <div
                onClick={() => fileInputRef.current?.click()}
                className={`
                  border-2 border-dashed rounded-lg p-6 text-center cursor-pointer transition-colors
                  ${uploadPreview ? 'border-green-500/50 bg-green-500/5' : 'border-zinc-600 hover:border-zinc-500'}
                `}
              >
                {uploadPreview ? (
                  <div className="space-y-3">
                    <img src={uploadPreview} alt="Preview" className="max-h-32 mx-auto object-contain" />
                    <p className="text-sm text-green-400">{uploadFile?.name}</p>
                  </div>
                ) : (
                  <div className="space-y-2">
                    <Upload className="w-8 h-8 text-zinc-500 mx-auto" />
                    <p className="text-zinc-400">Click to select an image</p>
                    <p className="text-xs text-zinc-500">PNG, JPG, GIF, WebP, SVG (max 5MB)</p>
                  </div>
                )}
                <input
                  ref={fileInputRef}
                  type="file"
                  accept="image/png,image/jpeg,image/gif,image/webp,image/svg+xml"
                  onChange={handleFileChange}
                  className="hidden"
                />
              </div>

              {/* Name */}
              <div>
                <label className="block text-sm text-zinc-400 mb-1">Asset Name</label>
                <input
                  type="text"
                  value={uploadName}
                  onChange={(e) => setUploadName(e.target.value)}
                  placeholder="e.g., The Rack Factor Logo"
                  className="w-full bg-zinc-800 border border-zinc-700 rounded-lg px-3 py-2 text-white"
                />
              </div>

              {/* Type */}
              <div>
                <label className="block text-sm text-zinc-400 mb-1">Asset Type</label>
                <select
                  value={uploadType}
                  onChange={(e) => setUploadType(e.target.value)}
                  className="w-full bg-zinc-800 border border-zinc-700 rounded-lg px-3 py-2 text-white"
                >
                  {ASSET_TYPES.map(type => (
                    <option key={type.id} value={type.id}>{type.label}</option>
                  ))}
                </select>
              </div>

              {/* Link to setting */}
              <div>
                <label className="block text-sm text-zinc-400 mb-1">
                  Link to Setting (optional)
                </label>
                <select
                  value={uploadSettingKey}
                  onChange={(e) => setUploadSettingKey(e.target.value)}
                  className="w-full bg-zinc-800 border border-zinc-700 rounded-lg px-3 py-2 text-white"
                >
                  <option value="">Don't link to a setting</option>
                  {SETTING_KEYS.map(s => (
                    <option key={s.key} value={s.key}>{s.label}</option>
                  ))}
                </select>
                <p className="text-xs text-zinc-500 mt-1">
                  Linking will automatically update the setting with the new URL
                </p>
              </div>
            </div>

            <div className="flex items-center justify-end gap-3 p-4 border-t border-zinc-700">
              <button
                onClick={() => setShowUpload(false)}
                className="px-4 py-2 text-zinc-400 hover:text-white transition-colors"
              >
                Cancel
              </button>
              <button
                onClick={handleUpload}
                disabled={!uploadFile || !uploadName || uploadProgress}
                className="flex items-center gap-2 px-4 py-2 bg-red-500 text-white rounded-lg hover:bg-red-600 disabled:opacity-50 disabled:cursor-not-allowed transition-colors"
              >
                {uploadProgress ? (
                  <>
                    <Loader2 className="w-4 h-4 animate-spin" />
                    Uploading...
                  </>
                ) : (
                  <>
                    <Upload className="w-4 h-4" />
                    Upload
                  </>
                )}
              </button>
            </div>
          </div>
        </div>
      )}

      {/* Asset Detail Sidebar */}
      {selectedAsset && (
        <div className="fixed inset-y-0 right-0 w-96 bg-zinc-900 border-l border-zinc-700 z-40 overflow-y-auto">
          <div className="sticky top-0 bg-zinc-900 border-b border-zinc-700 p-4 flex items-center justify-between">
            <h3 className="font-semibold text-white">Asset Details</h3>
            <button
              onClick={() => setSelectedAsset(null)}
              className="p-1 hover:bg-zinc-800 rounded-lg"
            >
              <X className="w-5 h-5 text-zinc-400" />
            </button>
          </div>

          <div className="p-4 space-y-6">
            {/* Preview */}
            <div className="bg-zinc-800 rounded-lg p-4 flex items-center justify-center">
              <img
                src={selectedAsset.url}
                alt={selectedAsset.name}
                className="max-w-full max-h-48 object-contain"
              />
            </div>

            {/* Info */}
            <div className="space-y-3">
              <div>
                <span className="text-xs text-zinc-500">Name</span>
                <p className="text-white">{selectedAsset.name}</p>
              </div>
              <div>
                <span className="text-xs text-zinc-500">Slug</span>
                <p className="text-zinc-400 font-mono text-sm">{selectedAsset.slug}</p>
              </div>
              <div className="grid grid-cols-2 gap-3">
                <div>
                  <span className="text-xs text-zinc-500">Type</span>
                  <p className="text-white capitalize">{selectedAsset.asset_type}</p>
                </div>
                <div>
                  <span className="text-xs text-zinc-500">Version</span>
                  <p className="text-white">v{selectedAsset.current_version}</p>
                </div>
              </div>
              <div className="grid grid-cols-2 gap-3">
                <div>
                  <span className="text-xs text-zinc-500">Dimensions</span>
                  <p className="text-white">
                    {selectedAsset.width && selectedAsset.height
                      ? `${selectedAsset.width} x ${selectedAsset.height}`
                      : 'N/A'}
                  </p>
                </div>
                <div>
                  <span className="text-xs text-zinc-500">Size</span>
                  <p className="text-white">{formatBytes(selectedAsset.file_size)}</p>
                </div>
              </div>
              <div>
                <span className="text-xs text-zinc-500">Updated</span>
                <p className="text-white">{formatDate(selectedAsset.updated_at)}</p>
              </div>
            </div>

            {/* URL */}
            <div>
              <span className="text-xs text-zinc-500">URL</span>
              <div className="flex items-center gap-2 mt-1">
                <input
                  type="text"
                  value={selectedAsset.url}
                  readOnly
                  className="flex-1 bg-zinc-800 border border-zinc-700 rounded px-2 py-1 text-sm text-zinc-400 font-mono"
                />
                <a
                  href={selectedAsset.url}
                  target="_blank"
                  rel="noopener noreferrer"
                  className="p-2 hover:bg-zinc-800 rounded-lg"
                >
                  <ExternalLink className="w-4 h-4 text-zinc-400" />
                </a>
              </div>
            </div>

            {/* Linked Settings */}
            {selectedAsset.linked_settings?.length > 0 && (
              <div>
                <span className="text-xs text-zinc-500 flex items-center gap-1">
                  <Settings2 className="w-3 h-3" />
                  Linked Settings
                </span>
                <div className="mt-2 space-y-1">
                  {selectedAsset.linked_settings.map(s => (
                    <div key={s.key} className="text-sm text-zinc-400 font-mono bg-zinc-800 px-2 py-1 rounded">
                      {s.key}
                    </div>
                  ))}
                </div>
              </div>
            )}

            {/* Version History */}
            {selectedAsset.versions?.length > 0 && (
              <div>
                <span className="text-xs text-zinc-500 flex items-center gap-1">
                  <History className="w-3 h-3" />
                  Version History
                </span>
                <div className="mt-2 space-y-2">
                  {selectedAsset.versions.map(v => (
                    <div key={v.id} className="flex items-center gap-3 p-2 bg-zinc-800 rounded-lg">
                      <span className="text-xs text-zinc-500">v{v.version}</span>
                      <span className="text-xs text-zinc-400">{formatBytes(v.file_size)}</span>
                      <span className="text-xs text-zinc-500 ml-auto">{formatDate(v.created_at)}</span>
                    </div>
                  ))}
                </div>
              </div>
            )}

            {/* Actions */}
            <div className="pt-4 border-t border-zinc-700 space-y-2">
              {selectedAsset.deleted_at ? (
                <button
                  onClick={() => handleRestore(selectedAsset.id)}
                  className="w-full flex items-center justify-center gap-2 px-4 py-2 bg-green-500/20 text-green-400 border border-green-500/30 rounded-lg hover:bg-green-500/30 transition-colors"
                >
                  <RotateCcw className="w-4 h-4" />
                  Restore Asset
                </button>
              ) : (
                <button
                  onClick={() => handleDelete(selectedAsset.id)}
                  className="w-full flex items-center justify-center gap-2 px-4 py-2 bg-red-500/20 text-red-400 border border-red-500/30 rounded-lg hover:bg-red-500/30 transition-colors"
                >
                  <Trash2 className="w-4 h-4" />
                  Delete Asset
                </button>
              )}
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
