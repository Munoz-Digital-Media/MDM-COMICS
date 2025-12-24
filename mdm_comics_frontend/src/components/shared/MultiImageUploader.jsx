/**
 * MultiImageUploader - Reusable multi-image upload component
 * Used across Create Comics, Create Funkos, Create Supplies, and Create Bundles
 *
 * Features (per sharedComponents.imageUploader):
 * - Upload 1-9 images (min/max configurable)
 * - Set/demote/promote primary image
 * - Drag to reorder
 * - File picker and drag-and-drop
 * - S3 upload integration
 */
import React, { useState, useCallback, useRef } from 'react';
import { Upload, Star, Trash2, GripVertical, Image, Loader2, X, Camera } from 'lucide-react';
import { adminAPI } from '../../services/adminApi';

export default function MultiImageUploader({
  images = [],
  onChange,
  maxImages = 9,
  minImages = 1,
  productType = 'product',
  productId = null,
}) {
  const [uploading, setUploading] = useState(false);
  const [draggedIndex, setDraggedIndex] = useState(null);
  const [dragOverIndex, setDragOverIndex] = useState(null);
  const fileInputRef = useRef(null);

  // Handle file selection
  const handleFileSelect = async (e) => {
    const files = Array.from(e.target.files || []);
    if (files.length === 0) return;

    // Check max limit
    const remaining = maxImages - images.length;
    if (remaining <= 0) return;

    const filesToUpload = files.slice(0, remaining);
    setUploading(true);

    try {
      const newImages = [...images];

      for (const file of filesToUpload) {
        // Validate file type
        if (!file.type.startsWith('image/')) {
          console.warn('Skipping non-image file:', file.name);
          continue;
        }

        // Validate file size (10MB max)
        if (file.size > 10 * 1024 * 1024) {
          console.warn('File too large:', file.name);
          continue;
        }

        try {
          // Upload to S3 via backend
          const result = await adminAPI.uploadProductImage(file, productType, productId);

          // Add to images array
          newImages.push({
            url: result.url,
            s3Key: result.s3_key,
            isPrimary: newImages.length === 0, // First image is primary
            order: newImages.length,
          });
        } catch (uploadErr) {
          console.error('Failed to upload image:', file.name, uploadErr);
          // For development, create a local preview URL
          if (import.meta.env.DEV) {
            const previewUrl = URL.createObjectURL(file);
            newImages.push({
              url: previewUrl,
              s3Key: null,
              isPrimary: newImages.length === 0,
              order: newImages.length,
              isLocal: true,
            });
          }
        }
      }

      onChange(newImages);
    } finally {
      setUploading(false);
      // Reset input
      if (fileInputRef.current) {
        fileInputRef.current.value = '';
      }
    }
  };

  // Handle drag and drop upload
  const handleDrop = (e) => {
    e.preventDefault();
    e.stopPropagation();

    const files = Array.from(e.dataTransfer?.files || []);
    if (files.length > 0) {
      // Simulate file input change
      const dataTransfer = new DataTransfer();
      files.forEach(f => dataTransfer.items.add(f));

      if (fileInputRef.current) {
        fileInputRef.current.files = dataTransfer.files;
        handleFileSelect({ target: { files: dataTransfer.files } });
      }
    }
  };

  // Set image as primary
  const setPrimary = (index) => {
    const newImages = images.map((img, i) => ({
      ...img,
      isPrimary: i === index,
    }));
    onChange(newImages);
  };

  // Remove image
  const removeImage = (index) => {
    const newImages = images.filter((_, i) => i !== index);
    // If we removed the primary, make the first one primary
    if (newImages.length > 0 && !newImages.some(img => img.isPrimary)) {
      newImages[0].isPrimary = true;
    }
    // Update order
    newImages.forEach((img, i) => {
      img.order = i;
    });
    onChange(newImages);
  };

  // Drag and drop reordering
  const handleDragStart = (e, index) => {
    setDraggedIndex(index);
    e.dataTransfer.effectAllowed = 'move';
  };

  const handleDragOver = (e, index) => {
    e.preventDefault();
    if (draggedIndex !== null && draggedIndex !== index) {
      setDragOverIndex(index);
    }
  };

  const handleDragEnd = () => {
    if (draggedIndex !== null && dragOverIndex !== null && draggedIndex !== dragOverIndex) {
      const newImages = [...images];
      const [draggedItem] = newImages.splice(draggedIndex, 1);
      newImages.splice(dragOverIndex, 0, draggedItem);
      // Update order
      newImages.forEach((img, i) => {
        img.order = i;
      });
      onChange(newImages);
    }
    setDraggedIndex(null);
    setDragOverIndex(null);
  };

  const canAddMore = images.length < maxImages;
  const needsMore = images.length < minImages;

  return (
    <div className="space-y-3">
      <div className="flex items-center justify-between">
        <label className="block text-sm text-zinc-400">
          Images
          <span className={needsMore ? 'text-red-400 ml-1' : 'text-zinc-600 ml-1'}>
            ({images.length}/{maxImages}, min {minImages})
          </span>
        </label>
        {needsMore && (
          <span className="text-xs text-red-400">At least {minImages} image required</span>
        )}
      </div>

      {/* Upload Area */}
      {canAddMore && (
        <div
          className={`border-2 border-dashed rounded-lg p-6 text-center cursor-pointer transition-colors ${
            uploading
              ? 'border-zinc-600 bg-zinc-800/50'
              : 'border-zinc-700 hover:border-zinc-500 hover:bg-zinc-800/30'
          }`}
          onClick={() => !uploading && fileInputRef.current?.click()}
          onDrop={handleDrop}
          onDragOver={(e) => e.preventDefault()}
        >
          <input
            ref={fileInputRef}
            type="file"
            accept="image/*"
            multiple
            onChange={handleFileSelect}
            className="hidden"
          />
          {uploading ? (
            <div className="flex flex-col items-center gap-2">
              <Loader2 className="w-8 h-8 text-zinc-400 animate-spin" />
              <span className="text-sm text-zinc-400">Uploading...</span>
            </div>
          ) : (
            <div className="flex flex-col items-center gap-2">
              <Upload className="w-8 h-8 text-zinc-500" />
              <span className="text-sm text-zinc-400">
                Click or drag images to upload
              </span>
              <span className="text-xs text-zinc-600">
                PNG, JPG, WebP up to 10MB
              </span>
            </div>
          )}
        </div>
      )}

      {/* Image Grid */}
      {images.length > 0 && (
        <div className="grid grid-cols-3 gap-3">
          {images.map((image, index) => (
            <div
              key={image.url + index}
              className={`relative group rounded-lg overflow-hidden border-2 transition-all ${
                image.isPrimary
                  ? 'border-yellow-500'
                  : dragOverIndex === index
                    ? 'border-blue-500'
                    : 'border-zinc-700'
              } ${draggedIndex === index ? 'opacity-50' : ''}`}
              draggable
              onDragStart={(e) => handleDragStart(e, index)}
              onDragOver={(e) => handleDragOver(e, index)}
              onDragEnd={handleDragEnd}
            >
              {/* Image */}
              <div className="aspect-square bg-zinc-800">
                <img
                  src={image.url}
                  alt={`Image ${index + 1}`}
                  className="w-full h-full object-contain"
                  onError={(e) => {
                    e.target.onerror = null;
                    e.target.src = '/assets/no-cover.png';
                  }}
                />
              </div>

              {/* Primary Badge */}
              {image.isPrimary && (
                <div className="absolute top-1 left-1 px-2 py-0.5 bg-yellow-500 text-black text-xs font-bold rounded">
                  Primary
                </div>
              )}

              {/* Local Badge */}
              {image.isLocal && (
                <div className="absolute top-1 right-1 px-2 py-0.5 bg-orange-500 text-white text-xs rounded">
                  Local
                </div>
              )}

              {/* Drag Handle */}
              <div className="absolute top-1/2 left-1 -translate-y-1/2 opacity-0 group-hover:opacity-100 transition-opacity cursor-grab">
                <GripVertical className="w-5 h-5 text-white drop-shadow-lg" />
              </div>

              {/* Actions Overlay */}
              <div className="absolute inset-0 bg-black/60 opacity-0 group-hover:opacity-100 transition-opacity flex items-center justify-center gap-2">
                {!image.isPrimary && (
                  <button
                    type="button"
                    onClick={() => setPrimary(index)}
                    className="p-2 bg-yellow-500 text-black rounded-lg hover:bg-yellow-400"
                    title="Set as primary"
                  >
                    <Star className="w-4 h-4" />
                  </button>
                )}
                <button
                  type="button"
                  onClick={() => removeImage(index)}
                  className="p-2 bg-red-500 text-white rounded-lg hover:bg-red-400"
                  title="Remove"
                >
                  <Trash2 className="w-4 h-4" />
                </button>
              </div>

              {/* Order Number */}
              <div className="absolute bottom-1 right-1 w-6 h-6 bg-zinc-900/80 rounded-full flex items-center justify-center text-xs text-zinc-400">
                {index + 1}
              </div>
            </div>
          ))}
        </div>
      )}

      {/* Empty State */}
      {images.length === 0 && !canAddMore && (
        <div className="text-center py-8 text-zinc-500">
          <Image className="w-12 h-12 mx-auto mb-2 text-zinc-700" />
          <p>No images uploaded</p>
        </div>
      )}
    </div>
  );
}
