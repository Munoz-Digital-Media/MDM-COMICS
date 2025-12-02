import React, { useState, useEffect } from "react";
import { X, Search, Plus, Trash2, Save, Package, Loader2 } from "lucide-react";
import { comicsAPI } from "../services/api";
import { adminAPI } from "../services/adminApi";

export default function AdminConsole({ onClose, token }) {
  const [activeTab, setActiveTab] = useState("search");
  const [searchParams, setSearchParams] = useState({ series: "", number: "", publisher: "" });
  const [searchResults, setSearchResults] = useState([]);
  const [searchLoading, setSearchLoading] = useState(false);
  const [products, setProducts] = useState([]);
  const [productsLoading, setProductsLoading] = useState(false);
  const [productsTotal, setProductsTotal] = useState(0);
  const [selectedComic, setSelectedComic] = useState(null);
  const [comicDetails, setComicDetails] = useState(null);
  const [detailsLoading, setDetailsLoading] = useState(false);
  const [productForm, setProductForm] = useState({
    sku: "", name: "", description: "", category: "comics", subcategory: "",
    price: "", original_price: "", stock: 1, image_url: "",
    issue_number: "", publisher: "", year: "", featured: false, tags: [],
  });
  const [saving, setSaving] = useState(false);
  const [message, setMessage] = useState(null);

  useEffect(() => {
    if (activeTab === "products") loadProducts();
  }, [activeTab]);

  const loadProducts = async () => {
    setProductsLoading(true);
    try {
      const result = await adminAPI.getProducts({});
      setProducts(result.products || []);
      setProductsTotal(result.total || 0);
    } catch (err) { console.error(err); }
    finally { setProductsLoading(false); }
  };

  const handleSearch = async (e) => {
    e?.preventDefault();
    setSearchLoading(true);
    try {
      const result = await comicsAPI.search({
        series: searchParams.series, number: searchParams.number,
        publisher: searchParams.publisher, page: 1,
      });
      setSearchResults(result.results || []);
    } catch (err) {
      setMessage({ type: "error", text: "Search failed: " + err.message });
    } finally { setSearchLoading(false); }
  };

  const selectComic = async (comic) => {
    setSelectedComic(comic);
    setDetailsLoading(true);
    try {
      const details = await comicsAPI.getIssue(comic.id);
      setComicDetails(details);
      const seriesName = details.series?.name || comic.series?.name || "";
      const issueNum = details.number || comic.number || "";
      const pubName = details.publisher?.name || details.series?.publisher?.name || "";
      const coverYear = details.cover_date ? new Date(details.cover_date).getFullYear() : "";
      setProductForm({
        sku: "COMIC-" + comic.id, name: seriesName + (issueNum ? " #" + issueNum : ""),
        description: details.desc || "", category: "comics", subcategory: pubName,
        price: "", original_price: "", stock: 1, image_url: details.image || comic.image || "",
        issue_number: issueNum, publisher: pubName, year: coverYear, featured: false, tags: [],
      });
      setActiveTab("create");
    } catch (err) {
      setMessage({ type: "error", text: "Failed to load comic details" });
    } finally { setDetailsLoading(false); }
  };

  const handleCreateProduct = async (e) => {
    e.preventDefault();
    if (!productForm.price || !productForm.name || !productForm.sku) {
      setMessage({ type: "error", text: "Name, SKU, and Price are required" });
      return;
    }
    setSaving(true);
    try {
      const data = {
        ...productForm,
        price: parseFloat(productForm.price),
        original_price: productForm.original_price ? parseFloat(productForm.original_price) : null,
        stock: parseInt(productForm.stock) || 1,
        year: productForm.year ? parseInt(productForm.year) : null,
        images: productForm.image_url ? [productForm.image_url] : [],
      };
      await adminAPI.createProduct(token, data);
      setMessage({ type: "success", text: "Product created!" });
      setProductForm({
        sku: "", name: "", description: "", category: "comics", subcategory: "",
        price: "", original_price: "", stock: 1, image_url: "",
        issue_number: "", publisher: "", year: "", featured: false, tags: [],
      });
      setSelectedComic(null);
      setComicDetails(null);
      setActiveTab("products");
      loadProducts();
    } catch (err) {
      setMessage({ type: "error", text: "Failed: " + err.message });
    } finally { setSaving(false); }
  };

  const handleDeleteProduct = async (productId) => {
    if (!confirm("Delete this product?")) return;
    try {
      await adminAPI.deleteProduct(token, productId);
      setMessage({ type: "success", text: "Product deleted" });
      loadProducts();
    } catch (err) {
      setMessage({ type: "error", text: "Failed: " + err.message });
    }
  };

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center">
      <div className="absolute inset-0 bg-black/80 backdrop-blur-sm" onClick={onClose} />
      <div className="relative bg-zinc-900 rounded-2xl border border-zinc-700 w-full max-w-6xl mx-4 max-h-[90vh] overflow-hidden flex flex-col">
        <div className="flex items-center justify-between p-4 border-b border-zinc-800">
          <h2 className="font-bold text-xl text-white flex items-center gap-2">
            <Package className="w-6 h-6 text-orange-500" /> Admin Console
          </h2>
          <button onClick={onClose} className="p-2 hover:bg-zinc-800 rounded-lg">
            <X className="w-5 h-5 text-zinc-400" />
          </button>
        </div>
        
        {message && (
          <div className={"mx-4 mt-2 p-3 rounded-lg text-sm " + (message.type === "error" ? "bg-red-900/50 text-red-300" : "bg-green-900/50 text-green-300")}>
            {message.text}
            <button onClick={() => setMessage(null)} className="float-right"><X className="w-4 h-4" /></button>
          </div>
        )}
        
        <div className="flex border-b border-zinc-800">
          <button onClick={() => setActiveTab("search")} className={"px-6 py-3 font-medium " + (activeTab === "search" ? "text-orange-500 border-b-2 border-orange-500" : "text-zinc-400 hover:text-white")}>
            <Search className="w-4 h-4 inline mr-2" />Find Comics
          </button>
          <button onClick={() => setActiveTab("products")} className={"px-6 py-3 font-medium " + (activeTab === "products" ? "text-orange-500 border-b-2 border-orange-500" : "text-zinc-400 hover:text-white")}>
            <Package className="w-4 h-4 inline mr-2" />Products ({productsTotal})
          </button>
          <button onClick={() => setActiveTab("create")} className={"px-6 py-3 font-medium " + (activeTab === "create" ? "text-orange-500 border-b-2 border-orange-500" : "text-zinc-400 hover:text-white")}>
            <Plus className="w-4 h-4 inline mr-2" />Create Product
          </button>
        </div>
        
        <div className="flex-1 overflow-auto p-4">
          {activeTab === "search" && (
            <div>
              <form onSubmit={handleSearch} className="flex gap-3 mb-4">
                <input type="text" placeholder="Series name..." value={searchParams.series} onChange={(e) => setSearchParams({ ...searchParams, series: e.target.value })} className="flex-1 px-4 py-2 bg-zinc-800 border border-zinc-700 rounded-lg text-white" />
                <input type="text" placeholder="Issue #" value={searchParams.number} onChange={(e) => setSearchParams({ ...searchParams, number: e.target.value })} className="w-24 px-4 py-2 bg-zinc-800 border border-zinc-700 rounded-lg text-white" />
                <input type="text" placeholder="Publisher" value={searchParams.publisher} onChange={(e) => setSearchParams({ ...searchParams, publisher: e.target.value })} className="w-32 px-4 py-2 bg-zinc-800 border border-zinc-700 rounded-lg text-white" />
                <button type="submit" disabled={searchLoading} className="px-6 py-2 bg-orange-500 text-white rounded-lg font-medium hover:bg-orange-600 disabled:opacity-50">
                  {searchLoading ? <Loader2 className="w-5 h-5 animate-spin" /> : "Search"}
                </button>
              </form>
              <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
                {searchResults.map((comic) => (
                  <div key={comic.id} onClick={() => selectComic(comic)} className="bg-zinc-800 rounded-lg overflow-hidden cursor-pointer hover:ring-2 hover:ring-orange-500">
                    <div className="h-40 bg-zinc-700">{comic.image && <img src={comic.image} alt="" className="w-full h-full object-cover" />}</div>
                    <div className="p-2">
                      <p className="text-xs text-orange-500">{comic.series?.name}</p>
                      <p className="text-sm font-medium text-white truncate">#{comic.number} {comic.issue_name || ""}</p>
                      <p className="text-xs text-zinc-500">{comic.cover_date}</p>
                    </div>
                  </div>
                ))}
              </div>
              {searchResults.length === 0 && !searchLoading && <div className="text-center py-12 text-zinc-500">Search for comics to create products</div>}
              {detailsLoading && <div className="fixed inset-0 bg-black/50 flex items-center justify-center z-50"><Loader2 className="w-8 h-8 text-orange-500 animate-spin" /></div>}
            </div>
          )}

          {activeTab === "products" && (
            <div>
              {productsLoading ? (
                <div className="flex justify-center py-12"><Loader2 className="w-8 h-8 text-orange-500 animate-spin" /></div>
              ) : products.length === 0 ? (
                <div className="text-center py-12 text-zinc-500">No products yet. Search for comics and create your first product!</div>
              ) : (
                <div className="space-y-2">
                  {products.map((product) => (
                    <div key={product.id} className="flex items-center gap-4 bg-zinc-800 rounded-lg p-3">
                      <div className="w-16 h-20 bg-zinc-700 rounded overflow-hidden flex-shrink-0">
                        {product.image_url && <img src={product.image_url} alt="" className="w-full h-full object-cover" />}
                      </div>
                      <div className="flex-1 min-w-0">
                        <p className="font-medium text-white truncate">{product.name}</p>
                        <p className="text-sm text-zinc-500">SKU: {product.sku}</p>
                        <div className="flex gap-4 text-sm">
                          <span className="text-orange-500 font-bold">${product.price}</span>
                          <span className="text-zinc-400">Stock: {product.stock}</span>
                          {product.featured && <span className="text-yellow-500">Featured</span>}
                        </div>
                      </div>
                      <button onClick={() => handleDeleteProduct(product.id)} className="p-2 text-red-500 hover:bg-red-500/10 rounded-lg">
                        <Trash2 className="w-4 h-4" />
                      </button>
                    </div>
                  ))}
                </div>
              )}
            </div>
          )}

          {activeTab === "create" && (
            <form onSubmit={handleCreateProduct} className="grid md:grid-cols-2 gap-6">
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
                    </select>
                  </div>
                </div>
                <div>
                  <label className="block text-sm text-zinc-400 mb-1">Name *</label>
                  <input type="text" value={productForm.name} onChange={(e) => setProductForm({ ...productForm, name: e.target.value })} className="w-full px-3 py-2 bg-zinc-800 border border-zinc-700 rounded-lg text-white" required />
                </div>
                <div>
                  <label className="block text-sm text-zinc-400 mb-1">Description</label>
                  <textarea value={productForm.description} onChange={(e) => setProductForm({ ...productForm, description: e.target.value })} rows={3} className="w-full px-3 py-2 bg-zinc-800 border border-zinc-700 rounded-lg text-white" />
                </div>
                <div className="grid grid-cols-3 gap-3">
                  <div>
                    <label className="block text-sm text-zinc-400 mb-1">Price *</label>
                    <input type="number" step="0.01" value={productForm.price} onChange={(e) => setProductForm({ ...productForm, price: e.target.value })} className="w-full px-3 py-2 bg-zinc-800 border border-zinc-700 rounded-lg text-white" required />
                  </div>
                  <div>
                    <label className="block text-sm text-zinc-400 mb-1">Original Price</label>
                    <input type="number" step="0.01" value={productForm.original_price} onChange={(e) => setProductForm({ ...productForm, original_price: e.target.value })} className="w-full px-3 py-2 bg-zinc-800 border border-zinc-700 rounded-lg text-white" />
                  </div>
                  <div>
                    <label className="block text-sm text-zinc-400 mb-1">Stock</label>
                    <input type="number" value={productForm.stock} onChange={(e) => setProductForm({ ...productForm, stock: e.target.value })} className="w-full px-3 py-2 bg-zinc-800 border border-zinc-700 rounded-lg text-white" />
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
                    <input type="number" value={productForm.year} onChange={(e) => setProductForm({ ...productForm, year: e.target.value })} className="w-full px-3 py-2 bg-zinc-800 border border-zinc-700 rounded-lg text-white" />
                  </div>
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
              <div className="bg-zinc-800 rounded-xl p-4">
                <h3 className="font-medium text-zinc-400 mb-3">Preview</h3>
                <div className="bg-zinc-900 rounded-lg overflow-hidden">
                  <div className="h-48 bg-zinc-700">{productForm.image_url && <img src={productForm.image_url} alt="" className="w-full h-full object-contain" onError={(e) => e.target.style.display = "none"} />}</div>
                  <div className="p-4">
                    <p className="text-xs text-orange-500 mb-1">{productForm.subcategory || productForm.publisher}</p>
                    <h4 className="font-bold text-white mb-2">{productForm.name || "Product Name"}</h4>
                    <p className="text-sm text-zinc-500 mb-3 line-clamp-2">{productForm.description || "Description..."}</p>
                    <div className="flex items-center justify-between">
                      <div>
                        <span className="text-xl font-bold text-white">${productForm.price || "0.00"}</span>
                        {productForm.original_price && <span className="ml-2 text-sm text-zinc-500 line-through">${productForm.original_price}</span>}
                      </div>
                      <span className="text-sm text-zinc-400">Stock: {productForm.stock}</span>
                    </div>
                  </div>
                </div>
              </div>
            </form>
          )}
        </div>
      </div>
    </div>
  );
}
