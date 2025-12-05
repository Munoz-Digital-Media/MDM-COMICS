import React, { useState, useMemo, useEffect } from "react";
  import { ShoppingCart, Search, X, Plus, Minus, Trash2, ChevronDown, Star, Package, CreditCard, Truck, User, LogOut, Database, Shield, Loader2 } from "lucide-react";
  import { authAPI } from "./services/api";
  import { useProducts } from "./hooks/useProducts";
  import ComicSearch from "./components/ComicSearch";
  import FunkoSearch from "./components/FunkoSearch";
  import AdminConsole from "./components/AdminConsole";
  import CheckoutForm, { OrderSuccess } from "./components/CheckoutForm";
  import ComingSoon from "./components/ComingSoon";
  import AuthModal from "./components/AuthModal";

  // ============================================================================
  // BUILD INFO - Update these on each release
  // ============================================================================
  const BUILD_INFO = {
    version: "1.5.0",
    buildNumber: 15,
    buildDate: new Date().toISOString(),
    environment: "development"
  };

  // Set to true to show "Coming Soon" page to visitors
  // Logged-in users bypass this and see the full site
  const UNDER_CONSTRUCTION = true;

  // ============================================================================
  // PRODUCT CARD COMPONENT
  // ============================================================================
  const ProductCard = ({ product, index, addToCart }) => (
    <div
      className="product-card bg-zinc-900 rounded-xl border border-zinc-800 fade-up"
      style={{ animationDelay: `${0.05 * index}s` }}
    >
      {/* Product Image */}
      <div className="relative h-36 bg-zinc-800 rounded-t-xl overflow-hidden">
        <img
          src={product.image}
          alt={product.name}
          className="w-full h-full object-cover"
          onError={(e) => {
            e.target.onerror = null;
            e.target.src = `https://placehold.co/400x500/27272a/f59e0b?text=${encodeURIComponent(product.category === 'comics' ? '📚' : '🎭')}`;
          }}
        />
        {/* Badges - compact */}
        <div className="absolute top-2 left-2 flex flex-col gap-1">
          {product.featured && (
            <span className="px-2 py-1 bg-orange-500 rounded-full text-[10px] font-bold text-white shadow-lg">
              ⭐ FEATURED
            </span>
          )}
          {product.stock <= 5 && (
            <span className="px-2 py-1 bg-red-600 rounded-full text-[10px] font-bold text-white shadow-lg">
              🔥 {product.stock} left
            </span>
          )}
        </div>
        {/* Sale badge - top right */}
        {product.originalPrice && (
          <div className="absolute top-2 right-2">
            <span className="px-2 py-1 bg-green-600 rounded-full text-[10px] font-bold text-white shadow-lg">
              SALE
            </span>
          </div>
        )}
      </div>

      {/* Product Info */}
      <div className="p-3">
        <p className="text-[10px] text-orange-500 font-semibold mb-0.5">{product.subcategory}</p>
        <h3 className="font-bold text-sm text-white mb-1 line-clamp-2 leading-tight">{product.name}</h3>
        <p className="text-zinc-500 text-xs mb-2 line-clamp-1">{product.description}</p>

        {/* Rating */}
        <div className="flex items-center gap-1 mb-2">
          <Star className="w-3 h-3 fill-orange-500 text-orange-500" />
          <span className="text-xs text-zinc-400">{product.rating}</span>
        </div>

        {/* Price & Add to Cart */}
        <div className="flex items-center justify-between">
          <div>
            <span className="text-lg font-bold text-white">${product.price}</span>
            {product.originalPrice && (
              <span className="ml-1 text-xs text-zinc-500 line-through">
                ${product.originalPrice}
              </span>
            )}
          </div>
          <button
            onClick={() => addToCart(product)}
            disabled={product.stock === 0}
            className={`p-2 rounded-lg transition-all ${
              product.stock === 0
                ? "bg-zinc-800 text-zinc-600 cursor-not-allowed"
                : "bg-orange-500 text-white hover:shadow-lg hover:shadow-orange-500/25 active:scale-95"
            }`}
          >
            <Plus className="w-4 h-4" />
          </button>
        </div>
      </div>
    </div>
  );

  // ============================================================================
  // MAIN APP COMPONENT
  // ============================================================================
  // NOTE: AuthModal has been extracted to components/AuthModal.jsx to prevent
  // form state resets on parent re-renders (FE-002 fix)
  export default function App() {
    // FE-004: Products from API instead of static array
    const { products, loading: productsLoading, error: productsError } = useProducts();

    // State management
    const [cart, setCart] = useState([]);
    const [isCartOpen, setIsCartOpen] = useState(false);
    const [searchQuery, setSearchQuery] = useState("");
    const [selectedCategory, setSelectedCategory] = useState("all");
    const [sortBy, setSortBy] = useState("featured");
    const [currentView, setCurrentView] = useState("shop");
    const [notification, setNotification] = useState(null);
    const [completedOrder, setCompletedOrder] = useState(null);

    // Auth state
    const [user, setUser] = useState(null);
    const [authToken, setAuthToken] = useState(localStorage.getItem('mdm_token'));
    const [isAuthModalOpen, setIsAuthModalOpen] = useState(false);
    const [isComicSearchOpen, setIsComicSearchOpen] = useState(false);
    const [isFunkoSearchOpen, setIsFunkoSearchOpen] = useState(false);
    const [isAdminOpen, setIsAdminOpen] = useState(false);
    const [authMode, setAuthMode] = useState("login");

    // Load user from token on mount
    useEffect(() => {
      if (authToken) {
        authAPI.me(authToken)
          .then(userData => {
            setUser({ ...userData, token: authToken });
          })
          .catch(() => {
            localStorage.removeItem('mdm_token');
            setAuthToken(null);
          });
      }
    }, []);

    // Show notification
    const showNotification = (message, type = "success") => {
      setNotification({ message, type });
      setTimeout(() => setNotification(null), 3000);
    };

    // Cart operations - using functional updaters to prevent stale closure bugs
    // Per constitution_ui.json §6: "Every action returns feedback state; critical flows persist"
    const addToCart = (product) => {
      setCart(prevCart => {
        const existing = prevCart.find(item => item.id === product.id);
        if (existing) {
          if (existing.quantity >= product.stock) {
            // Can't add more - show notification but return unchanged cart
            setTimeout(() => showNotification("Max stock reached", "error"), 0);
            return prevCart;
          }
          setTimeout(() => showNotification(`Added another ${product.name} to cart`), 0);
          return prevCart.map(item =>
            item.id === product.id
              ? { ...item, quantity: item.quantity + 1 }
              : item
          );
        }
        setTimeout(() => showNotification(`${product.name} added to cart`), 0);
        return [...prevCart, { ...product, quantity: 1 }];
      });
    };

    const updateQuantity = (productId, newQuantity) => {
      setCart(prevCart => {
        if (newQuantity <= 0) {
          setTimeout(() => showNotification("Item removed from cart"), 0);
          return prevCart.filter(item => item.id !== productId);
        }
        const product = products.find(p => p.id === productId);
        if (product && newQuantity > product.stock) return prevCart;

        return prevCart.map(item =>
          item.id === productId ? { ...item, quantity: newQuantity } : item
        );
      });
    };

    const removeFromCart = (productId) => {
      setCart(prevCart => prevCart.filter(item => item.id !== productId));
      showNotification("Item removed from cart");
    };

    const cartTotal = cart.reduce((sum, item) => sum + item.price * item.quantity, 0);
    const cartCount = cart.reduce((sum, item) => sum + item.quantity, 0);

    // Auth functions
    const handleSignup = async (name, email, password) => {
      try {
        const result = await authAPI.register(name, email, password);
        if (result.access_token) {
          localStorage.setItem('mdm_token', result.access_token);
          setAuthToken(result.access_token);
          const userData = await authAPI.me(result.access_token);
          setUser({ ...userData, token: result.access_token });
          setIsAuthModalOpen(false);
          showNotification(`Welcome to MDM Comics, ${name}!`);
          return true;
        }
      } catch (err) {
        showNotification(err.message || "Registration failed", "error");
        return false;
      }
    };

    const handleLogin = async (email, password) => {
      try {
        const result = await authAPI.login(email, password);
        if (result.access_token) {
          localStorage.setItem('mdm_token', result.access_token);
          setAuthToken(result.access_token);
          const userData = await authAPI.me(result.access_token);
          setUser({ ...userData, token: result.access_token });
          setIsAuthModalOpen(false);
          showNotification(`Welcome back, ${userData.name}!`);
          return true;
        }
      } catch (err) {
        showNotification("Invalid email or password", "error");
        return false;
      }
    };

    const handleLogout = () => {
      localStorage.removeItem('mdm_token');
      setAuthToken(null);
      setUser(null);
      setIsAdminOpen(false);
      showNotification("You've been logged out");
    };

    // Filtered and sorted products - FE-004: Now using products from API
    const filteredProducts = useMemo(() => {
      let filtered = products;

      if (selectedCategory !== "all") {
        filtered = filtered.filter(p => p.category === selectedCategory);
      }

      if (searchQuery) {
        const query = searchQuery.toLowerCase();
        filtered = filtered.filter(p =>
          p.name?.toLowerCase().includes(query) ||
          p.description?.toLowerCase().includes(query) ||
          p.tags?.some(tag => tag.toLowerCase().includes(query)) ||
          p.subcategory?.toLowerCase().includes(query)
        );
      }

      switch (sortBy) {
        case "price-low":
          filtered = [...filtered].sort((a, b) => a.price - b.price);
          break;
        case "price-high":
          filtered = [...filtered].sort((a, b) => b.price - a.price);
          break;
        case "rating":
          filtered = [...filtered].sort((a, b) => (b.rating || 0) - (a.rating || 0));
          break;
        case "featured":
        default:
          filtered = [...filtered].sort((a, b) => (b.featured ? 1 : 0) - (a.featured ? 1 : 0));
      }

      return filtered;
    }, [products, selectedCategory, searchQuery, sortBy]);

    // ============================================================================
    // RENDER
    // ============================================================================

    // Show Coming Soon page if under construction and user is not logged in
    if (UNDER_CONSTRUCTION && !user) {
      return (
        <ComingSoon
          onLogin={async (token) => {
            setAuthToken(token);
            try {
              const userData = await authAPI.me(token);
              setUser({ ...userData, token });
            } catch {
              localStorage.removeItem('mdm_token');
              setAuthToken(null);
            }
          }}
        />
      );
    }

    return (
      <div className="min-h-screen bg-zinc-950 text-zinc-100" style={{ fontFamily: "'Barlow', sans-serif" }}>
        {/* Google Fonts & Custom CSS */}
        <style>{`
          @import url('https://fonts.googleapis.com/css2?family=Bangers&family=Barlow:wght@400;500;600;700&display=swap');

          .font-comic { font-family: 'Bangers', cursive; letter-spacing: 0.05em; }

          .product-card {
            transition: all 0.3s cubic-bezier(0.4, 0, 0.2, 1);
          }

          .product-card:hover {
            transform: translateY(-8px) scale(1.02);
            box-shadow: 0 25px 50px -12px rgba(249, 115, 22, 0.25);
          }

          .pulse-glow {
            animation: pulseGlow 2s infinite;
          }

          @keyframes pulseGlow {
            0%, 100% { box-shadow: 0 0 20px rgba(249, 115, 22, 0.4); }
            50% { box-shadow: 0 0 40px rgba(249, 115, 22, 0.6); }
          }

          .slide-in {
            animation: slideIn 0.3s ease-out;
          }

          @keyframes slideIn {
            from { transform: translateX(100%); opacity: 0; }
            to { transform: translateX(0); opacity: 1; }
          }

          .fade-up {
            animation: fadeUp 0.5s ease-out forwards;
            opacity: 0;
          }

          @keyframes fadeUp {
            from { transform: translateY(20px); opacity: 0; }
            to { transform: translateY(0); opacity: 1; }
          }

          .notification-enter {
            animation: notificationEnter 0.3s ease-out;
          }

          @keyframes notificationEnter {
            from { transform: translateY(-100%); opacity: 0; }
            to { transform: translateY(0); opacity: 1; }
          }

          .hero-gradient {
            background: radial-gradient(ellipse at top, rgba(249, 115, 22, 0.12) 0%, transparent 50%),
                        radial-gradient(ellipse at bottom right, rgba(249, 115, 22, 0.06) 0%, transparent 40%);
          }
        `}</style>

        {/* Notification Toast */}
        {notification && (
          <div className={`fixed top-4 right-4 z-50 notification-enter px-6 py-3 rounded-lg shadow-xl ${
            notification.type === "error" ? "bg-red-600" : "bg-orange-500"
          } text-white font-semibold`}>
            {notification.message}
          </div>
        )}

        {/* Header */}
        <header className="sticky top-0 z-40 bg-zinc-950/95 backdrop-blur-md border-b border-zinc-800">
          <div className="max-w-7xl mx-auto px-4 py-4">
            <div className="flex items-center justify-between">
              {/* Logo */}
              <div
                className="flex items-center gap-3 cursor-pointer"
                onClick={() => setCurrentView("shop")}
              >
                <div className="w-12 h-12 bg-zinc-800 border border-orange-500/30 rounded-xl flex items-center justify-center shadow-lg shadow-orange-500/10">
                  <span className="font-comic text-2xl text-orange-500">M</span>
                </div>
                <div>
                  <h1 className="font-comic text-3xl text-orange-500">
                    MDM COMICS
                  </h1>
                  <p className="text-xs text-zinc-500 -mt-1">Comics • Collectibles • Intelligence</p>
                </div>
              </div>

              {/* Search Bar */}
              <div className="hidden md:flex flex-1 max-w-xl mx-8">
                <div className="relative w-full">
                  <Search className="absolute left-4 top-1/2 -translate-y-1/2 w-5 h-5 text-zinc-500" />
                  <input
                    type="text"
                    placeholder="Search comics, Funko POPs, characters..."
                    value={searchQuery}
                    onChange={(e) => setSearchQuery(e.target.value)}
                    className="w-full pl-12 pr-4 py-3 bg-zinc-900 border border-zinc-800 rounded-xl text-zinc-100 placeholder-zinc-500 focus:outline-none focus:border-orange-500
  focus:ring-2 focus:ring-orange-500/20 transition-all"
                  />
                  {searchQuery && (
                    <button
                      onClick={() => setSearchQuery("")}
                      className="absolute right-4 top-1/2 -translate-y-1/2 text-zinc-500 hover:text-zinc-300"
                    >
                      <X className="w-4 h-4" />
                    </button>
                  )}
                </div>
              </div>

              {/* User & Cart Buttons */}
              <div className="flex items-center gap-2">
                {/* User Button */}
                {user ? (
                  <div className="relative group">
                    <button className="flex items-center gap-2 px-3 py-2 bg-zinc-900 border border-zinc-800 rounded-xl hover:border-orange-500 transition-colors">
                      <div className="w-7 h-7 bg-orange-500 rounded-full flex items-center justify-center">
                        <span className="text-xs font-bold text-white">{user.name.charAt(0).toUpperCase()}</span>
                      </div>
                      <span className="text-sm text-zinc-300 hidden sm:block">{user.name.split(' ')[0]}</span>
                      <ChevronDown className="w-4 h-4 text-zinc-500" />
                    </button>
                    {/* Dropdown */}
                    <div className="absolute right-0 mt-2 w-48 bg-zinc-900 border border-zinc-800 rounded-xl shadow-xl opacity-0 invisible group-hover:opacity-100 group-hover:visible
  transition-all z-50">
                      <div className="p-3 border-b border-zinc-800">
                        <p className="text-sm font-semibold text-white flex items-center gap-2">
                          {user.name}
                          {user.is_admin && (
                            <span className="px-1.5 py-0.5 text-[10px] font-bold bg-red-500/20 text-red-400 rounded">ADMIN</span>
                          )}
                        </p>
                        <p className="text-xs text-zinc-500">{user.email}</p>
                      </div>
                      <button
                        onClick={handleLogout}
                        className="w-full flex items-center gap-2 px-3 py-2 text-sm text-zinc-400 hover:text-white hover:bg-zinc-800 transition-colors"
                      >
                        <LogOut className="w-4 h-4" />
                        Sign Out
                      </button>
                    </div>
                  </div>
                ) : (
                  <button
                    onClick={() => { setAuthMode("login"); setIsAuthModalOpen(true); }}
                    className="flex items-center gap-2 px-3 py-2 bg-zinc-900 border border-zinc-800 rounded-xl hover:border-orange-500 transition-colors group"
                  >
                    <User className="w-5 h-5 text-zinc-400 group-hover:text-orange-500 transition-colors" />
                    <span className="text-sm text-zinc-400 group-hover:text-orange-500 transition-colors hidden sm:block">Sign In</span>
                  </button>
                )}

                {/* Comic Database Search Button */}
                <button
                  onClick={() => setIsComicSearchOpen(true)}
                  className="relative p-3 bg-zinc-900 border border-zinc-800 rounded-xl hover:border-orange-500 transition-colors group"
                  title="Search Comic Database"
                >
                  <Database className="w-6 h-6 text-zinc-400 group-hover:text-orange-500 transition-colors" />
                </button>

                {/* Funko Database Search Button */}
                <button
                  onClick={() => setIsFunkoSearchOpen(true)}
                  className="relative p-3 bg-zinc-900 border border-zinc-800 rounded-xl hover:border-purple-500 transition-colors group"
                  title="Search Funko Database"
                >
                  <Package className="w-6 h-6 text-zinc-400 group-hover:text-purple-500 transition-colors" />
                </button>

                {/* Admin Console Button - Only for admins */}
                {user?.is_admin && (
                  <button
                    onClick={() => setIsAdminOpen(true)}
                    className="relative p-3 bg-zinc-900 border border-red-800 rounded-xl hover:border-red-500 transition-colors group"
                    title="Admin Console"
                  >
                    <Shield className="w-6 h-6 text-red-400 group-hover:text-red-500 transition-colors" />
                  </button>
                )}

                {/* Cart Button */}
                <button
                  onClick={() => setIsCartOpen(true)}
                  className="relative p-3 bg-zinc-900 border border-zinc-800 rounded-xl hover:border-orange-500 transition-colors group"
                >
                  <ShoppingCart className="w-6 h-6 text-zinc-400 group-hover:text-orange-500 transition-colors" />
                  {cartCount > 0 && (
                    <span className="absolute -top-2 -right-2 w-6 h-6 bg-orange-500 rounded-full flex items-center justify-center text-xs font-bold text-white pulse-glow">
                      {cartCount}
                    </span>
                  )}
                </button>
              </div>
            </div>

            {/* Mobile Search */}
            <div className="md:hidden mt-4">
              <div className="relative">
                <Search className="absolute left-4 top-1/2 -translate-y-1/2 w-5 h-5 text-zinc-500" />
                <input
                  type="text"
                  placeholder="Search..."
                  value={searchQuery}
                  onChange={(e) => setSearchQuery(e.target.value)}
                  className="w-full pl-12 pr-4 py-3 bg-zinc-900 border border-zinc-800 rounded-xl text-zinc-100 placeholder-zinc-500 focus:outline-none focus:border-orange-500
  transition-all"
                />
              </div>
            </div>
          </div>
        </header>

        {/* Main Content */}
        {currentView === "shop" && (
          <main className="hero-gradient">
            {/* Hero Section */}
            <section className="max-w-7xl mx-auto px-4 py-12">
              <div className="text-center mb-12 fade-up" style={{ animationDelay: "0.1s" }}>
                <h2 className="font-comic text-5xl md:text-7xl mb-4 text-white">
                  EVERY BOOK. <span className="text-orange-500">ANALYZED.</span>
                </h2>
                <p className="text-zinc-400 text-lg max-w-2xl mx-auto">
                  AI-powered CGC grade estimates on every ungraded book.
                  <br />
                  Premium comics and collectibles—zero guesswork.
                </p>
              </div>

              {/* Loading State */}
              {productsLoading && (
                <div className="flex flex-col items-center justify-center py-12">
                  <Loader2 className="w-12 h-12 text-orange-500 animate-spin mb-4" />
                  <p className="text-zinc-400">Loading products...</p>
                </div>
              )}

              {/* Error State */}
              {productsError && !productsLoading && (
                <div className="bg-red-500/10 border border-red-500/20 rounded-xl p-4 mb-8 text-center">
                  <p className="text-red-400 text-sm">Unable to load products. Showing cached data.</p>
                </div>
              )}

              {/* Comics Section */}
              {!productsLoading && (
                <section className="mb-12">
                  <div className="flex items-center justify-between mb-4">
                    <h3 className="font-comic text-2xl text-white flex items-center gap-2">
                      📚 COMIC BOOKS
                    </h3>
                    <button
                      onClick={() => { setSelectedCategory("comics"); setCurrentView("category"); }}
                      className="text-orange-500 hover:text-orange-400 text-sm font-semibold flex items-center gap-1 transition-colors"
                    >
                      See More →
                    </button>
                  </div>
                  <div className="grid grid-cols-4 gap-3">
                    {products.filter(p => p.category === "comics").slice(0, 4).map((product, index) => (
                      <ProductCard key={product.id} product={product} index={index} addToCart={addToCart} />
                    ))}
                  </div>
                </section>
              )}

              {/* Funko POPs Section */}
              {!productsLoading && (
                <section className="mb-12">
                  <div className="flex items-center justify-between mb-4">
                    <h3 className="font-comic text-2xl text-white flex items-center gap-2">
                      🎭 FUNKO POPS
                    </h3>
                    <button
                      onClick={() => { setSelectedCategory("funko"); setCurrentView("category"); }}
                      className="text-orange-500 hover:text-orange-400 text-sm font-semibold flex items-center gap-1 transition-colors"
                    >
                      See More →
                    </button>
                  </div>
                  <div className="grid grid-cols-4 gap-3">
                    {products.filter(p => p.category === "funko").slice(0, 4).map((product, index) => (
                      <ProductCard key={product.id} product={product} index={index} addToCart={addToCart} />
                    ))}
                  </div>
                </section>
              )}

            </section>

            {/* Features Section */}
            <section className="border-t border-zinc-800 bg-zinc-900/50">
              <div className="max-w-5xl mx-auto px-4 py-8">
                <div className="grid grid-cols-3 divide-x divide-zinc-800">
                  <div className="flex flex-col items-center text-center px-4">
                    <div className="w-12 h-12 bg-zinc-800 rounded-full flex items-center justify-center mb-2">
                      <Truck className="w-6 h-6 text-orange-500" />
                    </div>
                    <h4 className="font-bold text-white text-sm">Fast Shipping</h4>
                    <p className="text-zinc-500 text-xs">Free on orders $50+</p>
                  </div>
                  <div className="flex flex-col items-center text-center px-4">
                    <div className="w-12 h-12 bg-zinc-800 rounded-full flex items-center justify-center mb-2">
                      <Package className="w-6 h-6 text-orange-500" />
                    </div>
                    <h4 className="font-bold text-white text-sm">Secure Packaging</h4>
                    <p className="text-zinc-500 text-xs">Protective cases included</p>
                  </div>
                  <div className="flex flex-col items-center text-center px-4">
                    <div className="w-12 h-12 bg-zinc-800 rounded-full flex items-center justify-center mb-2">
                      <CreditCard className="w-6 h-6 text-orange-500" />
                    </div>
                    <h4 className="font-bold text-white text-sm">Secure Payment</h4>
                    <p className="text-zinc-500 text-xs">PayPal, Stripe & more</p>
                  </div>
                </div>
              </div>
            </section>
          </main>
        )}

        {/* Category View */}
        {currentView === "category" && (
          <main className="hero-gradient">
            <section className="max-w-7xl mx-auto px-4 py-12">
              {/* Back Button & Title */}
              <div className="mb-8">
                <button
                  onClick={() => setCurrentView("shop")}
                  className="text-orange-500 hover:text-orange-400 mb-4 flex items-center gap-2 transition-colors"
                >
                  ← Back to Home
                </button>
                <h2 className="font-comic text-4xl text-white">
                  {selectedCategory === "comics" ? "📚 COMIC BOOKS" : "🎭 FUNKO POPS"}
                </h2>
                <p className="text-zinc-500 mt-2">
                  {products.filter(p => p.category === selectedCategory).length} items
                </p>
              </div>

              {/* Sort */}
              <div className="flex justify-end mb-6">
                <div className="relative">
                  <select
                    value={sortBy}
                    onChange={(e) => setSortBy(e.target.value)}
                    className="appearance-none bg-zinc-900 border border-zinc-800 rounded-lg px-4 py-2 pr-10 text-zinc-300 focus:outline-none focus:border-orange-500 cursor-pointer"
                  >
                    <option value="featured">Featured</option>
                    <option value="price-low">Price: Low to High</option>
                    <option value="price-high">Price: High to Low</option>
                    <option value="rating">Top Rated</option>
                  </select>
                  <ChevronDown className="absolute right-3 top-1/2 -translate-y-1/2 w-4 h-4 text-zinc-500 pointer-events-none" />
                </div>
              </div>

              {/* Products Grid */}
              <div className="grid grid-cols-4 gap-3">
                {products
                  .filter(p => p.category === selectedCategory)
                  .sort((a, b) => {
                    switch (sortBy) {
                      case "price-low": return a.price - b.price;
                      case "price-high": return b.price - a.price;
                      case "rating": return b.rating - a.rating;
                      default: return (b.featured ? 1 : 0) - (a.featured ? 1 : 0);
                    }
                  })
                  .map((product, index) => (
                    <ProductCard key={product.id} product={product} index={index} addToCart={addToCart} />
                  ))
                }
              </div>
            </section>
          </main>
        )}

        {/* Checkout View */}
        {currentView === "checkout" && (
          <main className="max-w-4xl mx-auto px-4 py-12">
            {completedOrder ? (
              <OrderSuccess
                order={completedOrder}
                onClose={() => {
                  setCompletedOrder(null);
                  setCart([]);
                  setCurrentView("shop");
                }}
              />
            ) : (
              <>
                <button
                  onClick={() => setCurrentView("shop")}
                  className="text-orange-500 hover:text-orange-400 mb-8 flex items-center gap-2"
                >
                  ← Back to Shop
                </button>

                <h2 className="font-comic text-4xl text-white mb-8">CHECKOUT</h2>

                {!user ? (
                  <div className="bg-zinc-900 rounded-2xl p-8 border border-zinc-800 text-center">
                    <p className="text-zinc-400 mb-4">Please sign in to complete your purchase</p>
                    <button
                      onClick={() => setIsAuthModalOpen(true)}
                      className="px-6 py-3 bg-orange-500 text-white rounded-xl font-semibold hover:bg-orange-600"
                    >
                      Sign In
                    </button>
                  </div>
                ) : cart.length === 0 ? (
                  <div className="bg-zinc-900 rounded-2xl p-8 border border-zinc-800 text-center">
                    <p className="text-zinc-400 mb-4">Your cart is empty</p>
                    <button
                      onClick={() => setCurrentView("shop")}
                      className="px-6 py-3 bg-orange-500 text-white rounded-xl font-semibold hover:bg-orange-600"
                    >
                      Continue Shopping
                    </button>
                  </div>
                ) : (
                  <div className="grid md:grid-cols-2 gap-8">
                    {/* Order Summary */}
                    <div className="bg-zinc-900 rounded-2xl p-6 border border-zinc-800">
                      <h3 className="font-bold text-xl text-white mb-4">Order Summary</h3>
                      <div className="space-y-4 mb-6">
                        {cart.map((item) => (
                          <div key={item.id} className="flex items-center gap-4">
                            <img src={item.image} alt={item.name} className="w-16 h-16 object-cover rounded-lg" />
                            <div className="flex-1">
                              <p className="text-white font-semibold">{item.name}</p>
                              <p className="text-zinc-500 text-sm">Qty: {item.quantity}</p>
                            </div>
                            <p className="text-orange-500 font-bold">${(item.price * item.quantity).toFixed(2)}</p>
                          </div>
                        ))}
                      </div>
                      <div className="border-t border-zinc-800 pt-4 space-y-2">
                        <div className="flex justify-between text-zinc-400">
                          <span>Subtotal</span>
                          <span>${cartTotal.toFixed(2)}</span>
                        </div>
                        <div className="flex justify-between text-zinc-400">
                          <span>Shipping</span>
                          <span>{cartTotal >= 50 ? "FREE" : "$5.99"}</span>
                        </div>
                        <div className="flex justify-between text-xl font-bold text-white pt-2 border-t border-zinc-800">
                          <span>Total</span>
                          <span>${(cartTotal + (cartTotal >= 50 ? 0 : 5.99)).toFixed(2)}</span>
                        </div>
                      </div>
                    </div>

                    {/* Payment Form */}
                    <div>
                      <CheckoutForm
                        token={authToken}
                        cartItems={cart}
                        total={cartTotal + (cartTotal >= 50 ? 0 : 5.99)}
                        onSuccess={(order) => {
                          setCompletedOrder(order);
                          showNotification("Order placed successfully!", "success");
                        }}
                        onCancel={() => setCurrentView("shop")}
                      />
                    </div>
                  </div>
                )}
              </>
            )}
          </main>
        )}

        {/* Cart Sidebar */}
        {isCartOpen && (
          <div className="fixed inset-0 z-50">
            {/* Overlay */}
            <div
              className="absolute inset-0 bg-black/60 backdrop-blur-sm"
              onClick={() => setIsCartOpen(false)}
            />

            {/* Cart Panel */}
            <div className="absolute right-0 top-0 h-full w-full max-w-md bg-zinc-950 border-l border-zinc-800 slide-in">
              <div className="flex flex-col h-full">
                {/* Header */}
                <div className="flex items-center justify-between p-6 border-b border-zinc-800">
                  <h3 className="font-comic text-2xl text-white">YOUR CART</h3>
                  <button
                    onClick={() => setIsCartOpen(false)}
                    className="p-2 hover:bg-zinc-800 rounded-lg transition-colors"
                  >
                    <X className="w-6 h-6 text-zinc-400" />
                  </button>
                </div>

                {/* Cart Items */}
                <div className="flex-1 overflow-auto p-6">
                  {cart.length === 0 ? (
                    <div className="text-center py-12">
                      <ShoppingCart className="w-16 h-16 text-zinc-700 mx-auto mb-4" />
                      <p className="text-zinc-500">Your cart is empty</p>
                      <button
                        onClick={() => setIsCartOpen(false)}
                        className="mt-4 text-orange-500 hover:text-orange-400 font-semibold"
                      >
                        Continue Shopping
                      </button>
                    </div>
                  ) : (
                    <div className="space-y-4">
                      {cart.map((item) => (
                        <div
                          key={item.id}
                          className="flex gap-4 bg-zinc-900 rounded-xl p-4 border border-zinc-800"
                        >
                          <img
                            src={item.image}
                            alt={item.name}
                            className="w-20 h-24 object-cover rounded-lg"
                          />
                          <div className="flex-1">
                            <h4 className="font-semibold text-white">{item.name}</h4>
                            <p className="text-orange-500 font-bold">${item.price}</p>
                            <div className="flex items-center gap-3 mt-2">
                              <button
                                onClick={() => updateQuantity(item.id, item.quantity - 1)}
                                className="p-1 bg-zinc-800 rounded hover:bg-zinc-700 transition-colors"
                              >
                                <Minus className="w-4 h-4 text-zinc-400" />
                              </button>
                              <span className="text-white font-semibold">{item.quantity}</span>
                              <button
                                onClick={() => updateQuantity(item.id, item.quantity + 1)}
                                className="p-1 bg-zinc-800 rounded hover:bg-zinc-700 transition-colors"
                              >
                                <Plus className="w-4 h-4 text-zinc-400" />
                              </button>
                              <button
                                onClick={() => removeFromCart(item.id)}
                                className="ml-auto p-1 text-red-500 hover:bg-red-500/10 rounded transition-colors"
                              >
                                <Trash2 className="w-4 h-4" />
                              </button>
                            </div>
                          </div>
                        </div>
                      ))}
                    </div>
                  )}
                </div>

                {/* Footer */}
                {cart.length > 0 && (
                  <div className="p-6 border-t border-zinc-800">
                    <div className="flex justify-between mb-4">
                      <span className="text-zinc-400">Subtotal</span>
                      <span className="text-2xl font-bold text-white">${cartTotal.toFixed(2)}</span>
                    </div>
                    <button
                      onClick={() => { setIsCartOpen(false); setCurrentView("checkout"); }}
                      className="w-full py-4 bg-orange-500 rounded-xl font-bold text-white hover:shadow-lg hover:shadow-orange-500/25 transition-all active:scale-[0.98]"
                    >
                      Proceed to Checkout
                    </button>
                    <p className="text-zinc-500 text-xs text-center mt-3">
                      Free shipping on orders over $50
                    </p>
                  </div>
                )}
              </div>
            </div>
          </div>
        )}

        {/* Comic Database Search Modal */}
        {isComicSearchOpen && (
          <ComicSearch
            onClose={() => setIsComicSearchOpen(false)}
            onSelectComic={(comic) => {
              console.log("Selected comic:", comic);
              setIsComicSearchOpen(false);
            }}
          />
        )}

        {/* Funko Database Search Modal */}
        {isFunkoSearchOpen && (
          <FunkoSearch
            onClose={() => setIsFunkoSearchOpen(false)}
            onSelectFunko={(funko) => {
              console.log("Selected funko:", funko);
              setIsFunkoSearchOpen(false);
            }}
          />
        )}

        {/* Admin Console Modal */}
        {isAdminOpen && (
          <AdminConsole
            onClose={() => setIsAdminOpen(false)}
            token={user?.token}
          />
        )}

        {/* Auth Modal */}
        {isAuthModalOpen && (
          <AuthModal
            mode={authMode}
            setMode={setAuthMode}
            onClose={() => setIsAuthModalOpen(false)}
            onLogin={handleLogin}
            onSignup={handleSignup}
          />
        )}

        {/* Footer */}
        <footer className="bg-zinc-900 border-t border-zinc-800">
          <div className="max-w-7xl mx-auto px-4 py-8">
            <div className="flex flex-col items-center gap-4">
              <div className="flex gap-6 text-zinc-500 text-sm">
                <a href="#" className="hover:text-orange-500 transition-colors">About</a>
                <a href="#" className="hover:text-orange-500 transition-colors">Contact</a>
                <a href="#" className="hover:text-orange-500 transition-colors">Shipping</a>
                <a href="#" className="hover:text-orange-500 transition-colors">Returns</a>
                <a href="#" className="hover:text-orange-500 transition-colors">FAQ</a>
              </div>

              {/* Build Info */}
              <div className="text-xs text-zinc-600">
                <p>v{BUILD_INFO.version}.{BUILD_INFO.buildNumber} • Built {new Date(BUILD_INFO.buildDate).toLocaleDateString()}</p>
              </div>
            </div>

            <div className="mt-8 pt-6 border-t border-zinc-800 text-center text-zinc-600 text-sm">
              <p>© {new Date().getFullYear()} MDM Comics. All rights reserved.</p>
            </div>
          </div>
        </footer>
      </div>
    );
  }