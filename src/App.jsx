import React, { useState, useMemo, useEffect } from "react";
  import { ShoppingCart, Search, X, Plus, Minus, Trash2, ChevronDown, Star, Package, CreditCard, Truck, User, LogOut, Eye, EyeOff, Database, Shield } from "lucide-react";
  import { authAPI } from "./services/api";
  import ComicSearch from "./components/ComicSearch";
  import AdminConsole from "./components/AdminConsole";

  // ============================================================================
  // BUILD INFO - Update these on each release
  // ============================================================================
  const BUILD_INFO = {
    version: "1.3.0",
    buildNumber: 13,
    buildDate: new Date().toISOString(),
    environment: "development"
  };

  // ============================================================================
  // PRODUCT DATA - Replace with API calls when backend is ready
  // ============================================================================
  const PRODUCTS = [
    {
      id: "comic-001",
      name: "Amazing Spider-Man #300",
      category: "comics",
      subcategory: "Marvel",
      price: 299.99,
      originalPrice: 349.99,
      image: "https://placehold.co/400x500/1a1a2e/f59e0b",
      description: "First appearance of Venom. Near Mint condition.",
      stock: 2,
      featured: true,
      rating: 4.9,
      tags: ["key-issue", "first-appearance", "graded"]
    },
    {
      id: "comic-002",
      name: "Batman: The Killing Joke",
      category: "comics",
      subcategory: "DC",
      price: 89.99,
      image: "https://placehold.co/400x500/1a1a2e/a855f7",
      description: "Classic Alan Moore story. First print.",
      stock: 5,
      featured: true,
      rating: 4.8,
      tags: ["classic", "alan-moore"]
    },
    {
      id: "comic-003",
      name: "X-Men #1 (1991)",
      category: "comics",
      subcategory: "Marvel",
      price: 45.99,
      image: "https://placehold.co/400x500/1a1a2e/ef4444",
      description: "Jim Lee cover. Multiple variants available.",
      stock: 12,
      featured: false,
      rating: 4.5,
      tags: ["jim-lee", "variant"]
    },
    {
      id: "funko-001",
      name: "Funko POP! Spider-Man (Black Suit)",
      category: "funko",
      subcategory: "Marvel",
      price: 14.99,
      image: "https://placehold.co/400x500/27272a/f59e0b",
      description: "Exclusive black suit variant. #79",
      stock: 25,
      featured: true,
      rating: 4.7,
      tags: ["exclusive", "marvel"]
    },
    {
      id: "funko-002",
      name: "Funko POP! Batman (GITD)",
      category: "funko",
      subcategory: "DC",
      price: 24.99,
      originalPrice: 29.99,
      image: "https://placehold.co/400x500/27272a/22c55e",
      description: "Glow in the dark exclusive. Chase variant.",
      stock: 3,
      featured: true,
      rating: 4.9,
      tags: ["gitd", "chase", "exclusive"]
    },
    {
      id: "funko-003",
      name: "Funko POP! Deadpool (Chef)",
      category: "funko",
      subcategory: "Marvel",
      price: 12.99,
      image: "https://placehold.co/400x500/27272a/ef4444",
      description: "Deadpool in chef outfit. Standard edition.",
      stock: 50,
      featured: false,
      rating: 4.3,
      tags: ["deadpool", "standard"]
    },
    {
      id: "comic-004",
      name: "Watchmen TPB",
      category: "comics",
      subcategory: "DC",
      price: 24.99,
      image: "https://placehold.co/400x500/1a1a2e/eab308",
      description: "Complete collected edition. Alan Moore classic.",
      stock: 8,
      featured: false,
      rating: 5.0,
      tags: ["tpb", "alan-moore", "classic"]
    },
    {
      id: "funko-004",
      name: "Funko POP! Joker (Dark Knight)",
      category: "funko",
      subcategory: "DC",
      price: 34.99,
      image: "https://placehold.co/400x500/27272a/a855f7",
      description: "Heath Ledger tribute. Limited edition.",
      stock: 7,
      featured: true,
      rating: 4.8,
      tags: ["limited", "movies", "heath-ledger"]
    }
  ];

  const CATEGORIES = [
    { id: "all", name: "All Products", icon: "🏪" },
    { id: "comics", name: "Comic Books", icon: "📚" },
    { id: "funko", name: "Funko POPs", icon: "🎭" }
  ];

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
  // AUTH MODAL COMPONENT
  // ============================================================================
  const AuthModal = ({ mode, setMode, onClose, onLogin, onSignup }) => {
    const [name, setName] = useState("");
    const [email, setEmail] = useState("");
    const [password, setPassword] = useState("");
    const [confirmPassword, setConfirmPassword] = useState("");
    const [showPassword, setShowPassword] = useState(false);
    const [errors, setErrors] = useState({});

    const validateEmail = (email) => {
      return /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email);
    };

    const handleSubmit = (e) => {
      e.preventDefault();
      const newErrors = {};

      if (mode === "signup") {
        if (!name.trim()) newErrors.name = "Name is required";
        if (!validateEmail(email)) newErrors.email = "Valid email is required";
        if (password.length < 6) newErrors.password = "Password must be at least 6 characters";
        if (password !== confirmPassword) newErrors.confirmPassword = "Passwords don't match";

        if (Object.keys(newErrors).length === 0) {
          onSignup(name.trim(), email.trim(), password);
        }
      } else {
        if (!email.trim()) newErrors.email = "Email is required";
        if (!password) newErrors.password = "Password is required";

        if (Object.keys(newErrors).length === 0) {
          onLogin(email.trim(), password);
        }
      }

      setErrors(newErrors);
    };

    const switchMode = () => {
      setMode(mode === "login" ? "signup" : "login");
      setErrors({});
      setPassword("");
      setConfirmPassword("");
    };

    return (
      <div className="fixed inset-0 z-50 flex items-center justify-center">
        {/* Overlay */}
        <div
          className="absolute inset-0 bg-black/70 backdrop-blur-sm"
          onClick={onClose}
        />

        {/* Modal */}
        <div className="relative bg-zinc-900 rounded-2xl border border-zinc-800 w-full max-w-md mx-4 slide-in shadow-2xl">
          {/* Header */}
          <div className="flex items-center justify-between p-6 border-b border-zinc-800">
            <h3 className="font-comic text-2xl text-white">
              {mode === "login" ? "WELCOME BACK" : "JOIN THE CREW"}
            </h3>
            <button
              onClick={onClose}
              className="p-2 hover:bg-zinc-800 rounded-lg transition-colors"
            >
              <X className="w-5 h-5 text-zinc-400" />
            </button>
          </div>

          {/* Form */}
          <form className="p-6" onSubmit={handleSubmit}>
            <div className="space-y-4">
              {mode === "signup" && (
                <div>
                  <label className="block text-sm text-zinc-400 mb-1">Name</label>
                  <input
                    type="text"
                    value={name}
                    onChange={(e) => setName(e.target.value)}
                    placeholder="Your name"
                    className={`w-full px-4 py-3 bg-zinc-800 border rounded-xl text-white placeholder-zinc-500 focus:outline-none focus:border-orange-500 transition-colors ${
                      errors.name ? "border-red-500" : "border-zinc-700"
                    }`}
                  />
                  {errors.name && <p className="text-red-500 text-xs mt-1">{errors.name}</p>}
                </div>
              )}

              <div>
                <label className="block text-sm text-zinc-400 mb-1">Email</label>
                <input
                  type="email"
                  value={email}
                  onChange={(e) => setEmail(e.target.value)}
                  placeholder="you@example.com"
                  className={`w-full px-4 py-3 bg-zinc-800 border rounded-xl text-white placeholder-zinc-500 focus:outline-none focus:border-orange-500 transition-colors ${
                    errors.email ? "border-red-500" : "border-zinc-700"
                  }`}
                />
                {errors.email && <p className="text-red-500 text-xs mt-1">{errors.email}</p>}
              </div>

              <div>
                <label className="block text-sm text-zinc-400 mb-1">Password</label>
                <div className="relative">
                  <input
                    type={showPassword ? "text" : "password"}
                    value={password}
                    onChange={(e) => setPassword(e.target.value)}
                    placeholder="••••••••"
                    className={`w-full px-4 py-3 pr-12 bg-zinc-800 border rounded-xl text-white placeholder-zinc-500 focus:outline-none focus:border-orange-500 transition-colors ${
                      errors.password ? "border-red-500" : "border-zinc-700"
                    }`}
                  />
                  <button
                    type="button"
                    onClick={() => setShowPassword(!showPassword)}
                    className="absolute right-4 top-1/2 -translate-y-1/2 text-zinc-500 hover:text-zinc-300"
                  >
                    {showPassword ? <EyeOff className="w-5 h-5" /> : <Eye className="w-5 h-5" />}
                  </button>
                </div>
                {errors.password && <p className="text-red-500 text-xs mt-1">{errors.password}</p>}
              </div>

              {mode === "signup" && (
                <div>
                  <label className="block text-sm text-zinc-400 mb-1">Confirm Password</label>
                  <input
                    type={showPassword ? "text" : "password"}
                    value={confirmPassword}
                    onChange={(e) => setConfirmPassword(e.target.value)}
                    placeholder="••••••••"
                    className={`w-full px-4 py-3 bg-zinc-800 border rounded-xl text-white placeholder-zinc-500 focus:outline-none focus:border-orange-500 transition-colors ${
                      errors.confirmPassword ? "border-red-500" : "border-zinc-700"
                    }`}
                  />
                  {errors.confirmPassword && <p className="text-red-500 text-xs mt-1">{errors.confirmPassword}</p>}
                </div>
              )}

              <button
                type="submit"
                className="w-full py-4 bg-orange-500 rounded-xl font-bold text-white hover:shadow-lg hover:shadow-orange-500/25 transition-all active:scale-[0.98]"
              >
                {mode === "login" ? "Sign In" : "Create Account"}
              </button>
            </div>

            {/* Demo credentials hint for login */}
            {mode === "login" && (
              <div className="mt-4 p-3 bg-zinc-800/50 rounded-lg border border-zinc-700">
                <p className="text-xs text-zinc-500 text-center">
                  <span className="text-orange-500">Demo:</span> demo@mdmcomics.com / demo123
                </p>
              </div>
            )}

            {/* Switch mode */}
            <div className="mt-6 text-center">
              <p className="text-zinc-500 text-sm">
                {mode === "login" ? "Don't have an account?" : "Already have an account?"}{" "}
                <button
                  type="button"
                  onClick={switchMode}
                  className="text-orange-500 hover:text-orange-400 font-semibold transition-colors"
                >
                  {mode === "login" ? "Sign Up" : "Sign In"}
                </button>
              </p>
            </div>
          </form>
        </div>
      </div>
    );
  };

  // ============================================================================
  // MAIN APP COMPONENT
  // ============================================================================
  export default function App() {
    // State management
    const [cart, setCart] = useState([]);
    const [isCartOpen, setIsCartOpen] = useState(false);
    const [searchQuery, setSearchQuery] = useState("");
    const [selectedCategory, setSelectedCategory] = useState("all");
    const [sortBy, setSortBy] = useState("featured");
    const [currentView, setCurrentView] = useState("shop");
    const [notification, setNotification] = useState(null);

    // Auth state
    const [user, setUser] = useState(null);
    const [authToken, setAuthToken] = useState(localStorage.getItem('mdm_token'));
    const [isAuthModalOpen, setIsAuthModalOpen] = useState(false);
    const [isComicSearchOpen, setIsComicSearchOpen] = useState(false);
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

    // Cart operations
    const addToCart = (product) => {
      const existing = cart.find(item => item.id === product.id);
      if (existing) {
        if (existing.quantity < product.stock) {
          setCart(cart.map(item =>
            item.id === product.id
              ? { ...item, quantity: item.quantity + 1 }
              : item
          ));
          showNotification(`Added another ${product.name} to cart`);
        } else {
          showNotification("Max stock reached", "error");
        }
      } else {
        setCart([...cart, { ...product, quantity: 1 }]);
        showNotification(`${product.name} added to cart`);
      }
    };

    const updateQuantity = (productId, newQuantity) => {
      if (newQuantity <= 0) {
        removeFromCart(productId);
        return;
      }
      const product = PRODUCTS.find(p => p.id === productId);
      if (newQuantity > product.stock) return;

      setCart(cart.map(item =>
        item.id === productId ? { ...item, quantity: newQuantity } : item
      ));
    };

    const removeFromCart = (productId) => {
      setCart(cart.filter(item => item.id !== productId));
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

    // Filtered and sorted products
    const filteredProducts = useMemo(() => {
      let filtered = PRODUCTS;

      if (selectedCategory !== "all") {
        filtered = filtered.filter(p => p.category === selectedCategory);
      }

      if (searchQuery) {
        const query = searchQuery.toLowerCase();
        filtered = filtered.filter(p =>
          p.name.toLowerCase().includes(query) ||
          p.description.toLowerCase().includes(query) ||
          p.tags.some(tag => tag.toLowerCase().includes(query)) ||
          p.subcategory.toLowerCase().includes(query)
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
          filtered = [...filtered].sort((a, b) => b.rating - a.rating);
          break;
        case "featured":
        default:
          filtered = [...filtered].sort((a, b) => (b.featured ? 1 : 0) - (a.featured ? 1 : 0));
      }

      return filtered;
    }, [selectedCategory, searchQuery, sortBy]);

    // ============================================================================
    // RENDER
    // ============================================================================
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

              {/* Comics Section */}
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
                  {PRODUCTS.filter(p => p.category === "comics").slice(0, 4).map((product, index) => (
                    <ProductCard key={product.id} product={product} index={index} addToCart={addToCart} />
                  ))}
                </div>
              </section>

              {/* Funko POPs Section */}
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
                  {PRODUCTS.filter(p => p.category === "funko").slice(0, 4).map((product, index) => (
                    <ProductCard key={product.id} product={product} index={index} addToCart={addToCart} />
                  ))}
                </div>
              </section>

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
                  {PRODUCTS.filter(p => p.category === selectedCategory).length} items
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
                {PRODUCTS
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
            <button
              onClick={() => setCurrentView("shop")}
              className="text-orange-500 hover:text-orange-400 mb-8 flex items-center gap-2"
            >
              ← Back to Shop
            </button>

            <h2 className="font-comic text-4xl text-white mb-8">CHECKOUT</h2>

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

              {/* Payment Options */}
              <div className="bg-zinc-900 rounded-2xl p-6 border border-zinc-800">
                <h3 className="font-bold text-xl text-white mb-4">Payment Method</h3>
                <div className="space-y-3">
                  <button className="w-full p-4 bg-[#0070ba] rounded-xl text-white font-bold hover:bg-[#005ea6] transition-colors">
                    Pay with PayPal
                  </button>
                  <button className="w-full p-4 bg-gradient-to-r from-purple-600 to-indigo-600 rounded-xl text-white font-bold hover:opacity-90 transition-opacity">
                    Pay with Card (Stripe)
                  </button>
                  <div className="flex gap-3">
                    <button className="flex-1 p-4 bg-black border border-zinc-700 rounded-xl text-white font-bold hover:bg-zinc-900 transition-colors">
                      Apple Pay
                    </button>
                    <button className="flex-1 p-4 bg-white rounded-xl text-black font-bold hover:bg-zinc-200 transition-colors">
                      Google Pay
                    </button>
                  </div>
                </div>
                <p className="text-zinc-500 text-xs mt-4 text-center">
                  🔒 Your payment information is encrypted and secure
                </p>
              </div>
            </div>
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