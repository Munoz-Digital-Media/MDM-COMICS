import React, { useState, memo } from "react";
import { X, Eye, EyeOff } from "lucide-react";

/**
 * AuthModal component - extracted from App.jsx to prevent form state resets.
 *
 * ISSUE: When AuthModal was defined inside App(), every App re-render created a
 * new component identity, unmounting the previous AuthModal and resetting form state.
 *
 * FIX: Extract as standalone memoized component. Now form state persists across
 * unrelated parent re-renders (cart updates, notifications, etc.).
 *
 * Per constitution_ui.json §6: "Every action returns feedback state; critical flows persist"
 */
const AuthModal = memo(function AuthModal({ mode, setMode, onClose, onLogin, onSignup }) {
  const [name, setName] = useState("");
  const [email, setEmail] = useState("");
  const [password, setPassword] = useState("");
  const [confirmPassword, setConfirmPassword] = useState("");
  const [showPassword, setShowPassword] = useState(false);
  const [errors, setErrors] = useState({});
  const [isLoading, setIsLoading] = useState(false);

  const validateEmail = (email) => {
    return /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email);
  };

  const handleSubmit = async (e) => {
    e.preventDefault();
    const newErrors = {};

    if (mode === "signup") {
      if (!name.trim()) newErrors.name = "Name is required";
      if (!validateEmail(email)) newErrors.email = "Valid email is required";
      if (password.length < 6) newErrors.password = "Password must be at least 6 characters";
      if (password !== confirmPassword) newErrors.confirmPassword = "Passwords don't match";

      if (Object.keys(newErrors).length === 0) {
        setIsLoading(true);
        try {
          await onSignup(name.trim(), email.trim(), password);
        } finally {
          setIsLoading(false);
        }
      }
    } else {
      if (!email.trim()) newErrors.email = "Email is required";
      if (!password) newErrors.password = "Password is required";

      if (Object.keys(newErrors).length === 0) {
        setIsLoading(true);
        try {
          await onLogin(email.trim(), password);
        } finally {
          setIsLoading(false);
        }
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
                  disabled={isLoading}
                  className={`w-full px-4 py-3 bg-zinc-800 border rounded-xl text-white placeholder-zinc-500 focus:outline-none focus:border-orange-500 transition-colors disabled:opacity-50 ${
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
                disabled={isLoading}
                className={`w-full px-4 py-3 bg-zinc-800 border rounded-xl text-white placeholder-zinc-500 focus:outline-none focus:border-orange-500 transition-colors disabled:opacity-50 ${
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
                  disabled={isLoading}
                  className={`w-full px-4 py-3 pr-12 bg-zinc-800 border rounded-xl text-white placeholder-zinc-500 focus:outline-none focus:border-orange-500 transition-colors disabled:opacity-50 ${
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
                  disabled={isLoading}
                  className={`w-full px-4 py-3 bg-zinc-800 border rounded-xl text-white placeholder-zinc-500 focus:outline-none focus:border-orange-500 transition-colors disabled:opacity-50 ${
                    errors.confirmPassword ? "border-red-500" : "border-zinc-700"
                  }`}
                />
                {errors.confirmPassword && <p className="text-red-500 text-xs mt-1">{errors.confirmPassword}</p>}
              </div>
            )}

            <button
              type="submit"
              disabled={isLoading}
              className="w-full py-4 bg-orange-500 rounded-xl font-bold text-white hover:shadow-lg hover:shadow-orange-500/25 transition-all active:scale-[0.98] disabled:opacity-50 disabled:cursor-not-allowed"
            >
              {isLoading ? (
                <span className="flex items-center justify-center gap-2">
                  <svg className="animate-spin h-5 w-5" viewBox="0 0 24 24">
                    <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4" fill="none" />
                    <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z" />
                  </svg>
                  {mode === "login" ? "Signing In..." : "Creating Account..."}
                </span>
              ) : (
                mode === "login" ? "Sign In" : "Create Account"
              )}
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
                disabled={isLoading}
                className="text-orange-500 hover:text-orange-400 font-semibold transition-colors disabled:opacity-50"
              >
                {mode === "login" ? "Sign Up" : "Sign In"}
              </button>
            </p>
          </div>
        </form>
      </div>
    </div>
  );
});

export default AuthModal;
