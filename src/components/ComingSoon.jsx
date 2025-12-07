import React, { useState, useEffect } from 'react';
import { Instagram, Twitter, Facebook, Youtube, Mail, Lock, Eye, EyeOff, AlertCircle } from 'lucide-react';
import { authAPI } from '../services/api';

/**
 * ComingSoon page with login capability
 *
 * P1-5: Updated for cookie-based auth
 * - Cookies are set automatically by login response
 * - No more localStorage token storage
 * - authAPI.me() uses cookies automatically
 */
export default function ComingSoon({ onLogin }) {
  const [showLogin, setShowLogin] = useState(false);
  const [email, setEmail] = useState('');
  const [password, setPassword] = useState('');
  const [showPassword, setShowPassword] = useState(false);
  const [error, setError] = useState('');
  const [loading, setLoading] = useState(false);

  // P1-5: Check if user already has valid session cookie on mount
  useEffect(() => {
    authAPI.me()
      .then(() => {
        // Valid session exists, trigger login callback
        onLogin();
      })
      .catch(() => {
        // No valid session - stay on coming soon page
      });
  }, [onLogin]);

  const handleSubmit = async (e) => {
    e.preventDefault();
    setError('');
    setLoading(true);

    try {
      await authAPI.login(email, password);
      // P1-5: Cookies are set automatically by the login response
      // Just call onLogin to trigger user data fetch in parent
      onLogin();
    } catch (err) {
      setError('Invalid credentials');
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="min-h-screen bg-zinc-950 flex flex-col items-center justify-center relative overflow-hidden">
      {/* Background Effects */}
      <style>{`
        @import url('https://fonts.googleapis.com/css2?family=Bangers&family=Barlow:wght@400;500;600;700&display=swap');

        .font-comic { font-family: 'Bangers', cursive; letter-spacing: 0.05em; }

        .glow-pulse {
          animation: glowPulse 3s ease-in-out infinite;
        }

        @keyframes glowPulse {
          0%, 100% {
            filter: drop-shadow(0 0 20px rgba(249, 115, 22, 0.4));
          }
          50% {
            filter: drop-shadow(0 0 40px rgba(249, 115, 22, 0.7));
          }
        }

        .circuit-bg {
          background-image:
            radial-gradient(circle at 25% 25%, rgba(249, 115, 22, 0.03) 0%, transparent 50%),
            radial-gradient(circle at 75% 75%, rgba(249, 115, 22, 0.03) 0%, transparent 50%);
        }

        .fade-in {
          animation: fadeIn 1s ease-out forwards;
        }

        @keyframes fadeIn {
          from { opacity: 0; transform: translateY(20px); }
          to { opacity: 1; transform: translateY(0); }
        }

        .social-icon {
          transition: all 0.3s ease;
        }

        .social-icon:hover {
          transform: translateY(-4px);
          filter: drop-shadow(0 4px 12px rgba(249, 115, 22, 0.4));
        }
      `}</style>

      {/* Circuit pattern overlay */}
      <div className="absolute inset-0 circuit-bg" />

      {/* Gradient orbs */}
      <div className="absolute top-1/4 left-1/4 w-96 h-96 bg-orange-500/5 rounded-full blur-3xl" />
      <div className="absolute bottom-1/4 right-1/4 w-96 h-96 bg-orange-500/5 rounded-full blur-3xl" />

      {/* Main Content */}
      <div className="relative z-10 text-center px-6 max-w-2xl fade-in">
        {/* Logo */}
        <div className="mb-8">
          <img
            src="/mdm-logo.png"
            alt="MDM Comics"
            className="w-32 h-32 mx-auto glow-pulse"
          />
        </div>

        {/* Brand Name */}
        <h1 className="font-comic text-6xl md:text-7xl text-orange-500 mb-4 tracking-wider">
          MDM COMICS
        </h1>

        {/* Tagline */}
        <p className="text-zinc-400 text-lg md:text-xl mb-2">
          Comics • Collectibles • Supplies
        </p>

        {/* Coming Soon */}
        <div className="my-12">
          <h2 className="font-comic text-4xl md:text-5xl text-white mb-4">
            COMING SOON
          </h2>
          <p className="text-zinc-500 text-base max-w-md mx-auto">
            Slabs for the serious. Back issues for the curious. Funkos for everyone!
          </p>
        </div>

        {/* Social Icons */}
        <div className="flex items-center justify-center gap-6 mb-12">
          <button className="social-icon p-3 bg-zinc-900 border border-zinc-800 rounded-xl text-zinc-400 hover:text-orange-500 hover:border-orange-500/50">
            <Instagram className="w-6 h-6" />
          </button>
          <button className="social-icon p-3 bg-zinc-900 border border-zinc-800 rounded-xl text-zinc-400 hover:text-orange-500 hover:border-orange-500/50">
            <Twitter className="w-6 h-6" />
          </button>
          <button className="social-icon p-3 bg-zinc-900 border border-zinc-800 rounded-xl text-zinc-400 hover:text-orange-500 hover:border-orange-500/50">
            <Facebook className="w-6 h-6" />
          </button>
          <button className="social-icon p-3 bg-zinc-900 border border-zinc-800 rounded-xl text-zinc-400 hover:text-orange-500 hover:border-orange-500/50">
            <Youtube className="w-6 h-6" />
          </button>
        </div>

        {/* Login Toggle - FE-001: More visible sign in option */}
        {!showLogin ? (
          <button
            onClick={() => setShowLogin(true)}
            className="inline-flex items-center gap-2 px-6 py-3 bg-zinc-900/60 border border-zinc-800 rounded-xl text-zinc-400 hover:text-orange-500 hover:border-orange-500/50 transition-all"
          >
            <Lock className="w-4 h-4" />
            <span>Sign In</span>
          </button>
        ) : (
          <div className="max-w-sm mx-auto bg-zinc-900/80 backdrop-blur border border-zinc-800 rounded-2xl p-6 fade-in">
            <form onSubmit={handleSubmit} className="space-y-4">
              <div>
                <input
                  type="email"
                  value={email}
                  onChange={(e) => setEmail(e.target.value)}
                  placeholder="Email"
                  className="w-full px-4 py-3 bg-zinc-800 border border-zinc-700 rounded-xl text-white placeholder-zinc-500 focus:outline-none focus:border-orange-500 transition-colors"
                  required
                />
              </div>
              <div className="relative">
                <input
                  type={showPassword ? 'text' : 'password'}
                  value={password}
                  onChange={(e) => setPassword(e.target.value)}
                  placeholder="Password"
                  className="w-full px-4 py-3 pr-12 bg-zinc-800 border border-zinc-700 rounded-xl text-white placeholder-zinc-500 focus:outline-none focus:border-orange-500 transition-colors"
                  required
                />
                <button
                  type="button"
                  onClick={() => setShowPassword(!showPassword)}
                  className="absolute right-4 top-1/2 -translate-y-1/2 text-zinc-500 hover:text-zinc-300"
                >
                  {showPassword ? <EyeOff className="w-5 h-5" /> : <Eye className="w-5 h-5" />}
                </button>
              </div>

              {error && (
                <div className="flex items-center gap-2 text-red-400 text-sm">
                  <AlertCircle className="w-4 h-4" />
                  <span>{error}</span>
                </div>
              )}

              <button
                type="submit"
                disabled={loading}
                className="w-full py-3 bg-orange-500 rounded-xl font-semibold text-white hover:bg-orange-600 transition-colors disabled:opacity-50"
              >
                {loading ? 'Signing in...' : 'Sign In'}
              </button>

              <button
                type="button"
                onClick={() => setShowLogin(false)}
                className="w-full py-2 text-zinc-500 hover:text-zinc-300 text-sm transition-colors"
              >
                Cancel
              </button>
            </form>
          </div>
        )}
      </div>

      {/* Footer */}
      <div className="absolute bottom-6 text-center text-zinc-600 text-sm">
        <p>© {new Date().getFullYear()} MDM Comics. All rights reserved.</p>
      </div>
    </div>
  );
}
