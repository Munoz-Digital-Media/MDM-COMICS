/**
 * Error Boundary Component
 *
 * Catches JavaScript errors anywhere in the child component tree,
 * logs those errors, and displays a fallback UI instead of crashing.
 *
 * FE-ERR-001: Integrated with error tracking service for production monitoring
 * FE-ERR-002: Supports router-based navigation via navigate prop
 *
 * Usage:
 * <ErrorBoundary>
 *   <App />
 * </ErrorBoundary>
 *
 * With router integration:
 * <ErrorBoundary navigate={navigate}>
 *   <RouterOutlet />
 * </ErrorBoundary>
 */
import React from 'react';
import { captureException } from '../services/errorTracking';

class ErrorBoundary extends React.Component {
  constructor(props) {
    super(props);
    this.state = { hasError: false, error: null, errorInfo: null };
  }

  static getDerivedStateFromError(error) {
    // Update state so the next render will show the fallback UI
    return { hasError: true, error };
  }

  componentDidCatch(error, errorInfo) {
    // Log the error to console
    console.error('ErrorBoundary caught an error:', error, errorInfo);
    this.setState({ errorInfo });

    // FE-ERR-001: Send to error tracking service
    captureException(error, errorInfo, {
      source: 'ErrorBoundary',
      componentStack: errorInfo?.componentStack,
    });
  }

  handleReload = () => {
    window.location.reload();
  };

  handleGoHome = () => {
    // FE-ERR-002: Use router navigation if available, fallback to window.location
    if (this.props.navigate) {
      // Reset error state and navigate using router
      this.setState({ hasError: false, error: null, errorInfo: null }, () => {
        this.props.navigate('/');
      });
    } else {
      window.location.href = '/';
    }
  };

  handleRetry = () => {
    // Reset error state to allow retry
    this.setState({ hasError: false, error: null, errorInfo: null });
  };

  render() {
    // Allow custom fallback UI via props
    if (this.state.hasError) {
      if (this.props.fallback) {
        return this.props.fallback;
      }

      // Default fallback UI
      return (
        <div className="min-h-screen bg-zinc-950 flex items-center justify-center p-4">
          <div className="max-w-md w-full bg-zinc-900 rounded-xl border border-zinc-800 p-8 text-center">
            <div className="text-6xl mb-4">💥</div>
            <h1 className="text-2xl font-bold text-white mb-2">
              Something went wrong
            </h1>
            <p className="text-zinc-400 mb-6">
              We're sorry, but something unexpected happened.
              Please try refreshing the page.
            </p>

            {/* Error details (only in development) */}
            {import.meta.env.DEV && this.state.error && (
              <details className="mb-6 text-left">
                <summary className="text-orange-500 cursor-pointer hover:text-orange-400">
                  Error Details
                </summary>
                <pre className="mt-2 p-3 bg-zinc-800 rounded text-xs text-red-400 overflow-auto max-h-40">
                  {this.state.error.toString()}
                  {this.state.errorInfo?.componentStack}
                </pre>
              </details>
            )}

            <div className="flex gap-3 justify-center flex-wrap">
              <button
                onClick={this.handleRetry}
                className="px-4 py-2 bg-orange-500 text-white rounded-lg hover:bg-orange-600 transition-colors"
              >
                Try Again
              </button>
              <button
                onClick={this.handleReload}
                className="px-4 py-2 bg-zinc-700 text-white rounded-lg hover:bg-zinc-600 transition-colors"
              >
                Refresh Page
              </button>
              <button
                onClick={this.handleGoHome}
                className="px-4 py-2 bg-zinc-700 text-white rounded-lg hover:bg-zinc-600 transition-colors"
              >
                Go Home
              </button>
            </div>
          </div>
        </div>
      );
    }

    return this.props.children;
  }
}

export default ErrorBoundary;
