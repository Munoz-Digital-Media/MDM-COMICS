/**
 * Frontend Server with API Proxy
 *
 * Serves static files and proxies /api/* to the backend.
 * This eliminates cross-origin issues - everything is same-origin.
 */
import express from 'express';
import { createProxyMiddleware } from 'http-proxy-middleware';
import { fileURLToPath } from 'url';
import { dirname, join } from 'path';

// ES module __dirname equivalent
const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

const app = express();
const PORT = process.env.PORT || 3000;

// Backend URL - always HTTPS
const BACKEND_URL = process.env.BACKEND_URL || 'https://mdm-comics-backend-development.up.railway.app';

// Log all requests for debugging
app.use((req, res, next) => {
  console.log(`[Request] ${req.method} ${req.url}`);
  next();
});

// Proxy /api/* to backend
app.use('/api', createProxyMiddleware({
  target: BACKEND_URL,
  changeOrigin: true,
  secure: true,
  logLevel: 'debug',
  onProxyReq: (proxyReq, req, res) => {
    console.log(`[Proxy] Forwarding ${req.method} ${req.originalUrl} -> ${BACKEND_URL}${req.url}`);
    proxyReq.setHeader('X-Forwarded-Host', req.headers.host);
    proxyReq.setHeader('X-Forwarded-Proto', 'https');
  },
  onProxyRes: (proxyRes, req, res) => {
    console.log(`[Proxy] Response ${proxyRes.statusCode} for ${req.originalUrl}`);
  },
  onError: (err, req, res) => {
    console.error('[Proxy Error]', err.message);
    res.status(502).json({ error: 'Proxy error', message: err.message });
  }
}));

// Serve static files from dist/
app.use(express.static(join(__dirname, 'dist')));

// SPA fallback - serve index.html for all non-file routes
// Using app.use instead of app.get('*') for path-to-regexp v8 compatibility
app.use((req, res) => {
  res.sendFile(join(__dirname, 'dist', 'index.html'));
});

app.listen(PORT, '0.0.0.0', () => {
  console.log(`[Server] Running on port ${PORT}`);
  console.log(`[Server] Proxying /api/* to ${BACKEND_URL}`);
});
