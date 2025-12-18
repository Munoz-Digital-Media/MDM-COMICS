/**
 * Frontend Server with API Proxy
 *
 * Serves static files and proxies /api/* to the backend.
 * This eliminates cross-origin issues - everything is same-origin.
 */
import express from 'express';
import { createProxyMiddleware } from 'http-proxy-middleware';
import path from 'path';

const app = express();
const PORT = process.env.PORT || 3000;

// Backend URL - always HTTPS
const BACKEND_URL = process.env.BACKEND_URL || 'https://mdm-comics-backend-development.up.railway.app';

// Proxy /api/* to backend
app.use('/api', createProxyMiddleware({
  target: BACKEND_URL,
  changeOrigin: true,
  secure: true,
  logLevel: 'warn',
  onProxyReq: (proxyReq, req, res) => {
    // Forward original host for CORS
    proxyReq.setHeader('X-Forwarded-Host', req.headers.host);
    proxyReq.setHeader('X-Forwarded-Proto', 'https');
  },
  onError: (err, req, res) => {
    console.error('[Proxy Error]', err.message);
    res.status(502).json({ error: 'Proxy error', message: err.message });
  }
}));

// Serve static files from dist/
app.use(express.static(path.join(__dirname, 'dist')));

// SPA fallback - serve index.html for all non-file routes
app.get('*', (req, res) => {
  res.sendFile(path.join(__dirname, 'dist', 'index.html'));
});

app.listen(PORT, '0.0.0.0', () => {
  console.log(`[Server] Running on port ${PORT}`);
  console.log(`[Server] Proxying /api/* to ${BACKEND_URL}`);
});
