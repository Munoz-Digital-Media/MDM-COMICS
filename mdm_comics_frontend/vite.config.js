import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

// https://vitejs.dev/config/
export default defineConfig({
  plugins: [react()],
  // FE-STATE-002: Inject build-time constants
  define: {
    __BUILD_DATE__: JSON.stringify(new Date().toISOString()),
    __BUILD_ENV__: JSON.stringify(process.env.NODE_ENV || 'development'),
  },
  server: {
    port: 3000,
    open: true,
    proxy: {
      '/api': {
        target: 'https://mdm-comics-production.up.railway.app',
        changeOrigin: true,
        secure: true,
      }
    }
  },
  build: {
    outDir: 'dist',
    sourcemap: true
  },
  test: {
    environment: 'jsdom',
    globals: true,
    setupFiles: './src/test/setupTests.js',
    coverage: {
      provider: 'c8',
      reporter: ['text', 'html', 'lcov'],
      all: false,
      include: ['src/__tests__/**'],
      lines: 100,
      functions: 100,
      branches: 100,
      statements: 100,
      reportsDirectory: './coverage'
    }
  }
})
// Build cache bust: 1766036209
