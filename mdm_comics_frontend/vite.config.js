import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

// https://vitejs.dev/config/
export default defineConfig({
  plugins: [react()],
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
  }
})
