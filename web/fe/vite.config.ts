import { fileURLToPath, URL } from 'node:url'
import { defineConfig } from 'vite'
import vue from '@vitejs/plugin-vue'
import tailwindcss from '@tailwindcss/vite'

// Build ra `web/static/dist` và FastAPI phục vụ NGAY tại `/` (bản Vue là giao diện chính; bản
// vanilla giữ ở `/classic` để so sánh/rollback). Vì vậy `base` là '/'.
// `pnpm dev` proxy `/api` sang desk :8099 để REST + SSE chạy như bản thật.
export default defineConfig({
  base: '/',
  plugins: [vue(), tailwindcss()],
  resolve: {
    alias: { '@': fileURLToPath(new URL('./src', import.meta.url)) },
  },
  build: {
    outDir: '../static/dist',
    emptyOutDir: true,
  },
  server: {
    port: 5174,
    proxy: {
      '/api': { target: 'http://127.0.0.1:8099', changeOrigin: true },
    },
  },
})
