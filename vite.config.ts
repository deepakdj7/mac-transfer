import { defineConfig } from 'vite'

export default defineConfig({
  base: process.env.GITHUB_ACTIONS ? '/mac-transfer/' : '/',
  build: {
    target: 'es2022',
  },
  define: {
    global: 'globalThis',
  },
})
