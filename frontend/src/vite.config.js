import { defineConfig } from 'vite';

export default defineConfig({
  server: {
    host: true, // makes the server listen on all network interfaces (0.0.0.0)
  },
});