import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";
//import basicSsl from "@vitejs/plugin-basic-ssl";

export default defineConfig({
  plugins: [
    react(),
    //basicSsl(),
  ],
  server: {
    host: true,        // allows access from outside container
    watch: {
      usePolling: true, // enables Docker-compatible watching
      interval: 100     // how often to check for changes
    }
  },
});


