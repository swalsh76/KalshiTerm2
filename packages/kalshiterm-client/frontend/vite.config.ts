import { defineConfig } from "vitest/config";
import react from "@vitejs/plugin-react";

// Dev: `kterm ui --port 8765 --no-browser`, then `KTERM_UI_PORT=8765 npm run dev`.
// The backend checks Host and Origin, so the proxy rewrites both to the backend's own address.
const port = process.env.KTERM_UI_PORT ?? "8765";
const target = `http://127.0.0.1:${port}`;

export default defineConfig({
  plugins: [react()],
  build: { outDir: "../src/kalshiterm_client/ui_assets", emptyOutDir: true },
  server: {
    proxy: {
      "/api": {
        target,
        changeOrigin: true,
        ws: true,
        headers: { Origin: target },
      },
    },
  },
  test: { environment: "jsdom", include: ["src/**/*.test.{ts,tsx}"] },
});
