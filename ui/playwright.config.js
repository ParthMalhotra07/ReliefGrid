import { defineConfig } from "@playwright/test";
export default defineConfig({
  testDir: "./e2e",
  testMatch: "*.spec.js",
  workers: 1,
  retries: 0,
  timeout: 30000,
  use: {
    baseURL: "http://127.0.0.1:3412",
    browserName: "chromium",
    viewport: { width: 1440, height: 960 },
    trace: "retain-on-failure",
  },
  reporter: [["list"], ["json", { outputFile: "test-results/results.json" }]],
  webServer: [
    {
      command: "go run .",
      url: "http://127.0.0.1:3411/healthz",
      env: { HTTP_ADDR: "127.0.0.1:3411" },
      reuseExistingServer: false,
    },
    {
      command: "node e2e/fixture.mjs",
      url: "http://127.0.0.1:3412/__test/ready",
      reuseExistingServer: false,
    },
  ],
});
