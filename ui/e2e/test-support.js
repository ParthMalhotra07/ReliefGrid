import { test as base } from "@playwright/test";

export const tiles = "https://tile.openstreetmap.org/**";
export const test = base.extend({
  stubMapTiles: [
    async ({ context }, use) => {
      await context.route(tiles, (route) =>
        route.fulfill({
          contentType: "image/svg+xml",
          body: '<svg xmlns="http://www.w3.org/2000/svg" width="256" height="256"><rect width="256" height="256" fill="#f1efe7"/></svg>',
        }),
      );
      await use();
    },
    { auto: true },
  ],
});
export { expect } from "@playwright/test";
