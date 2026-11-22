import { test, expect, tiles } from "./test-support.js";

test.beforeEach(async ({ request }) => {
  await request.post("/__test/reset");
});

test("street map and resource inspector use authoritative coordinates", async ({
  page,
}) => {
  const requestedTile = page.waitForRequest((req) =>
    req.url().startsWith("https://tile.openstreetmap.org/"),
  );
  await page.goto("/");
  await expect(page.getByLabel("Map background status")).toHaveText(
    "Street map",
  );
  const headers = await (await requestedTile).allHeaders();
  expect(headers.referer).toBe("http://127.0.0.1:3412/");
  await expect(
    page.getByRole("link", { name: "OpenStreetMap contributors" }),
  ).toBeVisible();
  await page.getByRole("button", { name: "Fit to results" }).click();
  await page
    .getByRole("button", { name: "Resource: Water team Alpha" })
    .click();
  const details = page.getByRole("region", { name: "Resource details" });
  await expect(details).toContainText("19.10000, 72.90000");
  await expect(details).toContainText("#11111111");
  await expect(
    page.getByRole("region", { name: "Request details" }),
  ).toContainText("Water needed at the school");
  await expect(
    page.locator(".leaflet-overlay-pane path[stroke-dasharray]"),
  ).toHaveCount(1);
});

test("tile failure falls back without interrupting dispatch", async ({
  page,
}) => {
  await page.route(tiles, (route) => route.abort());
  await page.goto("/");
  await expect(page.getByLabel("Map background status")).toHaveText(
    "Street map unavailable · outline map shown",
  );
  await expect(page.locator(".leaflet-world-overview-pane path")).toHaveCount(
    1,
  );
  await expect(
    page.getByRole("status", { name: "Connection status" }),
  ).toContainText("Live");
  await expect(
    page.getByRole("button", { name: "Create request" }),
  ).toBeEnabled();
  await expect(
    page.locator(".leaflet-overlay-pane path[stroke-dasharray]"),
  ).toHaveCount(1);
  await page.getByRole("button", { name: "World overview" }).click();
  await expect(page.getByLabel("Map background status")).toHaveText(
    "World overview · no street detail",
  );
  await page.screenshot({ path: "test-results/dispatch-world-overview.png" });
  await page.unroute(tiles);
  await page.getByRole("button", { name: "Use street map" }).click();
  await expect(page.getByLabel("Map background status")).toHaveText(
    "Street map",
  );
});

test("world overview fits the continents, keeps records and returns to the street view", async ({
  page,
}) => {
  await page.setViewportSize({ width: 390, height: 844 });
  let streetRequests = 0;
  page.on("request", (request) => {
    if (request.url().startsWith("https://tile.openstreetmap.org/"))
      streetRequests++;
  });
  await page.goto("/");
  const map = page.getByRole("region", { name: "Dispatch map" });
  await expect(
    map.getByRole("button", { name: "Resource: Water team Alpha" }),
  ).toBeAttached();
  await expect(page.getByLabel("Map background status")).toHaveText(
    "Street map",
  );
  const before = streetRequests;
  await page.getByRole("button", { name: "World overview" }).click();
  await expect(page.getByLabel("Map background status")).toHaveText(
    "World overview · no street detail",
  );
  const bounds = await map.boundingBox();
  for (const name of ["North America", "Africa", "Asia", "Oceania"]) {
    const place = map.getByText(name, { exact: true });
    await expect(place).toBeVisible({ timeout: 3000 });
    const label = await place.boundingBox();
    expect(label.x).toBeGreaterThanOrEqual(bounds.x);
    expect(label.x + label.width).toBeLessThanOrEqual(bounds.x + bounds.width);
  }
  await expect(
    map.getByRole("button", { name: "Resource: Water team Alpha" }),
  ).toBeAttached();
  expect(streetRequests).toBe(before);
  await page.getByRole("button", { name: "Use street map" }).click();
  await expect(page.getByLabel("Map background status")).toHaveText(
    "Street map",
  );
  await expect(map.getByText("Africa", { exact: true })).toHaveCount(0);
});

test("one missing tile preserves the street map and its loaded tiles", async ({
  page,
}) => {
  let first = true;
  await page.route(tiles, (route) => {
    if (!first) return route.fallback();
    first = false;
    return route.abort();
  });
  await page.goto("/");
  await expect(page.getByLabel("Map background status")).toHaveText(
    "Street map · some tiles unavailable",
    { timeout: 4000 },
  );
  await expect(page.locator(".leaflet-tile-loaded").first()).toBeVisible();
});

test("selecting a resource from the mobile list brings its details into view", async ({
  page,
}) => {
  await page.setViewportSize({ width: 390, height: 844 });
  await page.goto("/");
  await page.getByRole("button", { name: "List", exact: true }).click();
  await page
    .getByRole("region", { name: "Resources", exact: true })
    .getByRole("button", { name: /Water team Alpha/ })
    .click();
  const details = page.getByRole("region", { name: "Resource details" });
  await expect(details).toBeFocused({ timeout: 3000 });
  await expect(details).toBeInViewport();
});

test("picking a point populates editable form coordinates before the normal submit", async ({
  page,
  request,
}) => {
  await page.goto("/");
  await page.getByRole("button", { name: "Create request" }).click();
  const form = page.getByRole("dialog");
  await form.getByLabel("Request details").fill("Supplies at the school");
  await form.getByRole("button", { name: "Pick on map" }).click();
  await form.getByText("Click the map or use its centre.").waitFor();
  await form.getByRole("button", { name: "Use map centre" }).click();
  const latitude = form.getByLabel("Latitude", { exact: true });
  const longitude = form.getByLabel("Longitude", { exact: true });
  await expect(latitude).not.toHaveValue("");
  await expect(longitude).not.toHaveValue("");
  const lat = Number(await latitude.inputValue());
  await longitude.fill("72.88");
  expect(
    (await (await request.get("/__test/creations")).json()).calls,
  ).toHaveLength(0);
  await form.getByRole("button", { name: "Submit request" }).click();
  await expect(
    form.getByRole("status", { name: "Creation result" }),
  ).toBeVisible();
  const { calls } = await (await request.get("/__test/creations")).json();
  expect(calls).toHaveLength(1);
  expect(calls[0].key).toMatch(/^[0-9a-f-]{36}$/);
  expect(JSON.parse(calls[0].body).location).toEqual({ lat, lng: 72.88 });
});
