import { test, expect } from "./test-support.js";

const nominatim = "https://nominatim.openstreetmap.org/**";
test.beforeEach(async ({ request, page }) => {
  await request.post("/__test/reset");
  // No test may reach the real Nominatim service.
  await page.route(nominatim, (route) => route.abort());
});

async function openForm(page, kind = "request") {
  await page.goto("/");
  await expect(page.getByRole("region", { name: "Requests" })).toContainText(
    "LATEST READ",
  );
  await page
    .getByRole("button", {
      name: kind === "request" ? "Create request" : "Register resource",
      exact: true,
    })
    .click();
  return page.getByRole("dialog");
}
const near = (value, want, tolerance) =>
  expect(Math.abs(Number(value) - want)).toBeLessThanOrEqual(tolerance);

test.describe("record details", () => {
  test("a selected resource shows type, status, ID, coordinates and its request", async ({
    page,
    request,
  }) => {
    await request.post("/__test/state", { data: { status: "MATCHED" } });
    await page.goto("/");
    await page
      .getByRole("region", { name: "Resources", exact: true })
      .getByRole("button", { name: /Water team Alpha/ })
      .click();
    const details = page.getByRole("region", { name: "Resource details" });
    await expect(details.getByRole("definition").first()).toBeVisible();
    const fact = (term) =>
      details.locator("div", {
        has: page
          .getByRole("term")
          .filter({ hasText: new RegExp(`^${term}$`) }),
      });
    await expect(fact("Type")).toContainText("Water");
    await expect(fact("Status")).toContainText("BUSY");
    await expect(fact("ID")).toContainText("#22222222");
    await expect(fact("Coordinates")).toContainText("19.10000, 72.90000");
    await expect(fact("Assigned to")).toContainText(
      "Water needed at the school",
    );
    await expect(fact("Assigned to")).toContainText("#11111111");
  });

  test("a matched request shows its resource and straight-line distance", async ({
    page,
    request,
  }) => {
    await request.post("/__test/state", { data: { status: "MATCHED" } });
    await page.goto("/");
    await page
      .getByRole("region", { name: "Requests", exact: true })
      .getByRole("button", { name: /Water needed at the school/ })
      .click();
    const details = page.getByRole("region", { name: "Request details" });
    const fact = (term) =>
      details.locator("div", {
        has: page
          .getByRole("term")
          .filter({ hasText: new RegExp(`^${term}$`) }),
      });
    await expect(fact("Type")).toContainText("Water");
    await expect(fact("Status")).toContainText("MATCHED");
    await expect(fact("ID")).toContainText("#11111111");
    await expect(fact("Coordinates")).toContainText("19.08000, 72.88000");
    await expect(fact("Assigned resource")).toContainText("Water team Alpha");
    const distance = await fact("Straight-line distance")
      .getByRole("definition")
      .textContent();
    const km = Number(distance.match(/([\d.]+) km/)[1]);
    // Haversine, mean Earth radius 6371.0088 km: 3.0598 km.
    near(km, 3.0598, 0.1);
    await request.post("/__test/state", { data: { status: "WAITING" } });
    await page.getByRole("button", { name: /Refresh data/ }).click();
    await expect(fact("Status")).toContainText("WAITING");
    await expect(fact("Straight-line distance")).toHaveCount(0);
  });
});

test.describe("use my location", () => {
  test("granted geolocation fills four-decimal coordinates and moves the picker", async ({
    page,
    context,
  }) => {
    await context.grantPermissions(["geolocation"]);
    await context.setGeolocation({ latitude: 18.52043, longitude: 73.85674 });
    const form = await openForm(page);
    await form.getByRole("button", { name: "Pick on map" }).click();
    await form.getByRole("button", { name: "Use my location" }).click();
    await expect(form.getByLabel("Latitude", { exact: true })).toHaveValue(
      "18.5204",
    );
    await expect(form.getByLabel("Longitude", { exact: true })).toHaveValue(
      "73.8567",
    );
    const picker = form.getByRole("region", { name: "Location map" });
    await expect(
      picker
        .getByRole("img", { name: "Selected location" })
        .or(picker.locator(".picked-pin")),
    ).toBeVisible();
    await form.getByLabel("Longitude", { exact: true }).fill("73.9");
    await expect(form.getByLabel("Longitude", { exact: true })).toHaveValue(
      "73.9",
    );
  });

  test("denied geolocation explains and leaves the fields unchanged", async ({
    page,
    context,
  }) => {
    await context.clearPermissions();
    const form = await openForm(page, "resource");
    await form.getByLabel("Latitude", { exact: true }).fill("19.08");
    await form.getByLabel("Longitude", { exact: true }).fill("72.88");
    await form.getByRole("button", { name: "Use my location" }).click();
    await expect(
      form.getByRole("status", { name: "Location result" }),
    ).toHaveText(/Location unavailable/, { timeout: 12000 });
    await expect(form.getByLabel("Latitude", { exact: true })).toHaveValue(
      "19.08",
    );
    await expect(form.getByLabel("Longitude", { exact: true })).toHaveValue(
      "72.88",
    );
    await form.getByRole("button", { name: "Pick on map" }).click();
    await form.getByRole("button", { name: "Use map centre" }).click();
    await expect(form.getByLabel("Latitude", { exact: true })).not.toHaveValue(
      "19.08",
    );
  });
});

test("How matching works explains the rules without pushing the workspace down", async ({
  page,
}) => {
  await page.setViewportSize({ width: 1366, height: 768 });
  await page.goto("/");
  const map = page.getByRole("region", { name: "Area overview" });
  const requests = page.getByRole("region", { name: "Requests", exact: true });
  const before = [await map.boundingBox(), await requests.boundingBox()];
  const toggle = page.getByRole("button", { name: "How matching works" });
  await expect(toggle).toHaveAttribute("aria-expanded", "false");
  await toggle.click();
  await expect(toggle).toHaveAttribute("aria-expanded", "true");
  const card = page.getByRole("region", { name: "How matching works" });
  for (const text of [
    "A request is stored and its matching work queued.",
    "same type",
    "within 50 km",
    "availability window is open now",
    "available and unclaimed",
    "The nearest eligible resource is claimed; only one request can win it.",
    "retried after 5 s, 30 s, then every 2 minutes",
    "registering a new resource wakes waiting requests immediately",
    "Start work → Mark fulfilled",
    "the resource returns if reusable",
    "the resource is released",
  ])
    await expect(card).toContainText(text);
  const after = [await map.boundingBox(), await requests.boundingBox()];
  expect(after).toEqual(before);
  expect(after[0].y).toBeLessThan(768);
  expect(after[1].y).toBeLessThan(768);
  await toggle.click();
  await expect(card).toHaveCount(0);
  await expect(toggle).toHaveAttribute("aria-expanded", "false");
});

test("a metric filter shows a chip that explains the empty list and clears", async ({
  page,
  request,
}) => {
  await request.post("/__test/state", { data: { status: "MATCHED" } });
  await page.goto("/");
  const list = page.getByRole("region", { name: "Requests", exact: true });
  await expect(list).toContainText("Water needed at the school");
  const attention = page.getByRole("button", { name: /Needs attention/ });
  const chip = page.getByRole("button", {
    name: "Clear the attention filter",
  });
  for (const clear of ["chip", "Clear filters"]) {
    await attention.click();
    await expect(page.getByText("Filter: Needs attention")).toBeVisible();
    await expect(list).toContainText("No requests need attention");
    await expect(list).not.toContainText("Water needed at the school");
    if (clear === "chip") await chip.click();
    else await page.getByRole("button", { name: "Clear filters" }).click();
    await expect(page.getByText("Filter: Needs attention")).toHaveCount(0);
    await expect(attention).toHaveAttribute("aria-pressed", "false");
    await expect(list).toContainText("Water needed at the school");
  }
});

test.describe("place search", () => {
  test("the bundled gazetteer answers offline without any network search", async ({
    page,
    context,
  }) => {
    let calls = 0;
    await page.route(nominatim, (route) => {
      calls++;
      return route.abort();
    });
    const form = await openForm(page);
    await context.setOffline(true);
    const box = form.getByRole("searchbox", { name: "Search place" });
    await box.fill("andh");
    await expect(form.getByRole("list", { name: "Place results" })).toHaveCount(
      0,
    );
    await box.press("Enter");
    const results = form.getByRole("list", { name: "Place results" });
    const andheri = results.getByRole("button", { name: /Andheri/ });
    await expect(andheri).toContainText("Maharashtra");
    await andheri.click();
    near(
      await form.getByLabel("Latitude", { exact: true }).inputValue(),
      19.1197,
      0.01,
    );
    near(
      await form.getByLabel("Longitude", { exact: true }).inputValue(),
      72.8468,
      0.01,
    );
    expect(
      (await form.getByLabel("Latitude", { exact: true }).inputValue()).split(
        ".",
      )[1],
    ).toHaveLength(4);
    await expect(
      form.getByRole("region", { name: "Location map" }),
    ).toBeVisible();
    await expect(form.locator(".picked-pin")).toBeVisible();
    await expect(form.locator(".leaflet-control-scale-line")).toHaveText(
      /^\d+ (m|km)$/,
    );
    await form.getByLabel("Latitude", { exact: true }).fill("19.2");
    await expect(form.getByLabel("Latitude", { exact: true })).toHaveValue(
      "19.2",
    );
    await box.fill("Lonavala");
    await box.press("Enter");
    await expect(
      form.getByRole("status", { name: "Place search result" }),
    ).toHaveText("Place search unavailable, use the map or enter coordinates.");
    expect(calls).toBe(0);
  });

  test("online search uses Nominatim once per second with attribution", async ({
    page,
  }) => {
    const seen = [];
    await page.route(nominatim, async (route) => {
      seen.push({
        url: route.request().url(),
        referer: (await route.request().allHeaders()).referer,
      });
      await route.fulfill({
        contentType: "application/json",
        headers: { "Access-Control-Allow-Origin": "*" },
        body: JSON.stringify(
          Array.from({ length: 7 }, (_, i) => ({
            display_name: `Lonavala ${i}, Pune, Maharashtra, India`,
            lat: String(18.7546 + i / 100),
            lon: String(73.4062 + i / 100),
          })),
        ),
      });
    });
    const form = await openForm(page);
    const box = form.getByRole("searchbox", { name: "Search place" });
    await box.fill("Lonavala");
    await form.getByRole("button", { name: "Search", exact: true }).click();
    await form.getByRole("button", { name: "Search", exact: true }).click();
    const results = form.getByRole("list", { name: "Place results" });
    await expect(results.getByRole("button")).toHaveCount(5);
    await expect(
      form.getByText("Search: Nominatim © OpenStreetMap contributors"),
    ).toBeVisible();
    await page.waitForTimeout(300);
    expect(seen).toHaveLength(1);
    const url = new URL(seen[0].url);
    expect(url.origin + url.pathname).toBe(
      "https://nominatim.openstreetmap.org/search",
    );
    expect(url.searchParams.get("format")).toBe("jsonv2");
    expect(url.searchParams.get("limit")).toBe("5");
    expect(url.searchParams.get("q")).toBe("Lonavala");
    expect(seen[0].referer).toBe("http://127.0.0.1:3412/");
    await results.getByRole("button", { name: /Lonavala 0/ }).click();
    await expect(form.getByLabel("Latitude", { exact: true })).toHaveValue(
      "18.7546",
    );
    await expect(form.getByLabel("Longitude", { exact: true })).toHaveValue(
      "73.4062",
    );
    await page.waitForTimeout(1100);
    await box.fill("Khandala");
    await box.press("Enter");
    await expect.poll(() => seen.length).toBe(2);
  });

  test("a failed online search shows the unavailable message", async ({
    page,
  }) => {
    await page.route(nominatim, (route) =>
      route.fulfill({
        status: 503,
        headers: { "Access-Control-Allow-Origin": "*" },
        body: "busy",
      }),
    );
    const form = await openForm(page, "resource");
    const box = form.getByRole("searchbox", { name: "Search place" });
    await box.fill("Lonavala");
    await box.press("Enter");
    await expect(
      form.getByRole("status", { name: "Place search result" }),
    ).toHaveText("Place search unavailable, use the map or enter coordinates.");
    await expect(form.getByLabel("Latitude", { exact: true })).toHaveValue("");
  });
});

test("the street layer never requests tiles outside the world", async ({
  page,
}) => {
  const requested = [];
  let rejected = 0;
  await page.route("https://tile.openstreetmap.org/**", (route) => {
    const [z, x, y] = new URL(route.request().url()).pathname
      .replace(".png", "")
      .split("/")
      .slice(1)
      .map(Number);
    requested.push({ z, x, y });
    const size = 2 ** z;
    if (x < 0 || y < 0 || x >= size || y >= size) {
      rejected++;
      return route.fulfill({ status: 400, body: "Tile out of range" });
    }
    return route.fulfill({
      contentType: "image/svg+xml",
      body: '<svg xmlns="http://www.w3.org/2000/svg" width="256" height="256"><rect width="256" height="256" fill="#f1efe7"/></svg>',
    });
  });
  await page.goto("/");
  const status = page.getByLabel("Map background status");
  await expect(status).toHaveText("Street map");
  await page.getByRole("button", { name: "Fit globe" }).click();
  await expect(page.locator(".leaflet-control-scale-line")).toHaveText(
    /000 km$/,
  );
  await page.waitForTimeout(1500);
  await expect(status).toHaveText("Street map");
  const zoomOut = page.getByRole("button", { name: "Zoom out" });
  for (let i = 0; i < 6; i++) {
    if ((await zoomOut.getAttribute("aria-disabled")) === "true") break;
    await zoomOut.click();
    await page.waitForTimeout(400);
  }
  await expect(zoomOut).toHaveAttribute("aria-disabled", "true");
  await page.waitForTimeout(1500);
  await expect(status).toHaveText("Street map");
  expect(requested.some((t) => t.z === 0)).toBe(true);
  const outside = requested.filter(
    ({ z, x, y }) => x < 0 || y < 0 || x >= 2 ** z || y >= 2 ** z,
  );
  expect(outside).toEqual([]);
  expect(rejected).toBe(0);
});

test("a gazetteer search is not blocked by a slow online search", async ({
  page,
}) => {
  let release;
  const replied = new Promise((resolve) => (release = resolve));
  await page.route(nominatim, async (route) => {
    await replied;
    await route
      .fulfill({
        contentType: "application/json",
        headers: { "Access-Control-Allow-Origin": "*" },
        body: JSON.stringify([
          { display_name: "Late Lonavala", lat: "18.75", lon: "73.4" },
        ]),
      })
      .catch(() => {});
  });
  const form = await openForm(page);
  const box = form.getByRole("searchbox", { name: "Search place" });
  await box.fill("Lonavala");
  await box.press("Enter");
  await expect(
    form.getByRole("status", { name: "Place search result" }),
  ).toHaveText("Searching…");
  await box.fill("andheri");
  await box.press("Enter");
  const results = form.getByRole("list", { name: "Place results" });
  await expect(results.getByRole("button", { name: /Andheri/ })).toBeVisible({
    timeout: 1000,
  });
  release();
  await page.waitForTimeout(500);
  await expect(
    results.getByRole("button", { name: /Late Lonavala/ }),
  ).toHaveCount(0);
  await expect(results.getByRole("button", { name: /Andheri/ })).toBeVisible();
});
