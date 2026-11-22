import { randomUUID } from "node:crypto";
import { test, expect } from "./test-support.js";

test.beforeEach(async ({ request }) => {
  await request.post("/__test/reset");
});

const create = async (request, kind, data) =>
  (
    await request.post(`/api/v1/${kind}`, {
      headers: { "Idempotency-Key": randomUUID() },
      data,
    })
  ).json();
const resources = (page) =>
  page.getByRole("region", { name: "Resources", exact: true });

test("resource cards and details use the map's labelled type icons", async ({
  page,
  request,
}) => {
  let lat = 18.9;
  for (const type of ["FOOD", "MEDICINE", "TRANSPORTATION", "SHELTER"])
    await create(request, "resources", {
      resourceType: type,
      name: `${type} unit`,
      location: { lat: (lat += 0.08), lng: 72.95 },
      reusable: true,
      windowStart: "2026-11-05T00:00:00Z",
      windowEnd: "2026-11-06T00:00:00Z",
    });
  await page.goto("/");
  for (const [name, icon, spoken] of [
    ["Water team Alpha", "water", "Water"],
    ["FOOD unit", "food", "Food"],
    ["MEDICINE unit", "medicine", "Medicine"],
    ["TRANSPORTATION unit", "vehicle", "Transportation"],
    ["SHELTER unit", "house", "Shelter"],
  ]) {
    const card = resources(page).locator(".resource-card", { hasText: name });
    const image = card.getByRole("img", { name: spoken, exact: true });
    await expect(image).toHaveAttribute("data-icon", icon);
    await card.click();
    const details = page.getByRole("region", { name: "Resource details" });
    await expect(
      details.getByRole("img", { name: spoken, exact: true }),
    ).toHaveAttribute("data-icon", icon);
  }
  // The map's own pins stay decorative inside their labelled buttons.
  await expect(
    page
      .getByRole("region", { name: "Dispatch map" })
      .locator('svg[data-icon="water"]'),
  ).toHaveAttribute("aria-hidden", "true");
});

test("the urgency sort lists unfinished requests before finished ones", async ({
  page,
  request,
}) => {
  // The fixture request is CRITICAL; once fulfilled it goes below open work.
  await request.post("/__test/state", { data: { status: "FULFILLED" } });
  await create(request, "requests", {
    requestType: "FOOD",
    urgency: "NORMAL",
    peopleAffected: 2,
    details: "Open normal meals",
    location: { lat: 19.2, lng: 72.85 },
  });
  await page.goto("/");
  await expect(page.getByLabel("Sort requests")).toHaveValue("urgency");
  await expect(
    page
      .getByRole("region", { name: "Requests", exact: true })
      .locator(".request-card strong"),
  ).toHaveText(["Open normal meals", "Water needed at the school"]);
});

test.describe("one shared clock", () => {
  // Card badge, details badge and map dimming, read in one snapshot.
  const snapshot = (page, name) =>
    page.evaluate((name) => {
      const card = [...document.querySelectorAll(".resource-card")].find((v) =>
        v.textContent.includes(name),
      );
      const details = document.querySelector(
        '[aria-label="Resource details"] .window-badge',
      );
      const pin = [
        ...document.querySelectorAll(
          '[aria-label="Dispatch map"] .resource-pin',
        ),
      ].find((v) =>
        v.getAttribute("aria-label").startsWith("Resource: " + name),
      );
      return {
        card: card?.querySelector(".window-badge")?.textContent ?? null,
        details: details?.textContent ?? null,
        pin: pin?.getAttribute("aria-label") ?? null,
      };
    }, name);

  for (const [kind, window, before, after] of [
    [
      "closing",
      (t) => ({ windowStart: t(-60), windowEnd: t(0) }),
      { badge: /^Open · closes in 1 min$/, reason: "" },
      { badge: /^Expired$/, reason: " · Window closed" },
    ],
    [
      "opening",
      (t) => ({ windowStart: t(0), windowEnd: t(120) }),
      { badge: /^Opens in 1 min$/, reason: " · Window closed" },
      { badge: /^Open · closes in 2 h$/, reason: "" },
    ],
  ])
    test(`a window ${kind} flips the badge, details and dimming together`, async ({
      page,
      request,
    }) => {
      const T = new Date("2026-11-05T12:00:00Z").getTime();
      const t = (minutes) => new Date(T + minutes * 60000).toISOString();
      await page.clock.install({ time: T - 20000 });
      // The 5 s tick then falls 2.5 s either side of T, so only the boundary
      // wake-up can flip the state at T + 100 ms.
      await page.clock.pauseAt(T - 12500);
      await request.post("/__test/state", { data: { status: "WAITING" } });
      await create(request, "resources", {
        resourceType: "WATER",
        name: "Edge van",
        location: { lat: 19.11, lng: 72.91 },
        reusable: true,
        ...window(t),
      });
      await page.goto("/");
      const name = "Edge van";
      const requestCard = page
        .getByRole("region", { name: "Requests", exact: true })
        .getByRole("button", { name: /Water needed at the school/ });
      const resourceCard = resources(page).getByRole("button", {
        name: /Edge van/,
      });
      // Phase 1: card and map dimming (a request is selected).
      await requestCard.click();
      await expect
        .poll(async () => (await snapshot(page, name)).card)
        .toMatch(before.badge);
      expect((await snapshot(page, name)).pin).toBe(
        `Resource: ${name}${before.reason}`,
      );
      await page.clock.runFor(11000);
      let s = await snapshot(page, name);
      expect(s.card).toMatch(before.badge);
      expect(s.pin).toBe(`Resource: ${name}${before.reason}`);
      // Cross T, then force an unrelated re-render: both must agree.
      await page.clock.runFor(1600);
      await page.getByRole("button", { name: /Refresh data/ }).click();
      const seen = [];
      await expect
        .poll(async () => {
          const v = await snapshot(page, name);
          seen.push(v);
          return v.card;
        })
        .toMatch(after.badge);
      for (const v of seen)
        expect([
          [
            before.badge.test(v.card),
            v.pin === `Resource: ${name}${before.reason}`,
          ],
          [
            after.badge.test(v.card),
            v.pin === `Resource: ${name}${after.reason}`,
          ],
        ]).toContainEqual([true, true]);
      // Phase 2: card and details panel (the resource is selected).
      await resourceCard.click();
      s = await snapshot(page, name);
      expect(s.card).toMatch(after.badge);
      expect(s.details).toMatch(after.badge);
    });

  test("the boundary wake-up re-arms for the next boundary", async ({
    page,
    request,
  }) => {
    const T = new Date("2026-11-05T12:00:00Z").getTime();
    await page.clock.install({ time: T - 20000 });
    // Ticks fall at T - 2.5 s, T + 57.5 s and T + 62.5 s.
    await page.clock.pauseAt(T - 12500);
    await create(request, "resources", {
      resourceType: "WATER",
      name: "Edge van",
      location: { lat: 19.11, lng: 72.91 },
      reusable: true,
      windowStart: new Date(T).toISOString(),
      windowEnd: new Date(T + 60000).toISOString(),
    });
    await page.goto("/");
    const card = async () => (await snapshot(page, "Edge van")).card;
    await expect.poll(card).toBe("Opens in 1 min");
    await page.clock.runFor(12600);
    await expect.poll(card).toBe("Open · closes in 1 min");
    // No new data from here on: only the clock can re-arm the wake-up.
    await request.post("/__test/state", { data: { fail: true } });
    await page.clock.runFor(60000);
    await expect.poll(card).toBe("Expired");
  });

  test("the details panel and card flip together too", async ({
    page,
    request,
  }) => {
    const T = new Date("2026-11-05T12:00:00Z").getTime();
    await page.clock.install({ time: T - 20000 });
    // The 5 s tick then falls 2.5 s either side of T, so only the boundary
    // wake-up can flip the state at T + 100 ms.
    await page.clock.pauseAt(T - 12500);
    await create(request, "resources", {
      resourceType: "WATER",
      name: "Edge van",
      location: { lat: 19.11, lng: 72.91 },
      reusable: true,
      windowStart: new Date(T - 3600000).toISOString(),
      windowEnd: new Date(T).toISOString(),
    });
    await page.goto("/");
    await resources(page)
      .getByRole("button", { name: /Edge van/ })
      .click();
    await expect
      .poll(async () => (await snapshot(page, "Edge van")).details)
      .toBe("Open · closes in 1 min");
    await page.clock.runFor(12600);
    await page.getByRole("button", { name: /Refresh data/ }).click();
    const seen = [];
    await expect
      .poll(async () => {
        const v = await snapshot(page, "Edge van");
        seen.push(v);
        return v.details;
      })
      .toBe("Expired");
    for (const v of seen) expect(v.card).toBe(v.details);
  });
});

test("the queue sparkline follows waiting and retrying requests over 15 minutes", async ({
  page,
  request,
}) => {
  await page.clock.install({ time: new Date("2026-11-05T12:00:00Z") });
  await request.post("/__test/state", { data: { status: "WAITING" } });
  const rows = [];
  for (const [details, lat] of [
    ["Queue meals one", 19.2],
    ["Queue meals two", 19.25],
  ])
    rows.push(
      await create(request, "requests", {
        requestType: "FOOD",
        urgency: "NORMAL",
        peopleAffected: 2,
        details,
        location: { lat, lng: 72.85 },
      }),
    );
  // Retrying requests count as queued too.
  await request.post("/__test/patch", {
    data: { id: rows[1].requestId, set: { status: "RETRYING" } },
  });
  const paths = new Set();
  let reads = 0;
  page.on("request", (r) => {
    const url = new URL(r.url());
    if (url.host !== "127.0.0.1:3412") return;
    paths.add(url.pathname);
    if (url.pathname === "/api/v1/requests") reads++;
  });
  await page.goto("/");
  const panel = page.getByRole("region", { name: "Matching queue" });
  const chart = panel.getByRole("img", { name: /^Queue:/ });
  await expect(chart).toHaveAccessibleName(
    "Queue: 3 now, peak 3 in last 15 min",
  );
  await expect(panel.locator(".trend-now")).toHaveText("3");
  // Two requests leave the queue; the next ordinary 15 s read samples it.
  for (const row of rows)
    await request.post("/__test/patch", {
      data: { id: row.requestId, set: { status: "MATCHED" } },
    });
  await page.clock.runFor(15000);
  await expect(chart).toHaveAccessibleName(
    "Queue: 1 now, peak 3 in last 15 min",
  );
  await expect(panel.locator(".trend-now")).toHaveText("1");
  const points = await chart.locator("polyline").getAttribute("points");
  expect(points.split(" ").length).toBeGreaterThanOrEqual(2);
  // No new polling: the usual read every 15 s, and only existing endpoints.
  const before = reads;
  await page.clock.runFor(60000);
  expect(reads - before).toBeLessThanOrEqual(5);
  expect([...paths].filter((v) => /^\/(api|ui)\//.test(v)).sort()).toEqual([
    "/api/v1/events",
    "/api/v1/requests",
    "/api/v1/resources",
    "/ui/status",
  ]);
  // After 15 minutes the earlier peak has left the window.
  await page.clock.runFor(16 * 60000);
  await expect(chart).toHaveAccessibleName(
    "Queue: 1 now, peak 1 in last 15 min",
  );
  expect(Number(await chart.getAttribute("data-samples"))).toBeLessThanOrEqual(
    90,
  );
});

test("the sparkline says it is collecting before the first read", async ({
  page,
  request,
}) => {
  await request.post("/__test/state", { data: { fail: true } });
  await page.goto("/");
  const panel = page.getByRole("region", { name: "Matching queue" });
  await expect(panel.getByText("collecting…")).toBeVisible();
  await expect(panel.getByRole("img", { name: /^Queue:/ })).toHaveCount(0);
});

test("the bundled Inter font loads from the app itself and works offline", async ({
  page,
  request,
}) => {
  const external = [];
  const fonts = [];
  page.on("request", (r) => {
    const url = new URL(r.url());
    if (!["127.0.0.1:3412", "tile.openstreetmap.org"].includes(url.host))
      external.push(r.url());
  });
  page.on("response", async (r) => {
    if (r.url().endsWith(".woff2"))
      fonts.push([new URL(r.url()).pathname, r.headers()["content-type"]]);
  });
  await page.goto("/");
  await page.evaluate(() => document.fonts.ready);
  expect(
    await page.evaluate(() =>
      document.fonts.check('600 16px "Inter Variable"'),
    ),
  ).toBe(true);
  expect(
    await page.evaluate(() => getComputedStyle(document.body).fontFamily),
  ).toMatch(/^"Inter Variable"/);
  expect(fonts).toEqual([
    [
      expect.stringMatching(/^\/assets\/inter-latin-wght-normal-\w+\.woff2$/),
      "font/woff2",
    ],
  ]);
  // Precached by the service worker, and its licence shipped with the app.
  const worker = await (await request.get("/sw.js")).text();
  expect(worker).toContain(fonts[0][0]);
  const licences = await (await request.get("/assets/licenses.txt")).text();
  expect(licences).toContain("SIL OPEN FONT LICENSE Version 1.1");
  expect(external).toEqual([]);
});

test("text meets WCAG AA contrast across the main views", async ({
  page,
  request,
}) => {
  const { contrastFailures } = await import("./contrast.js");
  await request.post("/__test/state", { data: { status: "WAITING" } });
  await page.goto("/");
  await expect(
    page.getByRole("status", { name: "Connection status" }),
  ).toContainText("Live");
  const failures = [];
  const check = async (view) =>
    failures.push(
      ...(await contrastFailures(page)).map((v) => `${view}: ${v}`),
    );
  await check("overview");
  await page
    .getByRole("region", { name: "Requests", exact: true })
    .getByRole("button", { name: /Water needed at the school/ })
    .click();
  await check("request selected");
  await page
    .getByRole("region", { name: "Resources", exact: true })
    .getByRole("button", { name: /Water team Alpha/ })
    .click();
  await check("resource selected");
  await page.getByRole("button", { name: "How matching works" }).click();
  await check("how matching works");
  await page.getByRole("button", { name: "How matching works" }).click();
  await request.post("/__test/state", {
    data: {
      status: "MATCHED",
      eventId: "12121212-1212-4212-8212-121212121212",
    },
  });
  await expect(
    page.getByRole("region", { name: "Notifications" }).getByRole("status"),
  ).toHaveCount(1);
  await check("toast");
  await page
    .getByRole("button", { name: "Register resource", exact: true })
    .click();
  await check("register form");
  await page.getByRole("button", { name: "Close creation form" }).click();
  await page.getByRole("button", { name: "List", exact: true }).click();
  await check("list view");
  expect(failures).toEqual([]);
});

test("surfaces share one radius and elevation, and focus has one ring", async ({
  page,
}) => {
  await page.goto("/");
  const styles = await page.evaluate(() =>
    [".panel", ".metric"].map((selector) => {
      const style = getComputedStyle(document.querySelector(selector));
      return [style.borderTopLeftRadius, style.boxShadow !== "none"];
    }),
  );
  expect(styles).toEqual([
    ["10px", true],
    ["10px", true],
  ]);
  const refresh = page.getByRole("button", { name: /Refresh data/ });
  await refresh.focus();
  await page.keyboard.press("Shift+Tab");
  await page.keyboard.press("Tab");
  await expect(refresh).toBeFocused();
  expect(
    await refresh.evaluate((e) => {
      const s = getComputedStyle(e);
      return [s.outlineStyle, s.outlineWidth, s.outlineColor];
    }),
  ).toEqual(["solid", "2px", "rgb(26, 113, 104)"]);
});

test.describe("dark mode", () => {
  const page_bg = (page) =>
    page.evaluate(
      () => getComputedStyle(document.documentElement).backgroundColor,
    );
  const LIGHT = "rgb(241, 244, 248)";
  const DARK = "rgb(15, 26, 36)";

  test("follows the system setting by default", async ({ page }) => {
    await page.emulateMedia({ colorScheme: "dark" });
    await page.goto("/");
    await expect(page.getByLabel("Colour theme")).toHaveValue("system");
    expect(await page_bg(page)).toBe(DARK);
    await page.emulateMedia({ colorScheme: "light" });
    expect(await page_bg(page)).toBe(LIGHT);
  });

  test("System, Light and Dark settings apply and are remembered", async ({
    page,
  }) => {
    await page.emulateMedia({ colorScheme: "light" });
    await page.goto("/");
    const theme = page.getByLabel("Colour theme");
    await theme.selectOption("dark");
    expect(await page_bg(page)).toBe(DARK);
    await page.reload();
    await expect(page.getByLabel("Colour theme")).toHaveValue("dark");
    expect(await page_bg(page)).toBe(DARK);
    await page.emulateMedia({ colorScheme: "dark" });
    await page.getByLabel("Colour theme").selectOption("light");
    expect(await page_bg(page)).toBe(LIGHT);
    await page.reload();
    expect(await page_bg(page)).toBe(LIGHT);
    await page.getByLabel("Colour theme").selectOption("system");
    expect(await page_bg(page)).toBe(DARK);
    await page.reload();
    await expect(page.getByLabel("Colour theme")).toHaveValue("system");
  });

  test("the setting works without browser storage", async ({ page }) => {
    await page.addInitScript(() => {
      const fail = () => {
        throw new Error("storage disabled");
      };
      Storage.prototype.getItem = fail;
      Storage.prototype.setItem = fail;
    });
    const errors = [];
    page.on("pageerror", (e) => errors.push(e.message));
    await page.emulateMedia({ colorScheme: "light" });
    await page.goto("/");
    await expect(page.getByLabel("Colour theme")).toHaveValue("system");
    await page.getByLabel("Colour theme").selectOption("dark");
    expect(await page_bg(page)).toBe(DARK);
    expect(errors).toEqual([]);
  });

  test("dark views meet AA, and the map stays readable", async ({
    page,
    request,
  }) => {
    const { contrastFailures } = await import("./contrast.js");
    await request.post("/__test/state", { data: { status: "WAITING" } });
    await create(request, "resources", {
      resourceType: "FOOD",
      name: "Food near",
      location: { lat: 19.09, lng: 72.89 },
      reusable: true,
      windowStart: "2026-11-05T00:00:00Z",
      windowEnd: "2026-11-06T00:00:00Z",
    });
    await page.emulateMedia({ colorScheme: "dark" });
    await page.goto("/");
    await expect(
      page.getByRole("status", { name: "Connection status" }),
    ).toContainText("Live");
    const failures = [];
    const check = async (view) =>
      failures.push(
        ...(await contrastFailures(page)).map((v) => `${view}: ${v}`),
      );
    await check("overview");
    await page
      .getByRole("region", { name: "Requests", exact: true })
      .getByRole("button", { name: /Water needed at the school/ })
      .click();
    await check("request selected");
    const map = page.getByRole("region", { name: "Dispatch map" });
    // A dimmed marker's reason stays readable.
    await map
      .getByRole("button", { name: /^Resource: Food near/ })
      .dispatchEvent("mouseover");
    await expect(map.locator(".leaflet-tooltip")).toBeVisible();
    await check("dimmed tooltip");
    await page.getByRole("button", { name: "How matching works" }).click();
    await check("how matching works");
    await page.getByRole("button", { name: "How matching works" }).click();
    await request.post("/__test/state", {
      data: {
        status: "MATCHED",
        eventId: "13131313-1313-4313-8313-131313131313",
      },
    });
    await expect(
      page.getByRole("region", { name: "Notifications" }).getByRole("status"),
    ).toHaveCount(1);
    await check("toast");
    await page
      .getByRole("button", { name: "Register resource", exact: true })
      .click();
    await check("register form");
    await page.getByRole("button", { name: "Close creation form" }).click();
    expect(failures).toEqual([]);
    // Map controls are themed; tiles get only a mild filter; pins keep colour.
    const look = await page.evaluate(() => {
      const css = (selector, prop) =>
        getComputedStyle(document.querySelector(selector))[prop];
      return {
        zoom: css(".leaflet-control-zoom a", "backgroundColor"),
        tiles: css(".leaflet-tile-pane", "filter"),
        markers: css(".leaflet-marker-pane", "filter"),
        radius: css("path.matching-radius", "stroke"),
        critical: getComputedStyle(
          document.querySelector(".urgency-critical"),
          "::after",
        ).backgroundColor,
      };
    });
    expect(look.zoom).toBe("rgb(22, 35, 47)");
    expect(look.tiles).toMatch(/^brightness\(0\.\d+\)/);
    expect(look.markers).toBe("none");
    expect(look.radius).toBe("rgb(127, 182, 224)");
    expect(look.critical).toBe("rgb(183, 62, 56)");
  });
  test("the record chooser and list view stay readable in dark", async ({
    page,
    request,
  }) => {
    const { contrastFailures } = await import("./contrast.js");
    for (const details of ["Blankets at the school", "Torches at the school"])
      await create(request, "requests", {
        requestType: "SHELTER",
        urgency: "HIGH",
        peopleAffected: 3,
        details,
        location: { lat: 19.08, lng: 72.88 },
      });
    await page.emulateMedia({ colorScheme: "dark" });
    await page.goto("/");
    await page
      .getByRole("region", { name: "Dispatch map" })
      .getByRole("button", { name: "3 records at this location" })
      .click();
    await expect(
      page.getByRole("list", { name: "Records at this location" }),
    ).toBeVisible();
    // Let Leaflet's pop-up fade-in finish.
    await expect
      .poll(() =>
        page.evaluate(
          () =>
            getComputedStyle(document.querySelector(".record-chooser")).opacity,
        ),
      )
      .toBe("1");
    const failures = await contrastFailures(page);
    await page.keyboard.press("Escape");
    await page.getByRole("button", { name: "List", exact: true }).click();
    failures.push(...(await contrastFailures(page)));
    expect(failures).toEqual([]);
  });
});

test("at 1366x768 the middle column fits without scrolling inside itself", async ({
  page,
}) => {
  await page.setViewportSize({ width: 1366, height: 768 });
  await page.goto("/");
  await expect(
    page.getByRole("region", { name: "Matching queue" }).getByRole("img", {
      name: /^Queue:/,
    }),
  ).toBeVisible();
  const fit = await page.evaluate(() => {
    const column = document.querySelector(".centre-column");
    return [column.scrollHeight, column.clientHeight];
  });
  expect(fit[0]).toBeLessThanOrEqual(fit[1]);
  // The map keeps a usable height.
  const map = await page
    .getByRole("region", { name: "Dispatch map" })
    .boundingBox();
  expect(map.height).toBeGreaterThanOrEqual(260);
});
