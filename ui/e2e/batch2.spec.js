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
const patch = (request, id, set) =>
  request.post("/__test/patch", { data: { id, set } });
const newRequest = (request, details, urgency, lat, lng = 72.85) =>
  create(request, "requests", {
    requestType: "FOOD",
    urgency,
    peopleAffected: 2,
    details,
    location: { lat, lng },
  });
const newResource = (request, name, resourceType, lat, window = {}) =>
  create(request, "resources", {
    resourceType,
    name,
    location: { lat, lng: 72.95 },
    reusable: true,
    windowStart: "2026-11-05T00:00:00Z",
    windowEnd: "2026-11-06T00:00:00Z",
    ...window,
  });
const map = (page) => page.getByRole("region", { name: "Dispatch map" });
const pin = (page, name) =>
  map(page).getByRole("button", { name: new RegExp(`^${name}`) });
const after = (locator, property) =>
  locator.evaluate(
    (element, property) =>
      getComputedStyle(element, "::after").getPropertyValue(property),
    property,
  );
const before = (locator, property) =>
  locator.evaluate(
    (element, property) =>
      getComputedStyle(element, "::before").getPropertyValue(property),
    property,
  );
const requestCards = (page) =>
  page
    .getByRole("region", { name: "Requests", exact: true })
    .locator(".request-card");

test.describe("markers by meaning", () => {
  test("request markers are sized and coloured by urgency", async ({
    page,
    request,
  }) => {
    await newRequest(request, "Normal meals", "NORMAL", 18.95);
    await newRequest(request, "High meals", "HIGH", 19.25);
    await page.goto("/");
    // The fixture request is CRITICAL.
    const rows = [
      ["Request: Normal meals", "urgency-normal", "15px", "rgb(26, 113, 104)"],
      ["Request: High meals", "urgency-high", "19px", "rgb(153, 88, 16)"],
      [
        "Request: Water needed at the school",
        "urgency-critical",
        "21px",
        "rgb(183, 62, 56)",
      ],
    ];
    for (const [name, className, size, colour] of rows) {
      const marker = pin(page, name);
      await expect(marker).toHaveClass(new RegExp(className));
      expect(await after(marker, "width")).toBe(size);
      expect(await after(marker, "height")).toBe(size);
      expect(await after(marker, "background-color")).toBe(colour);
    }
    // Only the critical, unfinished request pulses.
    expect(await before(pin(page, rows[2][0]), "animation-name")).toBe(
      "pin-pulse",
    );
    for (const [name] of rows.slice(0, 2))
      expect(await before(pin(page, name), "animation-name")).toBe("none");
    const legend = page.locator(".map-legend");
    for (const text of [
      "Critical request",
      "High urgency",
      "Normal request",
      "Water",
      "Food",
      "Medicine",
      "Transport",
      "Shelter",
      "Assignment",
    ])
      await expect(legend).toContainText(text);
    await expect(legend.locator("svg")).toHaveCount(5);
  });

  test("reduced motion stops the critical pulse", async ({ page }) => {
    await page.emulateMedia({ reducedMotion: "reduce" });
    await page.goto("/");
    const critical = pin(page, "Request: Water needed at the school");
    await expect(critical).toHaveClass(/pin-pulse/);
    expect(await before(critical, "animation-name")).toBe("none");
  });

  test("a finished critical request does not pulse", async ({
    page,
    request,
  }) => {
    await request.post("/__test/state", { data: { status: "CANCELLED" } });
    await page.goto("/");
    const critical = pin(page, "Request: Water needed at the school");
    await expect(critical).toHaveClass(/pin-terminal/);
    await expect(critical).not.toHaveClass(/pin-pulse/);
  });

  test("each resource type shows its own local icon", async ({
    page,
    request,
  }) => {
    const external = [];
    page.on("request", (r) => {
      const { host } = new URL(r.url());
      if (!["127.0.0.1:3412", "tile.openstreetmap.org"].includes(host))
        external.push(r.url());
    });
    const types = [
      ["FOOD", "food"],
      ["MEDICINE", "medicine"],
      ["TRANSPORTATION", "vehicle"],
      ["SHELTER", "house"],
    ];
    let lat = 18.9;
    for (const [type] of types)
      await newResource(request, `${type} unit`, type, (lat += 0.08));
    await page.goto("/");
    // The fixture resource is WATER.
    for (const [name, icon] of [
      ["Resource: Water team Alpha", "water"],
      ...types.map(([type, icon]) => [`Resource: ${type} unit`, icon]),
    ]) {
      const marker = pin(page, name);
      await expect(marker.locator(`svg[data-icon="${icon}"]`)).toHaveCount(1);
      await expect(marker.locator("svg")).toHaveCount(1);
    }
    expect(external).toEqual([]);
  });
});

test.describe("request list sorting", () => {
  test("each sort order is correct and remembered", async ({
    page,
    request,
  }) => {
    // Fixture request: CRITICAL, created 06:00, CANCELLING.
    const rows = [
      ["Old normal waiting", "NORMAL", "05:00", "WAITING", "05:40"],
      ["New high waiting", "HIGH", "07:00", "WAITING", "07:05"],
      ["Mid critical matched", "CRITICAL", "06:30", "MATCHED", "06:31"],
      ["Mid normal retrying", "NORMAL", "06:10", "RETRYING", "06:15"],
    ];
    let lat = 18.9;
    for (const [details, urgency, created, status, updated] of rows) {
      const row = await newRequest(request, details, urgency, (lat += 0.05));
      await patch(request, row.requestId, {
        status,
        createdAt: `2026-11-05T${created}:00Z`,
        updatedAt: `2026-11-05T${updated}:00Z`,
      });
    }
    await page.goto("/");
    const titles = () => requestCards(page).locator("strong").allInnerTexts();
    const sort = page.getByLabel("Sort requests");
    await expect(sort).toHaveValue("urgency");
    // Urgency, then oldest created first.
    await expect
      .poll(titles)
      .toEqual([
        "Water needed at the school",
        "Mid critical matched",
        "New high waiting",
        "Old normal waiting",
        "Mid normal retrying",
      ]);
    // Waiting and retrying first, longest wait first; then the rest, oldest first.
    await sort.selectOption("oldest-waiting");
    await expect
      .poll(titles)
      .toEqual([
        "Old normal waiting",
        "Mid normal retrying",
        "New high waiting",
        "Water needed at the school",
        "Mid critical matched",
      ]);
    await sort.selectOption("newest");
    await expect
      .poll(titles)
      .toEqual([
        "New high waiting",
        "Mid critical matched",
        "Mid normal retrying",
        "Water needed at the school",
        "Old normal waiting",
      ]);
    await page.reload();
    await expect(page.getByLabel("Sort requests")).toHaveValue("newest");
    await expect
      .poll(titles)
      .toEqual([
        "New high waiting",
        "Mid critical matched",
        "Mid normal retrying",
        "Water needed at the school",
        "Old normal waiting",
      ]);
  });

  test("a sort still works when browser storage is unavailable", async ({
    page,
  }) => {
    await page.addInitScript(() => {
      const fail = () => {
        throw new Error("storage disabled");
      };
      Storage.prototype.getItem = fail;
      Storage.prototype.setItem = fail;
    });
    await page.goto("/");
    const sort = page.getByLabel("Sort requests");
    await expect(sort).toHaveValue("urgency");
    await sort.selectOption("newest");
    await expect(sort).toHaveValue("newest");
  });

  test("waiting badges count from the first Waiting entry or creation", async ({
    page,
    request,
  }) => {
    await page.clock.install({ time: new Date("2026-11-05T06:12:30Z") });
    const retrying = await newRequest(
      request,
      "Retrying meals",
      "NORMAL",
      19.2,
    );
    await patch(request, retrying.requestId, {
      status: "RETRYING",
      createdAt: "2026-11-05T06:07:00Z",
      updatedAt: "2026-11-05T06:08:00Z",
    });
    const matched = await newRequest(request, "Matched meals", "NORMAL", 19.3);
    await patch(request, matched.requestId, {
      status: "MATCHED",
      createdAt: "2026-11-05T05:00:00Z",
    });
    await page.goto("/");
    const card = (title) => requestCards(page).filter({ hasText: title });
    const refresh = async () => {
      await page.getByRole("button", { name: /Refresh data/ }).click();
      await page.clock.runFor(500);
    };
    // The fixture request (created 06:00:00) first waits at 06:03:05, then
    // retries.
    for (const [status, at] of [
      ["WAITING", "06:03:05"],
      ["MATCHING", "06:03:10"],
      ["WAITING", "06:03:11"],
    ]) {
      await request.post("/__test/state", {
        data: { status, updatedAt: `2026-11-05T${at}Z` },
      });
      await refresh();
      await expect(card("Water needed at the school")).toContainText(status);
    }
    const badge = (title) => card(title).locator(".waiting-badge");
    await expect(badge("Water needed at the school")).toHaveText(
      "waiting 9 min",
    );
    // No Waiting entry observed: counted from creation (06:07:00).
    await expect(badge("Retrying meals")).toHaveText("waiting 5 min");
    await expect(badge("Matched meals")).toHaveCount(0);
    // The badges refresh every 30 s.
    await page.clock.runFor(60000);
    await expect(badge("Water needed at the school")).toHaveText(
      "waiting 10 min",
    );
    await expect(badge("Retrying meals")).toHaveText("waiting 6 min");
    // Long waits read in hours.
    await page.clock.runFor(60 * 60000);
    await expect(badge("Water needed at the school")).toHaveText(
      "waiting 1 h 10 min",
    );
  });
});

test.describe("resource window badges", () => {
  test("open, future and expired windows with a controlled clock", async ({
    page,
    request,
  }) => {
    const now = new Date("2026-11-05T12:00:00Z").getTime();
    const at = (minutes) => new Date(now + minutes * 60000).toISOString();
    await page.clock.install({ time: now });
    await newResource(request, "Open van", "WATER", 19.15, {
      windowStart: at(-60),
      windowEnd: at(45),
    });
    await newResource(request, "Future van", "WATER", 19.2, {
      windowStart: at(20),
      windowEnd: at(180),
    });
    await newResource(request, "Expired van", "WATER", 19.05, {
      windowStart: at(-180),
      windowEnd: at(-60),
    });
    await request.post("/__test/state", { data: { status: "WAITING" } });
    await page.goto("/");
    const list = page.getByRole("region", { name: "Resources", exact: true });
    const badge = (name) =>
      list
        .locator(".resource-card", { hasText: name })
        .locator(".window-badge");
    await expect(badge("Open van")).toHaveText("Open · closes in 45 min");
    await expect(badge("Future van")).toHaveText("Opens in 20 min");
    await expect(badge("Expired van")).toHaveText("Expired");
    // The fixture resource's window ends at midnight UTC.
    await expect(badge("Water team Alpha")).toHaveText(
      "Open · closes in 12 h 0 min",
    );
    // The details panel shows the same state.
    await list.getByRole("button", { name: /Future van/ }).click();
    const details = page.getByRole("region", { name: "Resource details" });
    await expect(details.locator(".window-badge")).toHaveText(
      "Opens in 20 min",
    );
    // A closed window is the dimming reason "Window closed" for the same resource.
    await page
      .getByRole("region", { name: "Requests", exact: true })
      .getByRole("button", { name: /Water needed at the school/ })
      .click();
    await expect(pin(page, "Resource: Expired van")).toHaveAccessibleName(
      "Resource: Expired van · Window closed",
    );
    await expect(pin(page, "Resource: Open van")).toHaveAccessibleName(
      "Resource: Open van",
    );
    // Refreshes every 30 s with the browser clock.
    await page.clock.runFor(15 * 60000);
    await expect(badge("Open van")).toHaveText("Open · closes in 30 min");
    await expect(badge("Future van")).toHaveText("Opens in 5 min");
    await page.clock.runFor(6 * 60000);
    await expect(badge("Future van")).toHaveText("Open · closes in 2 h 39 min");
    await page.clock.runFor(30 * 60000);
    await expect(badge("Open van")).toHaveText("Expired");
  });
});
