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
