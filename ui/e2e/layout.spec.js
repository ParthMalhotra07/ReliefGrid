import { randomUUID } from "node:crypto";
import { test, expect } from "./test-support.js";

test.beforeEach(async ({ request }) => {
  await request.post("/__test/reset");
});

const create = (request, kind, data) =>
  request.post(`/api/v1/${kind}`, {
    headers: { "Idempotency-Key": randomUUID() },
    data,
  });
const manyRequests = async (request, count) => {
  for (let i = 0; i < count; i++)
    await create(request, "requests", {
      requestType: "FOOD",
      urgency: "NORMAL",
      peopleAffected: 2,
      details: `Meals for shelter ${i + 1}`,
      location: { lat: 19 + i / 200, lng: 72.85 },
    });
};
const live = (page) =>
  expect(page.getByRole("status", { name: "Connection status" })).toContainText(
    "Live",
  );
// Order of the given regions from top to bottom of the page.
const order = (page, names) =>
  page.evaluate(
    (names) =>
      names
        .map((name) => {
          const element = document.querySelector(`[aria-label="${name}"]`);
          return [name, element.getBoundingClientRect().top + scrollY];
        })
        .sort((a, b) => a[1] - b[1])
        .map(([name]) => name),
    names,
  );

test("at 1366x768 the map stays in view and the middle column has no tall empty band", async ({
  page,
  request,
}) => {
  await page.setViewportSize({ width: 1366, height: 768 });
  await manyRequests(request, 30);
  await request.post("/__test/state", { data: { status: "WAITING" } });
  await page.goto("/");
  const card = page
    .getByRole("region", { name: "Requests", exact: true })
    .getByRole("button", { name: /Water needed at the school/ });
  // A long timeline for the selected request.
  for (const status of [
    "MATCHING",
    "RETRYING",
    "WAITING",
    "MATCHING",
    "RETRYING",
    "WAITING",
    "MATCHING",
    "MATCHED",
  ]) {
    await request.post("/__test/state", { data: { status } });
    await page.getByRole("button", { name: /Refresh data/ }).click();
    await expect(card.getByText(status, { exact: true })).toBeVisible();
  }
  await card.click();
  const map = page.getByRole("region", { name: "Dispatch map" });
  const band = () =>
    page.evaluate(() => {
      const column = document.querySelector(".centre-column");
      const layout = document.querySelector(".dispatch-layout");
      const bottom = Math.min(
        innerHeight,
        layout.getBoundingClientRect().bottom,
      );
      const top = Math.max(0, column.getBoundingClientRect().top);
      const last = Math.max(
        top,
        ...[...column.querySelectorAll(":scope > *")].map(
          (v) => v.getBoundingClientRect().bottom,
        ),
      );
      return Math.max(0, bottom - Math.min(last, bottom));
    });
  const pageHeight = await page.evaluate(
    () => document.documentElement.scrollHeight,
  );
  expect(pageHeight).toBeGreaterThan(1300);
  // Scroll the request list itself, then the page past the lists and the
  // long timeline down to the footer.
  await page
    .locator(".request-cards")
    .evaluate((list) => list.scrollTo(0, list.scrollHeight));
  const max = pageHeight - 768;
  const measured = [];
  for (const y of [300, 500, 700, 900, max]) {
    if (y > max) continue;
    await page.evaluate((y) => scrollTo(0, y), y);
    await page.waitForTimeout(100);
    await expect(map).toBeInViewport({ ratio: 0.9 });
    measured.push(await band());
  }
  expect(measured.length).toBeGreaterThanOrEqual(3);
  for (const height of measured) expect(height).toBeLessThanOrEqual(200);
  // The status panels and the feed sit in the middle column under the map.
  const below = await page.evaluate(() => {
    const box = (name) =>
      document.querySelector(`[aria-label="${name}"]`).getBoundingClientRect();
    const mapBox = box("Area overview");
    return ["Matching queue", "Shard reads", "Activity"].map((name) => {
      const b = box(name);
      return (
        b.top >= mapBox.bottom - 1 &&
        b.left >= mapBox.left - 1 &&
        b.right <= mapBox.right + 1
      );
    });
  });
  expect(below).toEqual([true, true, true]);
});

test("at 390 px the original order is kept, with the feed after the map", async ({
  page,
}) => {
  await page.setViewportSize({ width: 390, height: 844 });
  await page.goto("/");
  await live(page);
  expect(
    await order(page, [
      "Area overview",
      "Activity",
      "Requests",
      "Resources",
      "Request details",
      "Shard reads",
      "Matching queue",
    ]),
  ).toEqual([
    "Area overview",
    "Activity",
    "Requests",
    "Resources",
    "Request details",
    "Shard reads",
    "Matching queue",
  ]);
  // Not sticky on mobile: the map scrolls away with the page.
  await page.locator("footer").scrollIntoViewIfNeeded();
  await expect(
    page.getByRole("region", { name: "Dispatch map" }),
  ).not.toBeInViewport();
});

test("the activity feed shows live events, groups a burst and selects records", async ({
  page,
  request,
}) => {
  await page.goto("/");
  await live(page);
  const feed = page.getByRole("region", { name: "Activity" });
  const items = feed.getByRole("listitem");
  await create(request, "resources", {
    resourceType: "FOOD",
    name: "Food team Bravo",
    location: { lat: 19.2, lng: 72.95 },
    reusable: true,
    windowStart: "2026-11-05T00:00:00Z",
    windowEnd: "2026-11-06T00:00:00Z",
  });
  await expect(items.first()).toContainText("Resource registered", {
    timeout: 4000,
  });
  await expect(items.first()).toContainText("Food team Bravo");
  await expect(items.first()).toContainText(/just now|\d+ s ago/);
  await request.post("/__test/state", {
    data: {
      status: "MATCHED",
      eventId: "88888888-8888-4888-8888-888888888888",
    },
  });
  await expect(items.first()).toContainText("Request matched", {
    timeout: 4000,
  });
  await expect(items.first()).toContainText("Water needed at the school");
  await items.first().getByRole("button").click();
  await expect(
    page.getByRole("region", { name: "Request details" }),
  ).toContainText("Water needed at the school");
  await feed
    .getByRole("listitem")
    .filter({ hasText: "Food team Bravo" })
    .getByRole("button")
    .click();
  await expect(
    page.getByRole("region", { name: "Resource details" }),
  ).toContainText("Food team Bravo");
  // The burst follows the earlier events closely; they keep their own lines.
  await request.post("/__test/burst", { data: { count: 50 } });
  await expect(items.first()).toContainText("50 requests matched", {
    timeout: 5000,
  });
  // The burst is one feed entry, and the earlier events are still listed.
  await expect(items).toHaveCount(3);
  await items.first().getByRole("button").click();
  await expect(
    page.getByRole("region", { name: "Request details" }),
  ).toContainText(/Burst request \d+/);
});

test("Show on map brings the map into view centred on the record at 390 px", async ({
  page,
}) => {
  await page.setViewportSize({ width: 390, height: 844 });
  await page.goto("/");
  await live(page);
  await page
    .getByRole("region", { name: "Requests", exact: true })
    .getByRole("button", { name: /Water needed at the school/ })
    .click();
  const map = page.getByRole("region", { name: "Dispatch map" });
  // Move the map away from the record, then scroll down to its details.
  await page.getByRole("button", { name: "Fit globe" }).click();
  const details = page.getByRole("region", { name: "Request details" });
  await details.scrollIntoViewIfNeeded();
  await expect(map).not.toBeInViewport();
  await details.getByRole("button", { name: "Show on map" }).click();
  await expect(map).toBeInViewport({ ratio: 0.9 });
  const pin = map.getByRole("button", {
    name: "Request: Water needed at the school",
  });
  await expect
    .poll(async () => {
      const [m, p] = [await map.boundingBox(), await pin.boundingBox()];
      return Math.hypot(
        m.x + m.width / 2 - (p.x + p.width / 2),
        m.y + m.height / 2 - (p.y + p.height / 2),
      );
    })
    .toBeLessThan(12);
  await expect(page.locator(".leaflet-control-scale-line").first()).toHaveText(
    /^(\d+ m|\d km)$/,
  );
});

test("Show on map is offered only on mobile widths", async ({ page }) => {
  await page.setViewportSize({ width: 1366, height: 768 });
  await page.goto("/");
  await page
    .getByRole("region", { name: "Requests", exact: true })
    .getByRole("button", { name: /Water needed at the school/ })
    .click();
  await expect(
    page
      .getByRole("region", { name: "Request details" })
      .getByRole("button", { name: "Show on map" }),
  ).toBeHidden();
});

test("Show on map is disabled while filters hide the selected record (390 px)", async ({
  page,
}) => {
  // Parth's reproduction from the #196 review.
  await page.setViewportSize({ width: 390, height: 844 });
  await page.goto("/");
  await live(page);
  await page
    .getByRole("region", { name: "Requests", exact: true })
    .getByRole("button", { name: /Water needed at the school/ })
    .click();
  const details = page.getByRole("region", { name: "Request details" });
  const show = details.getByRole("button", { name: "Show on map" });
  await expect(show).toBeEnabled();
  await page.getByLabel("Search requests or resources").fill("no such record");
  await expect(show).toBeDisabled();
  await expect(details.getByText("Hidden by current filters")).toBeVisible();
  // Clearing the search makes the pin and the action available again.
  await page.getByLabel("Search requests or resources").fill("");
  await expect(details.getByText("Hidden by current filters")).toHaveCount(0);
  await show.click();
  const map = page.getByRole("region", { name: "Dispatch map" });
  await expect(map).toBeInViewport({ ratio: 0.9 });
  await expect(
    map.getByRole("button", { name: "Request: Water needed at the school" }),
  ).toBeInViewport();
  // A resource hidden by the type filter.
  await page
    .getByRole("region", { name: "Resources", exact: true })
    .getByRole("button", { name: /Water team Alpha/ })
    .click();
  const resource = page.getByRole("region", { name: "Resource details" });
  await page.getByLabel("Resource type").selectOption("FOOD");
  await expect(
    resource.getByRole("button", { name: "Show on map" }),
  ).toBeDisabled();
  await expect(resource.getByText("Hidden by current filters")).toBeVisible();
});

test("the feed stays bounded through a two-minute read outage", async ({
  page,
  request,
}) => {
  // Parth's simulation: 50 events a second for two minutes after the
  // baseline, with every read failing. The page clock drives the timers.
  await page.clock.install();
  await page.goto("/");
  await live(page);
  await expect(page.getByRole("region", { name: "Requests" })).toContainText(
    "LATEST READ",
  );
  const feed = page.getByRole("region", { name: "Activity" });
  const held = async () => Number(await feed.getAttribute("data-held"));
  await request.post("/__test/state", { data: { fail: true } });
  let peak = 0;
  for (let second = 0; second < 120; second += 5) {
    await request.post("/__test/events", { data: { count: 250 } });
    await page.waitForTimeout(150);
    await page.clock.runFor(5000);
    peak = Math.max(peak, await held());
    expect(await held()).toBeLessThanOrEqual(200);
  }
  expect(peak).toBeGreaterThan(0);
  // With no further events, the timer alone removes everything past 30 s.
  await page.clock.runFor(35000);
  await expect.poll(held).toBe(0);
  // Toast state stays bounded too (#197).
  const notices = page.getByRole("region", { name: "Notifications" });
  if (await notices.count())
    expect(Number(await notices.getAttribute("data-held"))).toBeLessThanOrEqual(
      200,
    );
});
