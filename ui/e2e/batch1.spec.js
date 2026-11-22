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
const hour = 3600000;
const openWindow = () => ({
  windowStart: new Date(Date.now() - hour).toISOString(),
  windowEnd: new Date(Date.now() + 6 * hour).toISOString(),
});
const resource = (
  request,
  name,
  resourceType,
  lat,
  lng,
  window = openWindow(),
) =>
  create(request, "resources", {
    resourceType,
    name,
    location: { lat, lng },
    reusable: true,
    ...window,
  });
const live = (page) =>
  expect(page.getByRole("status", { name: "Connection status" })).toContainText(
    "Live",
  );
const requestsList = (page) =>
  page.getByRole("region", { name: "Requests", exact: true });
const resourcesList = (page) =>
  page.getByRole("region", { name: "Resources", exact: true });
const selectWater = (page) =>
  requestsList(page)
    .getByRole("button", { name: /Water needed at the school/ })
    .click();
let eventNumber = 0;
const eventId = () =>
  `99999999-9999-4999-8999-${String(++eventNumber).padStart(12, "0")}`;

test.describe("matching radius", () => {
  test("a selected request shows its 50 km radius on both map modes", async ({
    page,
    request,
  }) => {
    await request.post("/__test/state", { data: { status: "WAITING" } });
    // 45 km and 60 km due north of the request at 19.08, 72.88.
    await resource(
      request,
      "Water north 45",
      "WATER",
      19.08 + 45 / 111.195,
      72.88,
    );
    await resource(
      request,
      "Water north 60",
      "WATER",
      19.08 + 60 / 111.195,
      72.88,
    );
    await page.goto("/");
    const map = page.getByRole("region", { name: "Dispatch map" });
    const circle = map.locator("path.matching-radius");
    const caption = map.getByText("Matching radius 50 km", { exact: true });
    await expect(circle).toHaveCount(0);
    await selectWater(page);
    await expect(circle).toHaveCount(1);
    await expect(caption).toBeVisible();
    await page.getByRole("button", { name: "Fit to results" }).click();
    await page.waitForTimeout(400);
    // The 45 km resource is inside the circle and the 60 km one outside.
    const top = (await circle.boundingBox()).y;
    const pinY = async (name) => {
      const box = await map
        .getByRole("button", { name: new RegExp(`Resource: ${name}`) })
        .boundingBox();
      return box.y + box.height / 2;
    };
    expect(await pinY("Water north 45")).toBeGreaterThan(top);
    expect(await pinY("Water north 60")).toBeLessThan(top);
    await page.getByRole("button", { name: "World overview" }).click();
    await expect(page.getByLabel("Map background status")).toContainText(
      "World overview",
    );
    await expect(circle).toHaveCount(1);
    await expect(caption).toBeAttached();
    await resourcesList(page)
      .getByRole("button", { name: /Water north 45/ })
      .click();
    await expect(circle).toHaveCount(0);
    await expect(caption).toHaveCount(0);
  });
});

test.describe("eligible resources", () => {
  test("ineligible resources are dimmed with one reason each", async ({
    page,
    request,
  }) => {
    // Water team Alpha (fixture) is unclaimed while WAITING; its window has closed.
    await request.post("/__test/state", { data: { status: "WAITING" } });
    await resource(request, "Water near", "WATER", 19.11, 72.91);
    // About 120 km north.
    await resource(request, "Water far", "WATER", 20.16, 72.88);
    await resource(request, "Food near", "FOOD", 19.09, 72.89);
    const busy = await resource(request, "Water busy", "WATER", 19.07, 72.87);
    await request.post("/__test/patch", {
      data: {
        id: busy.resourceId,
        set: {
          status: "ASSIGNED",
          activeClaim: { requestId: randomUUID(), attemptId: randomUUID() },
        },
      },
    });
    await page.goto("/");
    const map = page.getByRole("region", { name: "Dispatch map" });
    await expect(
      page.getByText(
        "Eligibility as estimated by the UI; the backend decides.",
      ),
    ).toBeVisible();
    const pin = (name) =>
      map.getByRole("button", { name: new RegExp(`^Resource: ${name}`) });
    // Nothing is dimmed until a request is selected.
    await expect(map.locator(".pin-dimmed")).toHaveCount(0);
    await selectWater(page);
    await page.getByRole("button", { name: "Fit to results" }).click();
    await page.waitForTimeout(400);
    await expect(pin("Water near")).not.toHaveClass(/pin-dimmed/);
    for (const [name, reason] of [
      ["Water far", "Too far: 120 km"],
      ["Water team Alpha", "Window closed"],
      ["Food near", "Wrong type"],
      ["Water busy", "Already assigned"],
    ]) {
      await expect(pin(name)).toHaveClass(/pin-dimmed/);
      await expect(pin(name)).toHaveAccessibleName(
        `Resource: ${name} · ${reason}`,
      );
      // Pins overlap at this zoom, so send the marker's own hover event.
      await pin(name).dispatchEvent("mouseover");
      await expect(
        map.getByRole("tooltip", {
          name: `Resource: ${name} · ${reason}`,
          exact: true,
        }),
      ).toBeVisible();
    }
    await pin("Water near").dispatchEvent("mouseover");
    await expect(
      map.getByRole("tooltip", { name: "Resource: Water near", exact: true }),
    ).toBeVisible();
    // Keyboard focus shows the same reason.
    await page.reload();
    await selectWater(page);
    await pin("Food near").focus();
    await expect(map.locator(".leaflet-tooltip")).toHaveText(
      "Resource: Food near · Wrong type",
    );
    // Selecting a resource removes the request's dimming.
    await resourcesList(page)
      .getByRole("button", { name: /Water near/ })
      .click();
    await expect(map.locator(".pin-dimmed")).toHaveCount(0);
  });

  test("the most relevant reason wins when several apply", async ({
    page,
    request,
  }) => {
    await request.post("/__test/state", { data: { status: "WAITING" } });
    // Far, wrong type and closed: distance is reported first.
    await resource(request, "Food far closed", "FOOD", 20.16, 72.88, {
      windowStart: "2026-11-01T00:00:00Z",
      windowEnd: "2026-11-02T00:00:00Z",
    });
    // Near, wrong type and closed: the window is reported first.
    await resource(request, "Food near closed", "FOOD", 19.09, 72.89, {
      windowStart: "2026-11-01T00:00:00Z",
      windowEnd: "2026-11-02T00:00:00Z",
    });
    await page.goto("/");
    const map = page.getByRole("region", { name: "Dispatch map" });
    await selectWater(page);
    await expect(
      map.getByRole("button", { name: /^Resource: Food far closed/ }),
    ).toHaveAccessibleName("Resource: Food far closed · Too far: 120 km");
    await expect(
      map.getByRole("button", { name: /^Resource: Food near closed/ }),
    ).toHaveAccessibleName("Resource: Food near closed · Window closed");
  });
});

test.describe("live toasts", () => {
  const toasts = (page) =>
    page.getByRole("region", { name: "Notifications" }).getByRole("status");

  test("a single match shows one closable toast that names the resource", async ({
    page,
    request,
  }) => {
    await request.post("/__test/state", { data: { status: "WAITING" } });
    await page.goto("/");
    await live(page);
    await request.post("/__test/state", {
      data: { status: "MATCHED", eventId: eventId() },
    });
    await expect(toasts(page)).toHaveCount(1);
    await expect(toasts(page)).toHaveText(
      "Request #11111111 matched to Water team Alpha",
    );
    await page.getByRole("button", { name: "Dismiss notification" }).click();
    await expect(toasts(page)).toHaveCount(0);
    for (const status of ["IN_PROGRESS", "FULFILLING", "FULFILLED"])
      await request.post("/__test/state", {
        data: { status, eventId: eventId() },
      });
    await expect(toasts(page)).toHaveText("Request #11111111 fulfilled");
    // Toasts close by themselves after 5 s.
    await page.waitForTimeout(5600);
    await expect(toasts(page)).toHaveCount(0);
  });

  test("a burst of 50 shows one grouped toast", async ({ page, request }) => {
    await page.goto("/");
    await live(page);
    await request.post("/__test/burst", { data: { count: 50 } });
    await expect(toasts(page)).toHaveText("50 requests matched");
    await page.waitForTimeout(1500);
    await expect(toasts(page)).toHaveCount(1);
  });

  test("a burst spread over more than a second still ends as one grouped toast", async ({
    page,
    request,
  }) => {
    await page.goto("/");
    await live(page);
    await request.post("/__test/burst", { data: { count: 10, spacing: 150 } });
    await expect(toasts(page).first()).toHaveText("10 requests matched");
    // Checked once, not retried: earlier toasts of the burst would only
    // disappear by expiring.
    expect(await toasts(page).allInnerTexts()).toEqual(["10 requests matched"]);
  });

  test("a request created 1 s before an unrelated burst keeps its own toast", async ({
    page,
    request,
  }) => {
    await page.goto("/");
    await live(page);
    const early = await create(request, "requests", {
      requestType: "FOOD",
      urgency: "NORMAL",
      peopleAffected: 2,
      details: "Early meals",
      location: { lat: 19.2, lng: 72.9 },
    });
    await request.post("/__test/patch", {
      data: {
        id: early.requestId,
        set: { status: "MATCHED" },
        eventId: eventId(),
      },
    });
    await page.waitForTimeout(1000);
    await request.post("/__test/burst", { data: { count: 50 } });
    await expect(toasts(page)).toHaveCount(2);
    await expect(toasts(page).nth(0)).toHaveText("50 requests matched");
    await expect(toasts(page).nth(1)).toHaveText(
      `Request #${early.requestId.slice(0, 8)} matched`,
    );
  });

  test("at most three toasts are shown, newest first", async ({
    page,
    request,
  }) => {
    const rows = [];
    for (let i = 0; i < 4; i++)
      rows.push(
        await create(request, "requests", {
          requestType: "FOOD",
          urgency: "NORMAL",
          peopleAffected: 1,
          details: "Meals " + i,
          location: { lat: 19.2 + i / 100, lng: 72.9 },
        }),
      );
    await page.goto("/");
    await live(page);
    for (const row of rows) {
      await request.post("/__test/patch", {
        data: {
          id: row.requestId,
          set: { status: "CANCELLED" },
          eventId: eventId(),
        },
      });
      await page.waitForTimeout(700);
    }
    await expect(toasts(page).first()).toHaveText(
      `Request #${rows[3].requestId.slice(0, 8)} cancelled`,
    );
    // Checked once, not retried: the oldest toasts expire after 5 s.
    expect(await toasts(page).allInnerTexts()).toEqual(
      [3, 2, 1].map(
        (i) => `Request #${rows[i].requestId.slice(0, 8)} cancelled`,
      ),
    );
  });

  test("no toasts for records already present on the initial page load", async ({
    page,
    request,
  }) => {
    await request.post("/__test/state", { data: { status: "MATCHED" } });
    await request.post("/__test/burst", { data: { count: 10 } });
    await page.goto("/");
    await live(page);
    await page.getByRole("button", { name: /Refresh data/ }).click();
    await page.waitForTimeout(2000);
    await expect(toasts(page)).toHaveCount(0);
  });
});

test("the timeline shows times since creation and the total wait", async ({
  page,
  request,
}) => {
  await page.goto("/");
  const details = page.getByRole("region", { name: "Request details" });
  const card = requestsList(page).getByRole("button", {
    name: /Water needed at the school/,
  });
  // The fixture request was created at 06:00:00.
  const steps = [
    ["WAITING", "06:00:01.500"],
    ["MATCHING", "06:00:06.500"],
    ["WAITING", "06:00:07.000"],
    ["MATCHING", "06:02:13.500"],
    ["MATCHED", "06:02:15.000"],
    ["IN_PROGRESS", "06:03:00.000"],
    ["FULFILLING", "06:10:20.000"],
    ["FULFILLED", "06:10:30.000"],
  ];
  for (const [status, at] of steps) {
    await request.post("/__test/state", {
      data: { status, updatedAt: `2026-11-05T${at}Z` },
    });
    await page.getByRole("button", { name: /Refresh data/ }).click();
    await expect(
      card.getByText(status.replaceAll("_", " "), { exact: true }),
    ).toBeVisible();
  }
  await card.click();
  const entry = (name) =>
    details.locator(".timeline-items li").filter({
      has: page.getByText(name, { exact: true }),
    });
  await expect(entry("Matched")).toContainText(
    "Matched 2 min 15 s after creation",
  );
  await expect(entry("Fulfilled")).toContainText(
    "Fulfilled 10 min 30 s after creation",
  );
  // From the first wait until the last wait ended.
  await expect(entry("Waiting").last()).toContainText("Waited 2 min 12 s");
  await expect(entry("Waiting").first()).not.toContainText("Waited");
  await expect(details.getByText(/after creation|Waited/)).toHaveCount(3);
});

test("a match within a minute is shown to a tenth of a second", async ({
  page,
  request,
}) => {
  await page.goto("/");
  const card = requestsList(page).getByRole("button", {
    name: /Water needed at the school/,
  });
  for (const [status, at] of [
    ["MATCHED", "06:00:03.800"],
    ["CANCELLING", "06:00:20.000"],
    ["CANCELLED", "06:00:21.250"],
  ]) {
    await request.post("/__test/state", {
      data: { status, updatedAt: `2026-11-05T${at}Z` },
    });
    await page.getByRole("button", { name: /Refresh data/ }).click();
    await expect(card.getByText(status, { exact: true })).toBeVisible();
  }
  await card.click();
  const details = page.getByRole("region", { name: "Request details" });
  await expect(details).toContainText("Matched 3.8 s after creation");
  await expect(details).toContainText("Cancelled 21.3 s after creation");
});

test("Register resource defaults Available until to six hours after Available from", async ({
  page,
  request,
}) => {
  await page.goto("/");
  await page
    .getByRole("button", { name: "Register resource", exact: true })
    .click();
  const form = page.getByRole("dialog");
  const from = form.getByLabel("Available from (local time)");
  const until = form.getByLabel("Available until (local time)");
  const gap = async () =>
    (new Date(await until.inputValue()) - new Date(await from.inputValue())) /
    hour;
  expect(await gap()).toBe(6);
  // It follows Available from until edited, and stays editable.
  await from.fill("2026-12-01T08:30");
  await expect(until).toHaveValue("2026-12-01T14:30");
  await until.fill("2026-12-01T10:00");
  await from.fill("2026-12-01T09:00");
  await expect(until).toHaveValue("2026-12-01T10:00");
  await form.getByLabel("Resource name").fill("Six hour van");
  await form.getByLabel("Latitude", { exact: true }).fill("19.1");
  await form.getByLabel("Longitude", { exact: true }).fill("72.9");
  await form
    .getByRole("button", { name: "Register resource", exact: true })
    .click();
  await expect(
    form.getByRole("status", { name: "Creation result" }),
  ).toBeVisible();
  const { calls } = await (await request.get("/__test/creations")).json();
  const body = JSON.parse(calls.at(-1).body);
  expect((new Date(body.windowEnd) - new Date(body.windowStart)) / hour).toBe(
    1,
  );
});

test("a new resource form also defaults to a six hour window when submitted unchanged", async ({
  page,
  request,
}) => {
  await page.goto("/");
  await page
    .getByRole("button", { name: "Register resource", exact: true })
    .click();
  const form = page.getByRole("dialog");
  await form.getByLabel("Resource name").fill("Default van");
  await form.getByLabel("Latitude", { exact: true }).fill("19.1");
  await form.getByLabel("Longitude", { exact: true }).fill("72.9");
  await form
    .getByRole("button", { name: "Register resource", exact: true })
    .click();
  await expect(
    form.getByRole("status", { name: "Creation result" }),
  ).toBeVisible();
  const { calls } = await (await request.get("/__test/creations")).json();
  const body = JSON.parse(calls.at(-1).body);
  expect((new Date(body.windowEnd) - new Date(body.windowStart)) / hour).toBe(
    6,
  );
});

test("the shared timer clears toast state once toasts have expired", async ({
  page,
  request,
}) => {
  await page.clock.install();
  await request.post("/__test/state", { data: { status: "WAITING" } });
  await page.goto("/");
  await live(page);
  const region = page.getByRole("region", { name: "Notifications" });
  const held = async () => Number(await region.getAttribute("data-held"));
  await request.post("/__test/state", {
    data: { status: "MATCHED", eventId: eventId() },
  });
  await expect(region.getByRole("status")).toHaveText(
    "Request #11111111 matched to Water team Alpha",
  );
  await page.clock.runFor(5000);
  expect(await held()).toBeGreaterThan(0);
  // Burst bookkeeping outlives the toast; only the timer removes it.
  await page.clock.runFor(25000);
  await expect.poll(held).toBe(0);
  await expect(region.getByRole("status")).toHaveCount(0);
});
