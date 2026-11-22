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

test("records sharing one location can each be chosen from the map", async ({
  page,
  request,
}) => {
  const shared = { lat: 18.95, lng: 72.83 };
  const titles = [
    "Tarps at the depot",
    "Insulin at the depot",
    "Rice at the depot",
  ];
  for (const details of titles)
    await create(request, "requests", {
      requestType: "SHELTER",
      urgency: "HIGH",
      peopleAffected: 3,
      details,
      location: shared,
    });
  await page.goto("/");
  const map = page.getByRole("region", { name: "Dispatch map" });
  const details = page.getByRole("region", { name: "Request details" });
  for (const title of titles) {
    const point = map.getByRole("button", {
      name: "3 records at this location",
    });
    await point.focus();
    await page.keyboard.press("Enter");
    const chooser = page.getByRole("list", {
      name: "Records at this location",
    });
    await expect(chooser).toBeVisible();
    await expect(chooser.getByRole("button")).toHaveCount(3);
    const choice = chooser.getByRole("button", { name: new RegExp(title) });
    await expect(choice).toContainText("Shelter");
    await expect(choice).toContainText("WAITING");
    await choice.focus();
    await page.keyboard.press("Enter");
    await expect(details).toContainText(title);
  }
});

test("a live refresh keeps keyboard focus on the same map pin", async ({
  page,
  request,
}) => {
  await page.goto("/");
  const map = page.getByRole("region", { name: "Dispatch map" });
  await expect(
    page.getByRole("status", { name: "Connection status" }),
  ).toContainText("Live");
  const pin = map.getByRole("button", { name: "Resource: Water team Alpha" });
  await pin.focus();
  await expect(pin).toBeFocused();
  await create(request, "resources", {
    resourceType: "FOOD",
    name: "Food team Bravo",
    location: { lat: 19.2, lng: 72.95 },
    reusable: true,
    windowStart: "2026-11-05T00:00:00Z",
    windowEnd: "2026-11-06T00:00:00Z",
  });
  await expect(
    map.getByRole("button", { name: "Resource: Food team Bravo" }),
  ).toBeAttached();
  await expect(pin).toBeFocused();
});

test("a selected request's actions are visible at 1366x768 without scrolling", async ({
  page,
  request,
}) => {
  await page.setViewportSize({ width: 1366, height: 768 });
  await request.post("/__test/state", { data: { status: "WAITING" } });
  await page.goto("/");
  const card = page
    .getByRole("region", { name: "Requests", exact: true })
    .getByRole("button", { name: /Water needed at the school/ });
  // A request that has been retried builds an observed history before matching.
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
  const rows = page
    .getByRole("region", { name: "Request details" })
    .locator(".timeline-items li");
  await expect.poll(() => rows.count()).toBeGreaterThanOrEqual(8);
  await page.evaluate(() => window.scrollTo(0, 0));
  const details = page.getByRole("region", { name: "Request details" });
  for (const name of ["Start work", "Cancel request"])
    await expect(details.getByRole("button", { name })).toBeInViewport({
      ratio: 1,
    });
});

test("request cards explain only reasons that apply to the current state", async ({
  page,
  request,
}) => {
  await page.goto("/");
  const card = page
    .getByRole("region", { name: "Requests", exact: true })
    .getByRole("button", { name: /Water needed at the school/ });
  for (const [status, lastError] of [
    ["FULFILLED", "RESOURCE_AVAILABLE"],
    ["CANCELLED", "NO_CANDIDATE"],
    ["MATCHED", "NO_CANDIDATE"],
    ["IN_PROGRESS", "ATTEMPT_CLOSED"],
  ]) {
    await request.post("/__test/state", { data: { status, lastError } });
    await page.getByRole("button", { name: /Refresh data/ }).click();
    await expect(
      card.getByText(status.replaceAll("_", " "), { exact: true }),
    ).toBeVisible();
    await expect(card).not.toContainText(lastError);
    await expect(card.locator(".retry-reason")).toHaveCount(0);
  }
  await request.post("/__test/state", {
    data: { status: "WAITING", lastError: "NO_CANDIDATE" },
  });
  await page.getByRole("button", { name: /Refresh data/ }).click();
  await expect(card).toContainText("No matching resource within 50 km yet");
  await expect(card).not.toContainText("NO_CANDIDATE");
});

test("Fit globe returns to the whole world and the mode label follows the zoom", async ({
  page,
}) => {
  await page.goto("/");
  const map = page.getByRole("region", { name: "Dispatch map" });
  const status = page.getByLabel("Map background status");
  const scale = page.locator(".leaflet-control-scale-line");
  await expect(status).toHaveText("Street map");
  await page.getByRole("button", { name: "Fit globe" }).click();
  await expect(scale).toHaveText(/000 km$/);
  await expect(status).toHaveText("Street map");
  await page.getByRole("button", { name: "World overview" }).click();
  await expect(status).toHaveText("World overview · no street detail");
  await page
    .getByRole("region", { name: "Requests", exact: true })
    .getByRole("button", { name: /Water needed at the school/ })
    .click();
  await expect(scale).toHaveText(/^(\d+ m|\d km)$/);
  await expect(status).not.toContainText("World overview");
  await expect(status).toHaveText("Outline map · no street detail");
  await page.getByRole("button", { name: "Fit globe" }).click();
  await expect(status).toHaveText("World overview · no street detail");
  await expect(map.getByText("Asia", { exact: true })).toBeVisible();
  await page.getByRole("button", { name: "Fit to results" }).click();
  await expect(status).toHaveText("Outline map · no street detail");
  await expect(map.getByText("Asia", { exact: true })).toHaveCount(0);
});

test("a globe request during a running zoom animation still ends on the globe", async ({
  page,
}) => {
  await page.goto("/");
  const status = page.getByLabel("Map background status");
  const scale = page.locator(".leaflet-control-scale-line");
  await expect(status).toHaveText("Street map");
  for (const name of ["Fit globe", "World overview"]) {
    await page.getByRole("button", { name: "Fit to results" }).click();
    await expect(scale).not.toHaveText(/000 km$/);
    // Zoom out animates; the globe request arrives before the animation ends.
    await page.getByRole("button", { name: "Zoom out" }).click();
    await page.getByRole("button", { name }).click();
    await page.waitForTimeout(600);
    await expect(scale).toHaveText(/000 km$/);
    await expect(status).toContainText(
      name === "Fit globe" ? "Street map" : "World overview",
    );
  }
});
