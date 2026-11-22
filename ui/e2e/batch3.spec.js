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
