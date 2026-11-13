import { test, expect } from "@playwright/test";

test.use({ timezoneId: "Asia/Kolkata" });
test.beforeEach(async ({ request }) => {
  await request.post("/__test/reset");
});
async function open(page, kind = "request") {
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
async function fillRequest(dialog) {
  await dialog.getByLabel("Request details").fill("Supplies at the school");
  await dialog.getByLabel("People affected").fill("6");
  await dialog.getByLabel("Latitude", { exact: true }).fill("19.08");
  await dialog.getByLabel("Longitude", { exact: true }).fill("72.88");
}

test("create request is keyed, same-task reentry sends once, and matching remains automatic", async ({
  page,
  request,
}) => {
  const dialog = await open(page);
  await fillRequest(dialog);
  const matches = [];
  page.on("request", (r) => {
    if (r.url().includes("/api/v1/match/")) matches.push(r.url());
  });
  await dialog.getByRole("form", { name: "New request" }).evaluate((form) => {
    form.requestSubmit();
    form.requestSubmit();
  });
  await expect(
    dialog.getByRole("status", { name: "Creation result" }),
  ).toContainText("WAITING");
  const result = await (await request.get("/__test/creations")).json();
  expect(result.calls).toHaveLength(1);
  expect(result.calls[0].key).toMatch(/^[0-9a-f-]{36}$/);
  expect(JSON.parse(result.calls[0].body)).toEqual({
    requestType: "WATER",
    details: "Supplies at the school",
    urgency: "NORMAL",
    peopleAffected: 6,
    location: { lat: 19.08, lng: 72.88 },
  });
  expect(matches).toEqual([]);
  await expect(page.getByRole("region", { name: "Requests" })).toContainText(
    "Supplies at the school",
  );
});

test("a lost create reply survives reload and retries identical body/key without duplicates", async ({
  page,
  request,
}) => {
  let first = true;
  await page.route(/\/api\/v1\/requests$/, async (route) => {
    if (route.request().method() === "POST" && first) {
      first = false;
      await route.fetch(); // The fixture committed the create; lose only the reply.
      await route.abort("failed");
    } else await route.continue();
  });
  let dialog = await open(page);
  await fillRequest(dialog);
  await dialog
    .getByRole("button", { name: "Submit request", exact: true })
    .click();
  await expect(dialog.getByRole("alert")).toContainText("not confirmed");
  await page.reload();
  await page
    .getByRole("button", { name: "Create request", exact: true })
    .click();
  dialog = page.getByRole("dialog");
  await expect(dialog.getByLabel("Request details")).toBeDisabled();
  await expect(dialog.getByLabel("Request details")).toHaveValue(
    "Supplies at the school",
  );
  await dialog
    .getByRole("button", { name: "Retry same request", exact: true })
    .click();
  await expect(
    dialog.getByRole("status", { name: "Creation result" }),
  ).toContainText("WAITING");
  const result = await (await request.get("/__test/creations")).json();
  expect(result.calls).toHaveLength(2);
  expect(result.calls[0]).toEqual(result.calls[1]);
  expect(result.records).toHaveLength(1);
});

test("register resource sends UTC windows and confirms it in the public list", async ({
  page,
  request,
}) => {
  const dialog = await open(page, "resource");
  await dialog.getByLabel("Resource name").fill("School water team");
  await dialog.getByLabel("Latitude", { exact: true }).fill("19.08");
  await dialog.getByLabel("Longitude", { exact: true }).fill("72.88");
  await dialog
    .getByLabel("Available from (local time)")
    .fill("2026-11-13T14:30");
  await dialog
    .getByLabel("Available until (local time)")
    .fill("2026-11-13T16:30");
  await dialog
    .getByRole("button", { name: "Register resource", exact: true })
    .click();
  await expect(
    dialog.getByRole("status", { name: "Creation result" }),
  ).toContainText("AVAILABLE");
  const result = await (await request.get("/__test/creations")).json();
  const body = JSON.parse(result.calls[0].body);
  expect(body.windowStart).toBe("2026-11-13T09:00:00.000Z");
  expect(body.windowEnd).toBe("2026-11-13T11:00:00.000Z");
  expect(body.reusable).toBe(true);
  expect(result.calls[0].key).toMatch(/^[0-9a-f-]{36}$/);
  await expect(page.getByRole("region", { name: "Resources" })).toContainText(
    "School water team",
  );
});

test("failed current read keeps known creation locked and confirmation sends no second POST", async ({
  page,
  request,
}) => {
  let fail = true;
  await page.route(/\/api\/v1\/requests\/[0-9a-f-]{36}$/, async (route) => {
    if (fail) {
      fail = false;
      await route.fulfill({
        status: 503,
        json: { code: "STORAGE_UNAVAILABLE" },
      });
    } else await route.continue();
  });
  const dialog = await open(page);
  await fillRequest(dialog);
  await dialog
    .getByRole("button", { name: "Submit request", exact: true })
    .click();
  await expect(dialog.getByRole("alert")).toContainText("confirm");
  await dialog
    .getByRole("button", { name: "Confirm saved request", exact: true })
    .click();
  await expect(
    dialog.getByRole("status", { name: "Creation result" }),
  ).toContainText("WAITING");
  expect(
    (await (await request.get("/__test/creations")).json()).calls,
  ).toHaveLength(1);
});

test("offline creation is disabled and the mobile form fits without horizontal overflow", async ({
  page,
  context,
}) => {
  await page.setViewportSize({ width: 390, height: 844 });
  const dialog = await open(page);
  await fillRequest(dialog);
  expect(await dialog.evaluate((el) => el.scrollWidth <= el.clientWidth)).toBe(
    true,
  );
  await page.screenshot({
    path: "test-results/create-request-mobile.png",
    fullPage: true,
  });
  await context.setOffline(true);
  await expect(
    dialog.getByRole("button", { name: "Submit request", exact: true }),
  ).toBeDisabled();
  await expect(dialog).toContainText("not sent or queued");
});
