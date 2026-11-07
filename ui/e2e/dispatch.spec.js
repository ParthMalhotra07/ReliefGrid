import { test, expect } from "@playwright/test";
const eventId = "44444444-4444-4444-8444-444444444444";
test.beforeEach(async ({ request }) => {
  await request.post("/__test/reset");
});
test("dispatch shows authoritative state, assignments, filters and telemetry", async ({
  page,
}) => {
  const errors = [];
  page.on("pageerror", (error) => errors.push(error.message));
  await page.goto("/");
  await expect(
    page.getByRole("heading", { name: "Dispatch overview" }),
  ).toBeVisible();
  await expect(page.getByRole("region", { name: "Requests" })).toContainText(
    "CANCELLING",
  );
  await expect(page.getByRole("region", { name: "Resources" })).toContainText(
    "Water team Alpha",
  );
  await expect(page.getByRole("region", { name: "Shard reads" })).toContainText(
    "Shard 2",
  );
  await expect(
    page.getByRole("region", { name: "Matching queue" }),
  ).toContainText("3");
  await expect(page.getByLabel("Dispatch map")).toBeVisible();
  await page
    .getByRole("button", { name: /Water needed at the school/ })
    .click();
  await expect(
    page.getByRole("heading", { name: "Request timeline" }),
  ).toBeVisible();
  await page.screenshot({
    path: "test-results/dispatch-desktop.png",
    fullPage: true,
  });
  await expect(
    page.locator(".leaflet-overlay-pane path[stroke-dasharray]"),
  ).toHaveCount(1);
  expect(errors).toEqual([]);
  await page.getByLabel("Request status").selectOption("WAITING");
  await expect(page.getByRole("region", { name: "Requests" })).toContainText(
    "No requests match",
  );
});
test("events trigger authoritative refresh and reconnect recovers a missed update", async ({
  page,
  request,
}) => {
  await request.post("/__test/state", { data: { status: "WAITING" } });
  await page.goto("/");
  await expect(page.getByRole("region", { name: "Requests" })).toContainText(
    "WAITING",
  );
  await request.post("/__test/state", {
    data: { status: "MATCHING", eventId },
  });
  await expect(page.getByRole("region", { name: "Requests" })).toContainText(
    "MATCHING",
  );
  await request.post("/__test/state", {
    data: { status: "MATCHED", disconnect: true },
  });
  await expect(page.getByRole("region", { name: "Requests" })).toContainText(
    "MATCHED",
  );
});
test("saved list survives disconnected navigation and reconnect refreshes it", async ({
  page,
  context,
  request,
}) => {
  await page.goto("/");
  await expect(page.getByRole("region", { name: "Requests" })).toContainText(
    "CANCELLING",
  );
  await page.evaluate(async () => {
    await navigator.serviceWorker.ready;
  });
  await expect
    .poll(() => page.evaluate(() => !!navigator.serviceWorker.controller))
    .toBe(true);
  await context.setOffline(true);
  await page.reload();
  await expect(
    page.getByRole("heading", { name: "Dispatch overview" }),
  ).toBeVisible();
  await expect(page.getByRole("region", { name: "Requests" })).toContainText(
    "CANCELLING",
  );
  await expect(
    page.getByRole("status", { name: "Connection status" }),
  ).toContainText("Offline");
  await request.post("/__test/state", { data: { status: "CANCELLED" } });
  await context.setOffline(false);
  await expect(page.getByRole("region", { name: "Requests" })).toContainText(
    "CANCELLED",
  );
});
test("failed authoritative scan preserves the saved view and marks it stale", async ({
  page,
  request,
}) => {
  await page.goto("/");
  await expect(page.getByRole("region", { name: "Requests" })).toContainText(
    "CANCELLING",
  );
  await request.post("/__test/state", { data: { fail: true, eventId } });
  await expect(page.getByRole("alert")).toContainText("saved");
  await expect(page.getByRole("region", { name: "Requests" })).toContainText(
    "CANCELLING",
  );
});

test("mobile layout retains the list, map and telemetry without horizontal overflow", async ({
  page,
}) => {
  await page.setViewportSize({ width: 390, height: 844 });
  await page.goto("/");
  await expect(page.getByRole("region", { name: "Requests" })).toContainText(
    "CANCELLING",
  );
  await expect(page.getByLabel("Dispatch map")).toBeVisible();
  expect(
    await page.evaluate(() => document.documentElement.scrollWidth),
  ).toBeLessThanOrEqual(390);
  await page.getByRole("button", { name: "List", exact: true }).click();
  await expect(page.getByLabel("Dispatch map")).not.toBeVisible();
  await expect(page.getByRole("region", { name: "Resources" })).toContainText(
    "Water team Alpha",
  );
  await page.screenshot({
    path: "test-results/dispatch-mobile.png",
    fullPage: true,
  });
});

test("an offline shell without a saved snapshot reports unknown data", async ({
  page,
  context,
}) => {
  await page.goto("/");
  await expect(page.getByRole("region", { name: "Requests" })).toContainText(
    "CANCELLING",
  );
  await page.evaluate(async () => {
    await navigator.serviceWorker.ready;
  });
  await expect
    .poll(() => page.evaluate(() => !!navigator.serviceWorker.controller))
    .toBe(true);
  await page.evaluate(() =>
    localStorage.removeItem("reliefgrid.dispatch.snapshot.v1"),
  );
  await context.setOffline(true);
  await page.reload();
  await expect(page.getByRole("region", { name: "Requests" })).toContainText(
    "No saved request data",
  );
  await expect(page.getByRole("region", { name: "Shard reads" })).toContainText(
    "Unknown",
  );
});

test("duplicate events do not refetch and hints never replace authoritative state", async ({
  page,
  request,
}) => {
  await page.addInitScript(() => {
    window.__reads = 0;
    window.__changes = 0;
    const originalFetch = window.fetch;
    window.fetch = function (input, init) {
      if (String(input).startsWith("/api/v1/requests")) window.__reads++;
      return originalFetch.call(this, input, init);
    };
    const Original = window.EventSource;
    window.EventSource = class extends Original {
      constructor(...args) {
        super(...args);
        this.addEventListener("change", () =>
          setTimeout(() => window.__changes++, 0),
        );
      }
    };
  });
  await page.goto("/");
  await expect(page.getByRole("region", { name: "Requests" })).toContainText(
    "CANCELLING",
  );
  await expect(
    page.getByRole("status", { name: "Connection status" }),
  ).toContainText("Live");
  await expect(
    page.getByRole("button", { name: /Refresh data/ }),
  ).toBeEnabled();
  await page.evaluate(() => {
    window.__falseTerminal = [];
    const target = document.querySelector('section[aria-label="Requests"]');
    new MutationObserver(() => {
      if (target.textContent.includes("CANCELLED"))
        window.__falseTerminal.push("CANCELLED");
    }).observe(target, { subtree: true, childList: true, characterData: true });
  });
  const before = await page.evaluate(() => window.__reads);
  const response = page.waitForResponse((r) =>
    r.url().includes("/api/v1/requests?"),
  );
  await request.post("/__test/state", {
    data: { eventId, hint: { status: "CANCELLED", entityVersion: 999 } },
  });
  await response;
  await expect(
    page.getByRole("button", { name: /Refresh data/ }),
  ).toBeEnabled();
  await expect.poll(() => page.evaluate(() => window.__changes)).toBe(1);
  expect(await page.evaluate(() => window.__reads)).toBe(before + 1);
  await expect(page.getByRole("region", { name: "Requests" })).toContainText(
    "CANCELLING",
  );
  expect(await page.evaluate(() => window.__falseTerminal)).toEqual([]);
  await request.post("/__test/state", {
    data: { eventId, hint: { status: "CANCELLED", entityVersion: 999 } },
  });
  await expect.poll(() => page.evaluate(() => window.__changes)).toBe(2);
  expect(await page.evaluate(() => window.__reads)).toBe(before + 1);
});
test("cancel uses only the public status endpoint and waits for committed cleanup", async ({
  page,
  request,
}) => {
  await request.post("/__test/state", { data: { status: "WAITING" } });
  await page.goto("/");
  await expect(page.getByRole("region", { name: "Requests" })).toContainText(
    "WAITING",
  );
  await page
    .getByRole("button", { name: /Water needed at the school/ })
    .click();
  page.on("dialog", (dialog) => dialog.accept());
  const outgoing = page.waitForRequest(
    (r) => r.method() === "PUT" && r.url().endsWith("/status"),
  );
  await page
    .getByRole("button", { name: "Cancel request", exact: true })
    .click();
  expect((await outgoing).postDataJSON()).toEqual({ status: "CANCELLED" });
  await expect(page.getByRole("region", { name: "Requests" })).toContainText(
    "CANCELLING",
  );
  await expect(
    page.getByText("Cleanup is in progress.", { exact: false }),
  ).toBeVisible();
  await expect(
    page.getByRole("button", { name: "Cancel request", exact: true }),
  ).toHaveCount(0);
  await request.post("/__test/state", {
    data: { status: "CANCELLED", eventId },
  });
  await expect(page.getByRole("region", { name: "Requests" })).toContainText(
    "CANCELLED",
  );
});
test("an unknown write outcome remains visible after a successful reread", async ({
  page,
  request,
}) => {
  await request.post("/__test/state", {
    data: { status: "WAITING", unknownWrite: true },
  });
  await page.goto("/");
  await expect(page.getByRole("region", { name: "Requests" })).toContainText(
    "WAITING",
  );
  await page
    .getByRole("button", { name: /Water needed at the school/ })
    .click();
  page.on("dialog", (dialog) => dialog.accept());
  await page
    .getByRole("button", { name: "Cancel request", exact: true })
    .click();
  await expect(page.getByRole("alert")).toContainText(
    "outcome is not confirmed",
  );
  await expect(page.getByRole("region", { name: "Requests" })).toContainText(
    "WAITING",
  );
});

test("first read failure reports unavailable lists instead of endless loading", async ({
  page,
  request,
}) => {
  await request.post("/__test/state", { data: { fail: true } });
  await page.goto("/");
  await expect(page.getByRole("alert")).toContainText(
    "Could not confirm current state",
  );
  await expect(page.getByRole("region", { name: "Requests" })).toContainText(
    "Request data unavailable",
  );
  await expect(page.getByRole("region", { name: "Resources" })).toContainText(
    "Resource data unavailable",
  );
});

test("resources identify latest and saved observations through refresh recovery", async ({
  page,
  request,
}) => {
  await page.goto("/");
  const resources = page.getByRole("region", { name: "Resources" });
  await expect(resources).toContainText("LATEST READ");
  await request.post("/__test/state", { data: { fail: true, eventId } });
  await expect(resources).toContainText("SAVED VIEW");
  await request.post("/__test/state", {
    data: { fail: false, eventId: "55555555-5555-4555-8555-555555555555" },
  });
  await expect(resources).toContainText("LATEST READ");
});

test("same-render action reentry admits only one pending write", async ({
  page,
  request,
}) => {
  await request.post("/__test/state", { data: { status: "WAITING" } });
  await page.goto("/");
  await page
    .getByRole("button", { name: /Water needed at the school/ })
    .click();
  const writes = await page
    .getByRole("button", { name: "Cancel request", exact: true })
    .evaluate((button) => {
      let submitted = 0;
      const original = window.fetch;
      window.confirm = () => true;
      window.fetch = (...args) => {
        if (args[1]?.method === "PUT") {
          submitted++;
          return new Promise(() => {});
        }
        return original(...args);
      };
      // Reenter the same rendered handler in one task, before React can commit
      // busy/disabled state. The pinned React version attaches its props here.
      const propsKey = Object.keys(button).find((key) =>
        key.startsWith("__reactProps$"),
      );
      if (!propsKey) throw new Error("Rendered action handler unavailable");
      const handler = button[propsKey].onClick;
      handler();
      handler();
      return submitted;
    });
  expect(writes).toBe(1);
});
