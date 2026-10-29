import { expect, test } from "@playwright/test";
import { evidence, login } from "./session";

test("overview reveals detail on demand and supports keyboard inspection", async ({
  page,
}) => {
  const errors: string[] = [];
  page.on("pageerror", (error) => errors.push(String(error)));
  await login(page);
  await expect(
    page.getByRole("heading", { name: "Pipeline overview" }),
  ).toBeVisible();
  await expect(
    page
      .getByRole("navigation", { name: "Main navigation" })
      .getByRole("button"),
  ).toHaveCount(6);
  await expect(page.getByTestId("chart-flow")).toBeVisible();
  await expect(page.getByTestId("chart-queue")).toBeVisible();
  await expect(page.getByTestId("chart-wait")).toHaveCount(0);
  await expect(page.getByTestId("sale-setup")).toBeHidden();
  await expect(page.getByRole("radio", { name: "System lens" })).toBeHidden();
  await page.getByRole("button", { name: "More metrics" }).click();
  await expect(page.getByTestId("chart-wait")).toBeVisible();
  await page.getByRole("button", { name: "Show fewer charts" }).click();
  await expect(page.getByTestId("chart-wait")).toHaveCount(0);

  await page.getByRole("button", { name: "List", exact: true }).click();
  const ledger = page
    .getByTestId("pipeline-list")
    .getByRole("button", { name: /Sales ledger/ });
  await ledger.focus();
  await page.keyboard.press("Enter");
  await expect(
    page.getByRole("dialog", { name: "Component inspector" }),
  ).toBeVisible();
  await expect(page.getByTestId("inspector")).toContainText(
    "Latest ledger entries",
  );
  await page.getByLabel("Inspect component").selectOption("lane0");
  await expect(page.getByTestId("inspector")).toContainText("Now serving");
  await page.keyboard.press("Escape");
  await expect(page.getByRole("dialog")).toBeHidden();
  await expect(ledger).toBeFocused();
  await page.getByRole("button", { name: "Map", exact: true }).click();
  await page.screenshot({
    path: `${evidence}/usability-overview.png`,
    fullPage: true,
  });
  expect(errors).toEqual([]);
});

test("sale drafts survive navigation and the lab shows one experiment at a time", async ({
  page,
}) => {
  await login(page);
  await page.getByTestId("tab-sale").click();
  await expect(page.getByTestId("sale-setup")).toBeVisible({
    timeout: 420_000,
  });
  await expect(
    page
      .getByRole("radiogroup", { name: "Presets", exact: true })
      .getByRole("radio"),
    // Four measured M7 shapes plus M16's gradual ramp and repeated sales.
  ).toHaveCount(6);
  await expect(page.getByLabel("Phase 1 seconds")).toBeHidden();
  await page
    .locator("summary")
    .filter({ hasText: "Customize this sale" })
    .click();
  await page.getByTestId("sale-seed").fill("4242");
  await page.getByLabel("Phase 1 rate").fill("75");
  await page.getByTestId("tab-scaling").click();
  await expect(
    page.getByRole("heading", { name: "Manage capacity" }),
  ).toBeFocused();
  await page.getByTestId("view-live").click();
  await page.getByTestId("tab-sale").click();
  await expect(page.getByTestId("sale-seed")).toHaveValue("4242");
  await expect(page.getByLabel("Phase 1 rate")).toHaveValue("75");
  await page.screenshot({
    path: `${evidence}/usability-sale.png`,
    fullPage: true,
  });
  await page.getByTestId("tab-faults").click();
  await expect(page.locator('[data-testid^="inject-"]:visible')).toHaveCount(1);
  await page
    .getByLabel("Experiment", { exact: true })
    .selectOption("slow_processing");
  await expect(page.getByLabel("Delay per record (ms)")).toBeVisible();
  await expect(page.getByTestId("inject-slow_processing")).toBeVisible();
  await expect(page.getByTestId("inject-postgres_outage")).toHaveCount(0);
});

test("phone and dark layouts keep navigation and inspection usable", async ({
  page,
}) => {
  await page.setViewportSize({ width: 390, height: 844 });
  await page.emulateMedia({ colorScheme: "dark", reducedMotion: "reduce" });
  const errors: string[] = [];
  page.on("pageerror", (error) => errors.push(String(error)));
  await login(page);
  await expect(page.getByTestId("pipeline-list")).toBeVisible();
  await expect(page.getByTestId("flow-map")).toHaveCount(0);
  for (const id of [
    "view-live",
    "tab-sale",
    "tab-scaling",
    "tab-faults",
    "view-sessions",
    "tab-guide",
  ]) {
    await page.getByTestId(id).click();
    await expect(page.getByTestId(id)).toHaveAttribute("aria-current", "page");
    await expect
      .poll(() =>
        page.evaluate(() => document.documentElement.scrollWidth <= innerWidth),
      )
      .toBe(true);
  }
  await page.getByTestId("view-live").click();
  await page
    .getByTestId("pipeline-list")
    .getByRole("button", { name: /Lane 0/ })
    .click();
  await expect(page.getByTestId("inspector")).toContainText("Checkout lane 0");
  await expect
    .poll(() =>
      page.evaluate(
        () => document.querySelector("dialog")!.scrollWidth <= innerWidth,
      ),
    )
    .toBe(true);
  await page.getByRole("button", { name: "Close inspector" }).click();
  await page.screenshot({
    path: `${evidence}/usability-mobile-dark.png`,
    fullPage: true,
  });
  expect(errors).toEqual([]);
});
