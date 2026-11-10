import fs from "node:fs/promises";
import path from "node:path";
import { pathToFileURL } from "node:url";

const [origin, directory] = process.argv.slice(2);
const url = new URL(origin);
if (url.protocol !== "http:" || url.hostname !== "127.0.0.1" || url.pathname !== "/" || url.username || url.password) throw new Error("Loopback Grafana origin required");
const modulePath = process.env.A42_PLAYWRIGHT_MODULE;
if (!modulePath) throw new Error("A42_PLAYWRIGHT_MODULE must name the locally installed @playwright/test 1.64.1 index.mjs");
const pkg = JSON.parse(await fs.readFile(path.join(path.dirname(modulePath), "package.json"),"utf8"));
if (pkg.name !== "@playwright/test" || pkg.version !== "1.64.1") throw new Error("Pinned Playwright 1.64.1 required");
const { chromium, expect } = await import(pathToFileURL(modulePath).href);
const browser = await chromium.launch();
try {
 const context = await browser.newContext({viewport:{width:1600,height:1400},deviceScaleFactor:1});
 const password = process.env.A42_GRAFANA_PASSWORD_FILE ? (await fs.readFile(process.env.A42_GRAFANA_PASSWORD_FILE,"utf8")).trim() : "reliefgrid-local";
 const response = await context.request.post(new URL("/login",origin).href,{data:{user:process.env.A42_GRAFANA_USER || "admin",password}});
 if (!response.ok()) throw new Error("Grafana login failed");
 const page = await context.newPage();
 for (const name of ["cluster","matching","storage"]) {
  await page.goto(new URL("/d/reliefgrid-"+name+"?kiosk&theme=light&from=now-15m&to=now",origin).href);
  await expect(page.locator("body")).toContainText("ReliefGrid");
  await page.waitForFunction(()=>!document.body.innerText.includes("Loading dashboard"),null,{timeout:15000});
  // Rendering interval, not a pass criterion; query/data correctness is asserted
  // by the live Python probe. The generated screenshots are inspected visually.
  await page.waitForTimeout(1500);
  await page.screenshot({path:path.join(directory,name+".png"),fullPage:true});
 }
 await context.close();
} finally { await browser.close(); }
