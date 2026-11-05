import { chromium, expect } from "../../../ui/node_modules/@playwright/test/index.mjs";
import { randomUUID } from "node:crypto";
import fs from "node:fs/promises";
const origin = process.env.A41_KIND_ORIGIN || "http://127.0.0.1:8080";
const u = new URL(origin);
if (!["127.0.0.1", "localhost"].includes(u.hostname) || u.username || u.password || u.pathname !== "/") throw new Error("Loopback kind origin required");
const browser = await chromium.launch();
const context = await browser.newContext({ baseURL: origin, viewport: {width:1440,height:960} });
const page = await context.newPage();
const errors = [];
page.on("pageerror", error => errors.push(error.message));
const report = { origin, checks: [], requestId: null, resourceId: null };
const check = name => { report.checks.push(name); console.log("PASS: "+name); };
async function api(method,path,data) {
 const r = await context.request.fetch(path,{method,data,headers:{"Idempotency-Key":randomUUID()},timeout:10000});
 if (!r.ok()) throw new Error(method+" "+path+" "+r.status()+" "+await r.text());
 return r.json();
}
try {
 await page.goto("/");
 await expect(page.getByRole("heading",{name:"Dispatch overview"})).toBeVisible();
 await expect(page.getByRole("region",{name:"Shard reads"})).toContainText("Shard 2");
 const queues = await api("GET","/ui/status");
 expect(queues.available).toBe(true);
 expect(queues.ready).toBeGreaterThanOrEqual(0);
 check("production UI, three shard observations and real broker aggregates");
 await page.evaluate(() => new Promise((resolve,reject) => {
  window.a41Hints=[];
  const source = new EventSource("/api/v1/events");
  window.a41Probe=source;
  const timer=setTimeout(()=>reject(new Error("SSE open timeout")),5000);
  source.onopen=()=>{clearTimeout(timer);resolve();};
  source.addEventListener("change",e=>window.a41Hints.push(JSON.parse(e.data)));
 }));
 const marker = "A4.1 live kind "+randomUUID().slice(0,8);
 const now=Date.now(), location={lat:40.7128,lng:-74.006};
 const resource = await api("POST","/api/v1/resources",{
  resourceType:"MEDICINE",name:marker+" resource",location,
  windowStart:new Date(now-60000).toISOString(),windowEnd:new Date(now+3600000).toISOString(),reusable:true
 });
 report.resourceId=resource.resourceId;
 const request = await api("POST","/api/v1/requests",{
  requestType:"MEDICINE",details:marker,peopleAffected:1,urgency:"HIGH",location
 });
 report.requestId=request.requestId;
 await expect.poll(()=>page.evaluate(id=>window.a41Hints.some(e=>e.entityId===id),request.requestId),{timeout:15000}).toBe(true);
 check("committed outbox event reached browser through real relay/broker/API SSE");
 let current;
 await expect.poll(async()=>{current=await api("GET","/api/v1/requests/"+request.requestId);return current.status;},{timeout:90000}).toBe("MATCHED");
 expect(current.assignedResourceId).toBe(resource.resourceId);
 const resources=await api("GET","/api/v1/resources?limit=100");
 const claimed=resources.items.find(r=>r.resourceId===resource.resourceId);
 expect(claimed.activeClaim.requestId).toBe(request.requestId);
 expect(claimed.activeClaim.attemptId).toMatch(/^[0-9a-f-]{36}$/);
 await page.getByLabel("Search requests or resources").fill(marker);
 const card=page.getByRole("region",{name:"Requests"}).getByRole("button",{name:new RegExp(marker)});
 await expect(card).toContainText("MATCHED",{timeout:20000});
 await card.click();
 await expect(page.locator(".leaflet-overlay-pane path[stroke-dasharray]")).toHaveCount(1);
 check("authoritative reciprocal assignment renders a map line");
 await page.screenshot({path:new URL("kind-desktop.png",import.meta.url).pathname,fullPage:true});
 await page.evaluate(()=>navigator.serviceWorker.ready);
 await expect.poll(()=>page.evaluate(()=>!!navigator.serviceWorker.controller)).toBe(true);
 await context.setOffline(true);
 await page.reload();
 await expect(page.getByRole("region",{name:"Requests"})).toContainText(marker);
 await expect(page.getByRole("region",{name:"Requests"})).toContainText("MATCHED");
 await expect(page.getByLabel("Connection status")).toContainText("Offline");
 check("real production shell and saved records survive offline reload");
 await api("PUT","/api/v1/requests/"+request.requestId+"/status",{status:"CANCELLED"});
 await expect.poll(async()=>{current=await api("GET","/api/v1/requests/"+request.requestId);return current.status;},{timeout:90000}).toBe("CANCELLED");
 await context.setOffline(false);
 await expect(page.getByRole("region",{name:"Requests"}).getByRole("button",{name:new RegExp(marker)})).toContainText("CANCELLED",{timeout:20000});
 expect(errors).toEqual([]);
 check("online reconnect refreshes cancellation completed during disconnection");
 report.result="PASS";
} finally {
 await fs.writeFile(new URL("kind-results.json",import.meta.url),JSON.stringify(report,null,2)+"\n");
 await browser.close();
}
