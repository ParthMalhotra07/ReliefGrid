import { build } from "esbuild";
import fs from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { createHash } from "node:crypto";
const root = path.dirname(fileURLToPath(import.meta.url)),
  out = path.join(root, "static");
await fs.mkdir(out, { recursive: true });
if (
  path.dirname(await fs.realpath(out)) !== (await fs.realpath(root)) ||
  path.basename(out) !== "static"
)
  throw new Error("Unexpected asset target");
for (const entry of await fs.readdir(out, { withFileTypes: true })) {
  if (!entry.isFile())
    throw new Error("Unexpected directory in generated assets");
  await fs.unlink(path.join(out, entry.name));
}
const result = await build({
  absWorkingDir: root,
  entryPoints: ["src/App.jsx"],
  outdir: "static",
  bundle: true,
  minify: true,
  format: "esm",
  target: "es2022",
  entryNames: "app-[hash]",
  assetNames: "[name]-[hash]",
  loader: { ".png": "file" },
  metafile: true,
  legalComments: "eof",
});
const js = Object.entries(result.metafile.outputs).find(
  ([name, v]) => v.entryPoint && name.endsWith(".js"),
)[0];
const css = result.metafile.outputs[js].cssBundle;
await fs.copyFile(
  path.join(root, "src/map-grid.svg"),
  path.join(out, "map-grid.svg"),
);
const licenses = [];
for (const pkg of ["react", "react-dom", "leaflet"])
  licenses.push(
    pkg +
      "\n" +
      (
        await fs.readFile(
          path.join(root, "node_modules", pkg, "LICENSE"),
          "utf8",
        )
      ).replaceAll("\r\n", "\n"),
  );
await fs.writeFile(path.join(out, "licenses.txt"), licenses.join("\n\n"));
const index = (await fs.readFile(path.join(root, "index.html"), "utf8"))
  .replace("@APP_JS@", "/assets/" + path.basename(js))
  .replace("@APP_CSS@", "/assets/" + path.basename(css));
await fs.writeFile(path.join(out, "index.html"), index);
const assets = (await fs.readdir(out))
  .sort()
  .filter((x) => x !== "index.html")
  .map((x) => "/assets/" + x);
const hash = createHash("sha256");
for (const file of (await fs.readdir(out)).sort())
  hash.update(await fs.readFile(path.join(out, file)));
const revision = hash.digest("hex").slice(0, 16);
const sw = `const NAME='reliefgrid-${revision}',ASSETS=${JSON.stringify(["/", ...assets])};
self.addEventListener('install',event=>event.waitUntil(caches.open(NAME).then(cache=>cache.addAll(ASSETS.map(url=>new Request(url,{cache:'reload'})))).then(()=>self.skipWaiting())));
self.addEventListener('activate',event=>event.waitUntil(caches.keys().then(keys=>Promise.all(keys.filter(k=>k.startsWith('reliefgrid-')&&k!==NAME).map(k=>caches.delete(k)))).then(()=>self.clients.claim())));
self.addEventListener('fetch',event=>{const u=new URL(event.request.url);if(event.request.method!=='GET'||u.origin!==self.location.origin||u.pathname.startsWith('/api/')||u.pathname==='/ui/status'||u.pathname.startsWith('/__test/'))return;if(u.pathname==='/'){event.respondWith(caches.open(NAME).then(cache=>cache.match('/')).then(hit=>hit||fetch(event.request)));return;}if(ASSETS.includes(u.pathname))event.respondWith(caches.open(NAME).then(cache=>cache.match(event.request)).then(hit=>hit||fetch(event.request)));});`;
await fs.writeFile(path.join(out, "sw.js"), sw + "\n");
const sources = [
  "package.json",
  "package-lock.json",
  "build.mjs",
  "index.html",
  ...(await fs.readdir(path.join(root, "src")))
    .filter((x) => !x.endsWith(".test.js"))
    .map((x) => "src/" + x),
];
const manifest = { revision, assets, sources: {}, outputs: {} };
for (const file of (await fs.readdir(out)).sort())
  manifest.outputs[file] = createHash("sha256")
    .update(await fs.readFile(path.join(out, file)))
    .digest("hex");
for (const file of sources.sort())
  manifest.sources[file] = createHash("sha256")
    .update(await fs.readFile(path.join(root, file)))
    .digest("hex");
await fs.writeFile(
  path.join(out, "manifest.json"),
  JSON.stringify(manifest, null, 2) + "\n",
);
console.log("Built offline dispatch assets: " + revision);
