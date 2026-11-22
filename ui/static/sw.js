const NAME='reliefgrid-3d6b93b02249d552',ASSETS=["/","/assets/app-FZSMOLYM.js","/assets/app-GNTO2275.css","/assets/layers-2x-TBM42ERR.png","/assets/layers-55W3Q4RM.png","/assets/licenses.txt","/assets/marker-icon-2V3QKKVC.png"];
self.addEventListener('install',event=>event.waitUntil(caches.open(NAME).then(cache=>cache.addAll(ASSETS.map(url=>new Request(url,{cache:'reload'})))).then(()=>self.skipWaiting())));
self.addEventListener('activate',event=>event.waitUntil(caches.keys().then(keys=>Promise.all(keys.filter(k=>k.startsWith('reliefgrid-')&&k!==NAME).map(k=>caches.delete(k)))).then(()=>self.clients.claim())));
async function navigate(request){
  try {
    const response=await fetch(request,{signal:AbortSignal.timeout(3000)});
    if(response.ok)return response;
  } catch {}
  return (await (await caches.open(NAME)).match('/')) || Response.error();
}
self.addEventListener('fetch',event=>{const u=new URL(event.request.url);if(event.request.method!=='GET'||u.origin!==self.location.origin||u.pathname.startsWith('/api/')||u.pathname==='/ui/status'||u.pathname.startsWith('/__test/'))return;if(u.pathname==='/'){event.respondWith(navigate(event.request));return;}if(ASSETS.includes(u.pathname))event.respondWith(caches.open(NAME).then(cache=>cache.match(event.request)).then(hit=>hit||fetch(event.request)));});
