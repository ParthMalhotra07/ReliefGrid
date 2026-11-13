import contextlib, importlib.util, json, os, sys, tempfile, threading, unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from unittest.mock import patch
sys.path.insert(0, str(Path(__file__).resolve().parent))
from metrics_fault import Traffic
from demos.app_demo import Client, Demo

def load(name, filename):
 spec=importlib.util.spec_from_file_location(name,Path(__file__).with_name(filename))
 module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module);return module
verify=load('verify_reconcile','verify-reconcile.py')

class OperationalAuthTest(unittest.TestCase):
 @contextlib.contextmanager
 def fixture(self, content=b'fixture-app-token'):
  seen=[]
  class Handler(BaseHTTPRequestHandler):
   def log_message(self,*args): pass
   def reply(self,body,code=200):
    raw=json.dumps(body).encode();self.send_response(code);self.send_header('Content-Length',str(len(raw)));self.end_headers();self.wfile.write(raw)
   def do_GET(self):
    if self.path=='/internal/redirect':
     seen.append((self.path,self.headers.get('X-ReliefGrid-Internal-Token'),None))
     self.send_response(302);self.send_header('Location','http://localhost:'+str(self.server.server_port)+'/api/v1/requests');self.send_header('Content-Length','0');self.end_headers();return
    seen.append((self.path,self.headers.get('X-ReliefGrid-Internal-Token'),self.headers.get('Authorization')))
    self.reply({'ok':True})
   def do_PUT(self):
    body=json.loads(self.rfile.read(int(self.headers['Content-Length'])))
    seen.append((self.path,self.headers.get('X-ReliefGrid-Internal-Token'),None))
    if seen[-1][1]!='fixture-app-token': self.reply({'code':'UNAUTHORIZED'},401);return
    self.reply(dict(ringVersion=1,result=dict(operationId=body['operationId'],key='scratch',version=1,outcome='OK')))
  server=ThreadingHTTPServer(('127.0.0.1',0),Handler)
  thread=threading.Thread(target=server.serve_forever);thread.start()
  try:
   with tempfile.TemporaryDirectory() as directory:
    path=Path(directory)/'sender';path.write_bytes(content)
    with patch.dict(os.environ,{'INTERNAL_AUTH_TOKEN_FILE':str(path)}):
     yield 'http://127.0.0.1:'+str(server.server_port),Path(directory),seen
  finally: server.shutdown();thread.join();server.server_close()
 def test_metrics_scratch_uses_app_sender(self):
  with self.fixture() as (origin,out,seen):
   traffic=Traffic(origin,1,out)
   try: self.assertEqual(traffic.command('scratch',0),1)
   finally: traffic.pool.shutdown(wait=True)
   self.assertEqual(seen[0][1],'fixture-app-token')
 def test_reconciler_coordinator_only(self):
  with self.fixture() as (origin,out,seen):
   obj=verify.Verify.__new__(verify.Verify);obj.coord=origin;obj.api=origin+'/api/v1'
   obj.call(obj.coord,'GET','/internal/ring')
   obj.call(obj.api,'GET','/requests')
   self.assertEqual([row[1] for row in seen],['fixture-app-token',None])
 def test_demo_coordinator_only(self):
  with self.fixture() as (origin,out,seen):
   Client(origin,out,internal=True).call('GET','/internal/outbox',{200})
   Client(origin+'/api/v1',out).call('GET','/requests',{200})
   self.assertEqual([row[1] for row in seen],['fixture-app-token',None])
 def test_configured_invalid_file_fails_before_scratch_io(self):
  for content in (b'',b'bad\n',b'bad,token',b'x'*4097,b'bad token',b'\xff'):
   with self.subTest(size=len(content)),self.fixture(content) as (origin,out,seen):
    with self.assertRaisesRegex(ValueError,'internal auth'):
     traffic=Traffic(origin,1,out);traffic.pool.shutdown(wait=True)
    self.assertEqual(seen,[])
 def test_demo_audit_job_mounts_only_app_sender(self):
  with self.fixture() as (origin,out,seen):
   demo=Demo.__new__(Demo);demo.out=out;demo.number=2;demo.seed=__import__('uuid').uuid4();demo.namespace='reliefgrid'
   demo.wait_outbox_empty=lambda:None;captured=[]
   def kube(*args,**kwargs):
    if args[0]=='create': captured.append(json.loads(kwargs['stdin']))
    if args[:2]==('get','job'): return json.dumps({'status':{'succeeded':1}})
    if args[0]=='logs': return '{}'
    return ''
   demo.kube=kube;demo.check=lambda value,message:self.assertTrue(value,message)
   demo.invariants()
   self.check_pod(captured[0]['spec']['template'])
 def check_pod(self,pod):
  spec=pod['spec'];self.assertEqual(spec['securityContext']['fsGroup'],65532)
  self.assertEqual(len(spec['volumes']),1)
  secret=spec['volumes'][0]['secret'];self.assertEqual(secret['secretName'],'reliefgrid-internal-auth')
  self.assertEqual(secret['items'],[{'key':'sender-token','path':'sender-token'}])
  self.assertEqual(secret['defaultMode'],0o440)
  for container in spec['containers']:
   self.assertIn({'name':'INTERNAL_AUTH_TOKEN_FILE','value':'/var/run/reliefgrid/internal-auth/sender-token'},container['env'])
   self.assertTrue(container['volumeMounts'][0]['readOnly'])
 def test_disabled_demo_audit_has_no_auth_mount(self):
  with patch.dict(os.environ,{'INTERNAL_AUTH_TOKEN_FILE':''}):
   from internal_auth import mount_app_sender
   pod={'spec':{'containers':[{'name':'check'}]}}
   before=json.loads(json.dumps(pod));mount_app_sender(pod);self.assertEqual(pod,before)

 def test_sender_rejects_redirect_and_foreign_routes_without_forwarding(self):
  from internal_auth import Sender
  import urllib.error, urllib.request
  with self.fixture() as (origin,out,seen):
   sender=Sender(origin)
   with self.assertRaises(urllib.error.HTTPError) as caught:
    sender.open(origin+'/internal/redirect',timeout=2)
   self.assertEqual(caught.exception.code,302);caught.exception.close()
   self.assertEqual(len(seen),1,'credentialed redirect was followed')
   for target in (origin+'/api/v1/requests',origin.replace('127.0.0.1','localhost')+'/internal/ring'):
    with self.assertRaises(ValueError): sender.open(target,timeout=2)
   self.assertEqual(len(seen),1,'out-of-scope request made network I/O')
   request=urllib.request.Request(origin+'/internal/ring',headers={
    'X-ReliefGrid-Internal-Token':'wrong','Authorization':'Bearer operator','Proxy-Authorization':'private'})
   with sender.open(request,timeout=2) as response: self.assertEqual(response.status,200)
   self.assertEqual(seen[-1][1:],('fixture-app-token',None))
   self.assertNotIn('fixture-app-token',repr(sender))
 def test_missing_sender_file_is_not_anonymous_fallback(self):
  from internal_auth import Sender
  with self.fixture() as (origin,out,seen):
   with patch.dict(os.environ,{'INTERNAL_AUTH_TOKEN_FILE':str(out/'absent')}):
    with self.assertRaisesRegex(ValueError,'cannot read sender file'): Sender(origin)
   self.assertEqual(seen,[])
 def test_disabled_sender_preserves_legacy_operator_header(self):
  import urllib.request
  from internal_auth import Sender
  with self.fixture() as (origin,out,seen),patch.dict(os.environ,{'INTERNAL_AUTH_TOKEN_FILE':''}):
   request=urllib.request.Request(origin+'/internal/ring',headers={'Authorization':'Bearer operator'})
   with Sender(origin).open(request,timeout=2) as response: self.assertEqual(response.status,200)
   self.assertEqual(seen,[('/internal/ring',None,'Bearer operator')])
 def test_metrics_get_does_not_send_internal_token_to_telemetry_or_public_api(self):
  metrics=load('live_metrics','verify-A4.2-live.py')
  with self.fixture() as (origin,out,seen):
   metrics.get(origin,'/internal/ring')
   metrics.get(origin,'/api/v1/requests')
   metrics.get(origin,'/api/v1/query?query=up')
   self.assertEqual([row[1] for row in seen],['fixture-app-token',None,None])
 def test_reconcile_recovery_pod_and_audit_job_mount_app_sender(self):
  class Captured(Exception): pass
  with self.fixture() as (origin,out,seen):
   for method in ('crash','audit'):
    with self.subTest(method=method):
     obj=verify.Verify.__new__(verify.Verify);obj.out=out;obj.coord=origin;obj.seed='fixture';obj.scale=lambda count:None;obj.cases=[]
     obj.scan_requests=lambda:[];obj.save=lambda *args:None;obj.call=lambda *args:{'events':[],'read':{'consistency':'strong'}}
     manifests=[]
     def kube(*args,**kwargs):
      if args[0]=='create': manifests.append(json.loads(kwargs['stdin']));raise Captured()
      return ''
     obj.k=kube
     with self.assertRaises(Captured):
      obj.crash_case('claim',1) if method=='crash' else obj.audit()
     pod=manifests[0] if method=='crash' else manifests[0]['spec']['template']
     self.check_pod(pod)
     self.assertNotIn('fixture-app-token',json.dumps(pod))

if __name__=='__main__': unittest.main()
