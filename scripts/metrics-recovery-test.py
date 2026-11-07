import contextlib,json,tempfile,threading,unittest,urllib.parse,uuid
from http.server import BaseHTTPRequestHandler,ThreadingHTTPServer
from pathlib import Path
from metrics_fault import Traffic

class RecoveryTest(unittest.TestCase):
 @contextlib.contextmanager
 def fixture(self,partial=False,foreign=False,lost_delete=False,forged=False,weak=False,wrong_ring=False,incomplete=False,omitted=False):
  state={'live':True,'deletes':[],'puts':[]}
  class Handler(BaseHTTPRequestHandler):
   def reply(self,payload):
    data=json.dumps(payload).encode();self.send_response(200);self.send_header('Content-Length',str(len(data)));self.end_headers();self.wfile.write(data)
   def log_message(self,*args):pass
   def do_PUT(self):
    body=json.loads(self.rfile.read(int(self.headers['Content-Length'])));state['puts'].append(body)
    self.reply(dict(ringVersion=1,result=dict(outcome='OK',operationId='foreign' if forged else body['operationId'],key=state['key'],version=1)))
   def do_GET(self):
    row=dict(key=state['key'],kind='kv',version=1,value={'real':'foreign'} if foreign else {'syntheticMetricsLoad':'x'*1024})
    page=dict(partial=partial,failedShards=[1] if partial else [],records=[row] if state['live'] else [],nextCursor='',consistency='stale' if weak else 'strong',ringVersion=2 if wrong_ring else 1,positions=[dict(shard=i) for i in range(2 if incomplete else 3)])
    if omitted:page.pop('failedShards')
    self.reply(page)
   def do_DELETE(self):
    meta={k:v[0] for k,v in urllib.parse.parse_qs(urllib.parse.urlsplit(self.path).query).items()};state['deletes'].append((meta,self.headers['If-Match']));state['live']=False
    if lost_delete and len(state['deletes'])==1:
     self.connection.close();return
    self.reply(dict(ringVersion=1,result=dict(outcome='OK',operationId=meta['operationId'],key=state['key'],version=2)))
  server=ThreadingHTTPServer(('127.0.0.1',0),Handler);thread=threading.Thread(target=server.serve_forever);thread.start()
  try:
   with tempfile.TemporaryDirectory() as directory:
    traffic=Traffic('http://127.0.0.1:'+str(server.server_port),1,Path(directory));key=traffic.prefix+':0:0';state['key']=key
    meta=dict(operationId=str(uuid.uuid4()),at=123,ringVersion=1)
    body=json.dumps(dict(meta,value={'syntheticMetricsLoad':'x'*1024}))
    traffic.pending[meta['operationId']]=dict(key=key,expectedVersion=0,delete=False,meta=meta,body=body)
    traffic.errors=['original retry budget exhausted'];traffic.writes=32
    yield traffic,state,meta
  finally:server.shutdown();thread.join();server.server_close()
 def test_unknown_committed_write_is_replayed_then_cleaned_without_passing_failed_run(self):
  with self.fixture() as (traffic,state,meta):
   with self.assertRaises(AssertionError):traffic.stop()
   self.assertFalse(state['live'],'unknown committed scratch write remained live')
   self.assertFalse(traffic.pending)
   self.assertEqual({k:state['puts'][0][k] for k in meta},meta)
   self.assertIn('original retry budget exhausted',traffic.errors)
 def test_lost_cleanup_response_retries_identical_operation(self):
  with self.fixture(lost_delete=True) as (traffic,state,meta):
   with self.assertRaises(AssertionError):traffic.stop()
   self.assertFalse(state['live']);self.assertFalse(traffic.pending)
   self.assertEqual(len(state['deletes']),2)
   self.assertEqual(state['deletes'][0],state['deletes'][1])
 def test_partial_scan_and_foreign_value_never_authorize_cleanup(self):
  for flags in ({'partial':True},{'foreign':True}):
   with self.subTest(flags=flags),self.fixture(**flags) as (traffic,state,meta):
    with self.assertRaises(AssertionError):traffic.stop()
    self.assertTrue(state['live']);self.assertEqual(state['deletes'],[])
 def test_foreign_operation_result_remains_unresolved(self):
  with self.fixture(forged=True) as (traffic,state,meta):
   with self.assertRaises(AssertionError):traffic.stop()
   self.assertIn(meta['operationId'],traffic.pending)
   self.assertEqual(state['deletes'],[])
 def test_weak_wrong_ring_and_missing_shard_cannot_authorize_cleanup(self):
  for flags in ({'weak':True},{'wrong_ring':True},{'incomplete':True}):
   with self.subTest(flags=flags),self.fixture(**flags) as (traffic,state,meta):
    with self.assertRaises(AssertionError):traffic.stop()
    self.assertTrue(state['live']);self.assertEqual(state['deletes'],[])
 def test_omitted_empty_failed_shards_is_valid(self):
  with self.fixture(omitted=True) as (traffic,state,meta):
   with self.assertRaises(AssertionError):traffic.stop()
   self.assertFalse(state['live']);self.assertFalse(traffic.pending)
if __name__=='__main__':unittest.main()
