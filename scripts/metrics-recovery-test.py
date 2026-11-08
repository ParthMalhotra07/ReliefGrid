import base64,contextlib,hashlib,itertools,json,tempfile,threading,unittest,urllib.parse,uuid
from http.server import BaseHTTPRequestHandler,ThreadingHTTPServer
from pathlib import Path
from metrics_fault import Traffic,run_fault
from unittest.mock import Mock,patch

class RecoveryTest(unittest.TestCase):
 @contextlib.contextmanager
 def fixture(self,partial=False,foreign=False,lost_delete=False,forged=False,weak=False,wrong_ring=False,incomplete=False,omitted=False,race=False,scan_suffix=None,foreign_namespace=False,kind='kv'):
  state={'live':True,'version':2,'deletes':[],'puts':[],'operations':{},'mismatches':[]}
  class Handler(BaseHTTPRequestHandler):
   def reply(self,payload,status=200):
    data=json.dumps(payload).encode();self.send_response(status);self.send_header('Content-Length',str(len(data)));self.end_headers();self.wfile.write(data)
   def log_message(self,*args):pass
   def do_PUT(self):
    body=json.loads(self.rfile.read(int(self.headers['Content-Length'])));state['puts'].append(body)
    self.reply(dict(ringVersion=1,result=dict(outcome='OK',operationId='foreign' if forged else body['operationId'],key=state['key'],version=1)))
   def do_GET(self):
    state['scans']=state.get('scans',0)+1
    row=dict(key=state['scan_key'],kind=kind,version=state['version'],value={'real':'foreign'} if foreign else {'syntheticMetricsLoad':'x'*1024})
    page=dict(partial=partial,failedShards=[1] if partial else [],records=[row] if state['live'] else [],nextCursor='',consistency='stale' if weak else 'strong',ringVersion=2 if wrong_ring else 1,positions=[dict(shard=i) for i in range(2 if incomplete else 3)])
    if omitted:page.pop('failedShards')
    self.reply(page)
   def do_DELETE(self):
    meta={k:v[0] for k,v in urllib.parse.parse_qs(urllib.parse.urlsplit(self.path).query).items()}
    key=urllib.parse.unquote(urllib.parse.urlsplit(self.path).path.removeprefix('/internal/data/'))
    condition=self.headers['If-Match'];state['deletes'].append((meta,condition))
    identity=(meta,condition);operation=meta['operationId']
    if operation in state['operations']:
     previous,status,payload=state['operations'][operation]
     if previous!=identity:
      self.reply(dict(code='OPERATION_ID_REUSED',outcomeUnknown=False),409);return
     self.reply(payload,status);return
    # Deterministic interleaving: a concurrent update commits after the scan.
    if race and len(state['deletes'])==1:state['version']+=1
    if condition!='"'+str(state['version'])+'"':
     state['mismatches'].append((condition,state['version']))
     payload=dict(code='VERSION_CONFLICT',outcomeUnknown=False,ringVersion=1,
      result=dict(outcome='VERSION_CONFLICT',operationId=operation,key=key,version=state['version']))
     state['operations'][operation]=(identity,412,payload)
     self.reply(payload,412);return
    state['live']=False;state['version']+=1
    payload=dict(ringVersion=1,result=dict(outcome='OK',operationId=operation,key=key,version=state['version']))
    state['operations'][operation]=(identity,200,payload)
    if lost_delete and len(state['deletes'])==1:
     self.connection.close();return
    self.reply(payload)
  server=ThreadingHTTPServer(('127.0.0.1',0),Handler);thread=threading.Thread(target=server.serve_forever);thread.start()
  try:
   with tempfile.TemporaryDirectory() as directory:
    traffic=Traffic('http://127.0.0.1:'+str(server.server_port),1,Path(directory));key=traffic.prefix+':0:0';state['key']=key
    state['scan_key']=(('a42-metrics-foreign' if foreign_namespace else traffic.prefix)+':'+(scan_suffix or '0:0'))
    meta=dict(operationId=str(uuid.uuid4()),at=123,ringVersion=1)
    body=json.dumps(dict(meta,value={'syntheticMetricsLoad':'x'*1024}))
    traffic.pending[meta['operationId']]=dict(key=key,expectedVersion=0,delete=False,meta=meta,body=body)
    state['original_intent']=json.loads(json.dumps(traffic.pending[meta['operationId']]))
    (traffic.out/'scratch-intents.json').write_text(json.dumps(traffic.pending))
    traffic.errors=['original retry budget exhausted'];traffic.writes=32
    yield traffic,state,meta
  finally:server.shutdown();thread.join();server.server_close()
 def test_unknown_committed_write_is_replayed_then_cleaned_without_passing_failed_run(self):
  with self.fixture() as (traffic,state,meta):
   with self.assertRaises(AssertionError):traffic.stop()
   self.assertFalse(state['live'],'unknown committed scratch write remained live')
   self.assertFalse(traffic.pending)
   self.assertEqual({k:state['puts'][0][k] for k in meta},meta)
   self.assertEqual(state['deletes'][0][1],'"2"','cleanup must use authoritative v2, not historical v1')
   self.assertEqual(state['version'],3)
   self.assertEqual(state['mismatches'],[])
   self.assertIn('original retry budget exhausted',traffic.errors)
 def test_lost_cleanup_response_retries_identical_operation(self):
  with self.fixture(lost_delete=True) as (traffic,state,meta):
   with self.assertRaises(AssertionError):traffic.stop()
   self.assertFalse(state['live']);self.assertFalse(traffic.pending)
   self.assertEqual(len(state['deletes']),2)
   self.assertEqual(state['deletes'][0],state['deletes'][1])
   self.assertEqual(state['deletes'][0][1],'"2"')
   self.assertEqual(state['version'],3,'replayed DELETE must not advance state twice')
   self.assertIn('original retry budget exhausted',traffic.errors)
 def test_version_race_rejects_stale_cleanup_without_false_success(self):
  with self.fixture(race=True) as (traffic,state,meta):
   with self.assertRaises(AssertionError):traffic.stop()
   self.assertTrue(state['live'],'stale cleanup deleted the concurrent update')
   self.assertEqual(state['version'],3,'rejected DELETE advanced the version')
   self.assertEqual(state['mismatches'],[('"2"',3)])
   self.assertEqual(state['deletes'][0][1],'"2"')
   self.assertTrue(traffic.pending,'unresolved cleanup intent was discarded')
   self.assertIn('original retry budget exhausted',traffic.errors)
   self.assertTrue(any('scratch recovery:' in error for error in traffic.errors))
   self.assertFalse((traffic.out/'scratch-recovery.json').exists(),'race falsely recorded successful recovery')
 def test_partial_scan_and_foreign_value_never_authorize_cleanup(self):
  for flags in ({'partial':True},{'foreign':True}):
   with self.subTest(flags=flags),self.fixture(**flags) as (traffic,state,meta):
    with self.assertRaises(AssertionError):traffic.stop()
    self.assertTrue(state['live']);self.assertEqual(state['deletes'],[])
 def test_foreign_operation_result_remains_unresolved(self):
  with self.fixture(forged=True) as (traffic,state,meta):
   with self.assertRaises(AssertionError):traffic.stop()
   self.assertIn(meta['operationId'],traffic.pending)
   self.assertEqual(traffic.pending[meta['operationId']],state['original_intent'])
   persisted=json.loads((traffic.out/'scratch-intents.json').read_text())
   self.assertIn(meta['operationId'],persisted,'durable unresolved intent was discarded')
   self.assertEqual(persisted[meta['operationId']],state['original_intent'])
   self.assertEqual(state['deletes'],[])
   self.assertFalse((traffic.out/'scratch-recovery.json').exists())
 def test_weak_wrong_ring_and_missing_shard_cannot_authorize_cleanup(self):
  for flags in ({'weak':True},{'wrong_ring':True},{'incomplete':True}):
   with self.subTest(flags=flags),self.fixture(**flags) as (traffic,state,meta):
    with self.assertRaises(AssertionError):traffic.stop()
    self.assertTrue(state['live']);self.assertEqual(state['deletes'],[])
 def test_omitted_empty_failed_shards_is_valid(self):
  with self.fixture(omitted=True) as (traffic,state,meta):
   with self.assertRaises(AssertionError):traffic.stop()
   self.assertFalse(state['live']);self.assertFalse(traffic.pending)
 def assert_invalid_owned_row(self,expected_error,**flags):
  with self.fixture(**flags) as (traffic,state,meta):
   with self.assertRaises(AssertionError):traffic.stop()
   self.assertGreater(state.get('scans',0),0,'scan did not reach the ownership check')
   self.assertTrue(state['live'])
   self.assertEqual(state['deletes'],[])
   self.assertFalse((traffic.out/'scratch-recovery.json').exists())
   self.assertIn('original retry budget exhausted',traffic.errors)
   self.assertTrue(any(expected_error in error for error in traffic.errors))
 def test_worker_overflow_rejected_before_delete(self):self.assert_invalid_owned_row('foreign scratch key',scan_suffix='4:0')
 def test_index_overflow_rejected_before_delete(self):self.assert_invalid_owned_row('foreign scratch key',scan_suffix='0:32')
 def test_wrong_kind_rejected_before_delete(self):self.assert_invalid_owned_row('foreign scratch content',kind='request')
 def test_wrong_marker_rejected_before_delete(self):self.assert_invalid_owned_row('foreign scratch content',foreign=True)
 def test_valid_boundary_key_is_cleaned(self):
  with self.fixture(scan_suffix='3:31') as (traffic,state,meta):
   with self.assertRaises(AssertionError):traffic.stop()
   self.assertFalse(state['live'])
   self.assertEqual(len(state['deletes']),1)
   self.assertFalse(traffic.pending)
   self.assertEqual(json.loads((traffic.out/'scratch-intents.json').read_text()),{})
   self.assertEqual(json.loads((traffic.out/'scratch-recovery.json').read_text()),dict(resolvedIntents=1,deletedKeys=1))
   self.assertEqual(traffic.errors,['original retry budget exhausted'])
 def test_other_namespace_is_untouched(self):
  with self.fixture(foreign_namespace=True) as (traffic,state,meta):
   with self.assertRaises(AssertionError):traffic.stop()
   self.assertGreater(state.get('scans',0),0)
   self.assertTrue(state['live'])
   self.assertEqual(state['deletes'],[])
   self.assertEqual(json.loads((traffic.out/'scratch-recovery.json').read_text()),dict(resolvedIntents=1,deletedKeys=0))
   self.assertEqual(traffic.errors,['original retry budget exhausted'])
 def run_mock_fault_lifecycle(self,fail_stop):
  old='storage-shard-0-0';traffic=Mock(prefix='private-test',writes=32)
  if fail_stop:traffic.stop.side_effect=AssertionError('injected stop failure')
  def kube(args,**kwargs):
   if 'get' in args and 'pod' in args:return json.dumps(dict(spec=dict(nodeName='test-worker')))
   if 'get' in args and 'node' in args:return json.dumps(dict(spec=dict(unschedulable=False)))
   return ''
  def number(expr):
   if 'state="DOWN"' in expr:return 0
   if 'state="UP"' in expr:return 1
   if 'count(storage_raft_applied_index' in expr:return 9
   if 'relief_workflow_age' in expr:return 10
   return 0
  ticks=itertools.count(0,100)
  with tempfile.TemporaryDirectory() as directory,patch('metrics_fault.Traffic',return_value=traffic),patch('metrics_fault.subprocess.check_output',side_effect=kube),patch('metrics_fault.time.monotonic',side_effect=lambda:next(ticks)),patch('metrics_fault.time.sleep',side_effect=AssertionError('unexpected sleep')),patch('metrics_fault.judge',return_value={}):
   invoke=lambda:run_fault('kind-test','test',old,number,lambda expr:[dict(metric=dict(node=old))],lambda *args:dict(ringVersion=1),'http://fixture',Path(directory))
   if fail_stop:
    with self.assertRaisesRegex(AssertionError,'injected stop failure'):invoke()
   else:self.assertEqual(invoke(),dict(scratchWrites=32))
  traffic.start.assert_called_once_with()
  self.assertEqual(traffic.stop.call_count,1,'outer cleanup executed failed stop twice')
 def test_failed_stop_not_repeated_by_outer_cleanup(self):self.run_mock_fault_lifecycle(True)
 def test_successful_stop_not_repeated_by_outer_cleanup(self):self.run_mock_fault_lifecycle(False)
 @contextlib.contextmanager
 def two_page_fixture(self,fault=None,further=False):
  state={'scans':[],'deletes':[],'rows':{}}
  class Handler(BaseHTTPRequestHandler):
   def reply(self,payload,status=200):
    data=json.dumps(payload).encode();self.send_response(status);self.send_header('Content-Length',str(len(data)));self.end_headers();self.wfile.write(data)
   def log_message(self,*args):pass
   def do_PUT(self):
    body=json.loads(self.rfile.read(int(self.headers['Content-Length'])))
    key=urllib.parse.unquote(urllib.parse.urlsplit(self.path).path.removeprefix('/internal/data/'))
    self.reply(dict(ringVersion=1,result=dict(outcome='OK',operationId=body['operationId'],key=key,version=1)))
   def do_GET(self):
    query={k:v[0] for k,v in urllib.parse.parse_qs(urllib.parse.urlsplit(self.path).query).items()}
    state['scans'].append(query)
    cursor=query.get('cursor','')
    cursors=['',state['cursor']]+([state['last_cursor']] if further else [])
    if cursor not in cursors:
     self.reply(dict(code='INVALID'),400);return
    index=cursors.index(cursor);second=index==1
    key=state['keys'][index]
    active=(0,1,2) if index==0 else ((1,2) if index==1 else (2,))
    remaining=(1,2) if index==0 else ((2,) if second and further else ())
    positions=[dict(shard=i,appliedIndex=10,hasMore=i in remaining) for i in active]
    next_cursor=state['cursor'] if index==0 else (state['last_cursor'] if second and further else '')
    page=dict(partial=False,failedShards=[],records=[state['rows'][key]] if key in state['rows'] else [],
     nextCursor=next_cursor,positions=positions,consistency='strong',ringVersion=1)
    # First page stays valid; each fault changes one second-page property.
    if second:
     if fault=='partial':page['partial']=True
     elif fault=='failed_shard':page['failedShards']=[2]
     elif fault=='consistency':page['consistency']='stale'
     elif fault=='ring':page['ringVersion']=2
     elif fault=='coverage':page['positions']=[pos for pos in positions if pos['shard']!=2]
     elif fault=='duplicate':page['positions']=positions+[dict(positions[0])]
     elif fault=='unexpected':page['positions']=positions+[dict(shard=0,appliedIndex=10,hasMore=False)]
     elif fault=='premature_end':page['nextCursor']=''
    self.reply(page)
   def do_DELETE(self):
    key=urllib.parse.unquote(urllib.parse.urlsplit(self.path).path.removeprefix('/internal/data/'))
    query={k:v[0] for k,v in urllib.parse.parse_qs(urllib.parse.urlsplit(self.path).query).items()}
    condition=self.headers['If-Match'];state['deletes'].append((key,condition))
    row=state['rows'][key]
    if condition!='"'+str(row['version'])+'"':
     self.reply(dict(code='VERSION_CONFLICT',outcomeUnknown=False),412);return
    del state['rows'][key]
    self.reply(dict(ringVersion=1,result=dict(outcome='OK',operationId=query['operationId'],key=key,version=row['version']+1)))
  server=ThreadingHTTPServer(('127.0.0.1',0),Handler);thread=threading.Thread(target=server.serve_forever);thread.start()
  try:
   with tempfile.TemporaryDirectory() as directory:
    traffic=Traffic('http://127.0.0.1:'+str(server.server_port),1,Path(directory))
    state['keys']=[traffic.prefix+':0:0',traffic.prefix+':1:0']+([traffic.prefix+':2:0'] if further else [])
    state['rows']={key:dict(key=key,kind='kv',version=2,value={'syntheticMetricsLoad':'x'*1024}) for key in state['keys']}
    # Encode the actual Store cursor wire shape. Only shards 1 and 2 remain;
    # the exhausted shard 0 must not be required on page two.
    query=dict(kind='kv',type='',statuses=[],prefixes=[],limit=1000)
    fingerprint=hashlib.sha256(json.dumps(query,separators=(',',':')).encode()).hexdigest()[:16]
    def continuation(shards):
     wire=dict(v=1,r=1,q=fingerprint,a=[dict(s=i,k=base64.b64encode(b'').decode()) for i in shards])
     return base64.urlsafe_b64encode(json.dumps(wire,separators=(',',':')).encode()).decode().rstrip('=')
    state['cursor']=continuation((1,2));state['last_cursor']=continuation((2,))
    meta=dict(operationId=str(uuid.uuid4()),at=123,ringVersion=1)
    traffic.pending[meta['operationId']]=dict(key=state['keys'][0],expectedVersion=0,delete=False,meta=meta,
     body=json.dumps(dict(meta,value={'syntheticMetricsLoad':'x'*1024})))
    traffic.errors=['original retry budget exhausted'];traffic.writes=32
    yield traffic,state
  finally:server.shutdown();thread.join();server.server_close()
 def assert_second_page_reached(self,state):
  self.assertGreaterEqual(len(state['scans']),2)
  self.assertNotIn('cursor',state['scans'][0])
  self.assertEqual(state['scans'][1]['cursor'],state['cursor'])
  for query in state['scans']:
   self.assertEqual({k:query[k] for k in ('kind','limit','ringVersion','consistency')},
    dict(kind='kv',limit='1000',ringVersion='1',consistency='strong'))
 def test_two_page_valid_control_cleans_both_pages(self):
  with self.two_page_fixture() as (traffic,state):
   with self.assertRaises(AssertionError):traffic.stop()
   self.assert_second_page_reached(state)
   self.assertEqual(state['rows'],{})
   self.assertCountEqual(state['deletes'],[(key,'"2"') for key in state['keys']])
   self.assertFalse(traffic.pending)
   proof=json.loads((traffic.out/'scratch-recovery.json').read_text())
   self.assertEqual(proof,dict(resolvedIntents=1,deletedKeys=2))
   self.assertEqual(traffic.errors,['original retry budget exhausted'])
 def assert_invalid_second_page(self,fault):
  with self.two_page_fixture(fault=fault,further=fault=='premature_end') as (traffic,state):
   with self.assertRaises(AssertionError):traffic.stop()
   self.assert_second_page_reached(state)
   self.assertEqual(state['deletes'],[],'invalid page two authorized cleanup, including page-one data')
   self.assertEqual(set(state['rows']),set(state['keys']))
   self.assertFalse((traffic.out/'scratch-recovery.json').exists(),'invalid page two produced a success proof')
   self.assertIn('original retry budget exhausted',traffic.errors)
   self.assertTrue(any('scratch recovery:' in error for error in traffic.errors))
 def test_further_shard_exhaustion_cleans_complete_walk(self):
  with self.two_page_fixture(further=True) as (traffic,state):
   with self.assertRaises(AssertionError):traffic.stop()
   self.assert_second_page_reached(state)
   self.assertEqual([query.get('cursor','') for query in state['scans'][:3]],['',state['cursor'],state['last_cursor']])
   self.assertEqual(state['rows'],{})
   self.assertCountEqual(state['deletes'],[(key,'"2"') for key in state['keys']])
   self.assertFalse(traffic.pending)
   self.assertEqual(json.loads((traffic.out/'scratch-recovery.json').read_text()),dict(resolvedIntents=1,deletedKeys=3))
   self.assertEqual(traffic.errors,['original retry budget exhausted'])
 def test_two_page_duplicate_position_rejected_before_any_delete(self):self.assert_invalid_second_page('duplicate')
 def test_two_page_exhausted_shard_reappearing_rejected_before_any_delete(self):self.assert_invalid_second_page('unexpected')
 def test_premature_continuation_end_rejected_before_any_delete(self):self.assert_invalid_second_page('premature_end')
 def test_two_page_partial_rejected_before_any_delete(self):self.assert_invalid_second_page('partial')
 def test_two_page_failed_shard_rejected_before_any_delete(self):self.assert_invalid_second_page('failed_shard')
 def test_two_page_weak_consistency_rejected_before_any_delete(self):self.assert_invalid_second_page('consistency')
 def test_two_page_wrong_ring_rejected_before_any_delete(self):self.assert_invalid_second_page('ring')
 def test_two_page_missing_required_shard_rejected_before_any_delete(self):self.assert_invalid_second_page('coverage')
if __name__=='__main__':unittest.main()
