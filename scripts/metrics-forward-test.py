import contextlib,importlib.util,json,os,sys,tempfile,unittest,urllib.request
from pathlib import Path
from unittest.mock import patch
spec=importlib.util.spec_from_file_location('live',Path(__file__).with_name('verify-A4.2-live.py'))
live=importlib.util.module_from_spec(spec);spec.loader.exec_module(live)
class ForwardTest(unittest.TestCase):
 def test_noisy_forward_remains_live_and_preserves_log(self):
  with tempfile.TemporaryDirectory() as directory:
   root=Path(directory); binary=root/'kubectl'
   binary.write_text('#!'+sys.executable+'\n'+"""import http.server,sys
s=http.server.HTTPServer(('127.0.0.1',0),http.server.SimpleHTTPRequestHandler)
print('Forwarding from 127.0.0.1:'+str(s.server_port)+' -> 8080',flush=True)
sys.stdout.write('x'*2000000+'\\n');sys.stdout.flush()
s.serve_forever()
""")
   binary.chmod(0o755)
   with patch.dict(os.environ,{'PATH':str(root)+':'+os.environ['PATH']}):
    with live.forward('kind-test','test','test',8080,root/'forward.log') as origin:
     with urllib.request.build_opener(urllib.request.ProxyHandler({})).open(origin,timeout=2) as response:
      self.assertEqual(response.status,200)
   self.assertGreater((root/'forward.log').stat().st_size,2000000)
if __name__=='__main__':unittest.main()
