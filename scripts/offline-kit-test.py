import ipaddress
import hashlib
import io
import tarfile
import importlib.util
import json
import os
import subprocess
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

spec=importlib.util.spec_from_file_location('offline_kit',Path(__file__).with_name('offline-kit.py'))
module=importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)


class OfflineKitTest(unittest.TestCase):
 def test_corrupt_missing_and_escaping_assets_fail_before_deployment(self):
  with tempfile.TemporaryDirectory() as tmp:
   root=Path(tmp);asset=root/'asset';asset.write_bytes(b'original')
   manifest={'version':1,'files':{'asset':hashlib.sha256(b'original').hexdigest()}}
   (root/'manifest.json').write_text(json.dumps(manifest))
   module.verify_files(root)
   asset.write_bytes(b'changed')
   with self.assertRaises(ValueError):module.verify_files(root)
   asset.unlink()
   with self.assertRaises(ValueError):module.verify_files(root)
   manifest['files']={'../outside':'0'*64};(root/'manifest.json').write_text(json.dumps(manifest))
   with self.assertRaises(ValueError):module.verify_files(root)
 def test_read_only_verification_never_deploys(self):
  with tempfile.TemporaryDirectory() as tmp:
   root=Path(tmp);(root/'manifest.json').write_text(json.dumps({'version':1,'files':{}}))
   with self.assertRaises(ValueError):module.verify_files(root)
 def test_prebuilt_drainer_is_used_without_a_build_or_network_fallback(self):
  with tempfile.TemporaryDirectory() as tmp:
   root=Path(tmp);binary=root/'drainer';target=root/'copied';marker=root/'unexpected-build'
   binary.write_text('#!/bin/sh\nexit 0\n');binary.chmod(0o755)
   go=root/'go';go.write_text('#!/bin/sh\ntouch "$UNEXPECTED_BUILD"\nexit 99\n');go.chmod(0o755)
   env=dict(os.environ,PATH=str(root)+os.pathsep+os.environ['PATH'],DEMO_OUTBOX_DRAIN_BINARY=str(binary),UNEXPECTED_BUILD=str(marker))
   script=Path(__file__).with_name('prepare-outbox-drainer.sh')
   subprocess.run(['bash',str(script),str(target)],env=env,check=True,timeout=5)
   self.assertEqual(target.read_bytes(),binary.read_bytes());self.assertTrue(os.access(target,os.X_OK));self.assertFalse(marker.exists())
   binary.unlink()
   result=subprocess.run(['bash',str(script),str(target)],env=env,capture_output=True,timeout=5)
   self.assertNotEqual(result.returncode,0);self.assertFalse(marker.exists())
 def test_only_the_external_export_failure_can_use_private_api_recovery(self):
  known='failed to get api server port: .NetworkSettings.Ports: slice index out of range'
  self.assertTrue(module.external_export_unavailable(known))
  for failure in ['bootstrap failed','node not Ready','failed to get api server port','slice index out of range']:
   self.assertFalse(module.external_export_unavailable(failure))
 def test_only_new_isolated_kind_names_are_allowed(self):
  for name in ['reliefgrid','production','../bad','reliefgrid-offline;evil']:
   with self.subTest(name=name),self.assertRaises(ValueError):module.check_cluster_name(name)
  module.check_cluster_name('reliefgrid-offline-check')


 def test_missing_oci_blob_is_rejected_before_cluster_creation(self):
  with tempfile.TemporaryDirectory() as tmp:
   root=Path(tmp);archive=root/'images.tar'
   config=b'{"architecture":"amd64","os":"linux"}'
   checksum=hashlib.sha256(config).hexdigest()
   index=json.dumps({'manifests':[{'mediaType':'application/vnd.oci.image.config.v1+json','digest':'sha256:'+checksum,'size':len(config)}]}).encode()
   def write(include):
    with tarfile.open(archive,'w') as output:
     for name,data in [('index.json',index)]+([('blobs/sha256/'+checksum,config)] if include else []):
      member=tarfile.TarInfo(name);member.size=len(data);output.addfile(member,io.BytesIO(data))
   validate=getattr(module,'verify_image_archive',lambda path:None)
   write(True);validate(archive)
   write(False)
   with self.assertRaises(ValueError):validate(archive)



 def test_image_identity_is_inspected_for_the_exported_platform(self):
  with patch.object(module,'run',return_value='[{}]') as command:
   module.inspect('example:tag')
  self.assertIn('--platform=linux/amd64',command.call_args.args)



 def test_internal_bridge_contains_routed_pods_but_not_external_destinations(self):
  network=ipaddress.ip_network(getattr(module,'OFFLINE_SUBNET','172.22.0.0/16'))
  pods=ipaddress.ip_network('10.244.0.0/16')
  nodes=ipaddress.ip_network(getattr(module,'OFFLINE_NODE_POOL','172.22.0.0/24'))
  self.assertTrue(pods.subnet_of(network),'Docker internal filtering drops routed pod traffic outside its subnet')
  self.assertTrue(nodes.subnet_of(network));self.assertFalse(nodes.overlaps(pods))
  self.assertNotIn(ipaddress.ip_address('1.1.1.1'),network)



 def test_generated_offline_values_render_the_exact_local_traefik_tag(self):
  roles={'rabbitmq':'local/rabbit:revision'}
  values=module.offline_app_values(roles,'offline-test')
  with tempfile.TemporaryDirectory() as tmp:
   path=Path(tmp)/'values.json';path.write_text(json.dumps(values))
   result=subprocess.run(['helm','template','reliefgrid','deploy/helm/reliefgrid','-f',str(path)],
      cwd=module.ROOT,capture_output=True,text=True,timeout=30)
   self.assertEqual(result.returncode,0,result.stderr)
   import yaml
   deployment=next(d for d in yaml.safe_load_all(result.stdout) if d and d.get('kind')=='Deployment' and d['metadata']['name']=='reliefgrid-traefik')
   self.assertEqual(deployment['spec']['template']['spec']['containers'][0]['image'],'docker.io/reliefgrid-offline/traefik:offline-test')



 def test_runner_api_forward_exports_loopback_and_is_stopped(self):
  with tempfile.TemporaryDirectory() as tmp:
   root=Path(tmp);binary=root/'kubectl'
   binary.write_text('#!/usr/bin/env python3\n'+"""from http.server import HTTPServer,BaseHTTPRequestHandler
class Handler(BaseHTTPRequestHandler):
 def do_GET(self):
  self.send_response(200);self.end_headers();self.wfile.write(b'{}')
 def log_message(self,*args):pass
s=HTTPServer(('127.0.0.1',0),Handler)
print('Forwarding from 127.0.0.1:'+str(s.server_port)+' -> 80',flush=True)
s.serve_forever()
""")
   binary.chmod(0o755)
   env=dict(os.environ,PATH=str(root)+os.pathsep+os.environ['PATH'],EVIDENCE_ROOT=tmp,KUBE_CONTEXT='kind-test',NAMESPACE='test')
   helper=module.ROOT/'deploy/offline/forward-api.sh'
   script='source "$1"; trap cleanup_api_forward EXIT; start_api_forward; saved="$API_FORWARD_PID"; [[ "$API_URL" == http://127.0.0.1:*/api/v1 ]]; cleanup_api_forward; ! kill -0 "$saved" 2>/dev/null'
   subprocess.run(['bash','-euc',script,'test',str(helper)],env=env,check=True,timeout=10)



if __name__=='__main__':unittest.main()
