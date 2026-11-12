#!/usr/bin/env python3
"""Run the 16 S-29 review mutations; restore each source even on test failure.

Usage: python3 scripts/internal-auth-mutations.py [output-directory]
Requires Go and Helm. Checks 14 and 16 exercise both binaries independently.
Run without concurrent builds/tests in this checkout. RED means the test kills
the mutation; GREEN means it survives; INVALID means a non-test failure.
"""
from pathlib import Path
import subprocess,json,sys
root=Path(__file__).resolve().parents[1]
logs=Path(sys.argv[1] if len(sys.argv)>1 else '/tmp/reliefgrid-s29-mutations');logs.mkdir(parents=True,exist_ok=True)
checks=[]
def add(num,label,path,old,new,pkg,test):
 def mutate(s):
  assert old in s,(num,old)
  return s.replace(old,new,1)
 checks.append((str(num),label,path,mutate,pkg,test))
auth='internal/auth/auth.go';client='internal/coordclient/coordclient.go';route='internal/coordinator/route.go'
add(1,'reject receiver overlap',auth,'return a|b == 1','_ = b\n return a == 1','./internal/auth','TestReceiver')
add(2,'accept empty header',auth,'count != 1 || !validToken(candidate)','count != 1','./internal/auth','TestReceiver')
add(3,'skip receiver config validation',auth,'c.Validate() != nil || r == nil','r == nil','./internal/auth','TestReceiver')
add(4,'protect only data prefix','internal/coordinator/coordinator.go','strings.HasPrefix(r.URL.Path, "/internal/")','strings.HasPrefix(r.URL.Path, "/internal/data")','./internal/coordinator','TestCoordinatorSeparates')
add(5,'app prefix before node prefix','internal/coordinator/coordinator.go','''case strings.HasPrefix(r.URL.Path, "/internal/nodes/"):
			nodeHandler.ServeHTTP(w, r)
		case strings.HasPrefix(r.URL.Path, "/internal/"):
			appHandler.ServeHTTP(w, r)''','''case strings.HasPrefix(r.URL.Path, "/internal/"):
            appHandler.ServeHTTP(w,r)
        case strings.HasPrefix(r.URL.Path, "/internal/nodes/"):
            nodeHandler.ServeHTTP(w,r)''','./internal/coordinator','TestCoordinatorSeparates')
# Keep import in use so removal is a behavioral failure, not a compiler error.
add(6,'bypass storage receiver','internal/storagenode/server.go','handler = internalauth.Receiver(*s.internalAuth, mux)','_ = internalauth.Receiver(*s.internalAuth, mux)','./internal/storagenode','TestDataAuth')
add(7,'erase earlier forwarding uncertainty',route,'''if unknown {
					return nil, true, "authentication refused after an unresolved attempt; retry the same operationId"
				}''','','./internal/coordinator','TestAuthRefusalAfterUnknown')
add(8,'malformed refusal loses uncertainty',route,'''unknown = unknown || mutation
			return nil, unknown, "unrecognized authentication response"''','''return nil, unknown, "unrecognized authentication response"''','./internal/coordinator','TestMalformedStorageAuth')
add(9,'forget earlier client uncertainty',client,'err = &Error{Code: internalapi.OUTCOMEUNKNOWN, Message: "earlier attempt unresolved; retry the same operationId: " + err.Error(), Retryable: true, OutcomeUnknown: true}','_ = err','./internal/coordclient','TestAuthRefusalKeepsEarlierUnknown')
add(10,'never resolve committed operation',client,'delete(c.unknown, identity)','// omit resolution','./internal/coordclient','TestMalformedAuthRefusal')
add(11,'drop DELETE operation identity',client,'operationID := m.query.Get("operationId")','operationID := ""','./internal/coordclient','TestDeleteAuthRefusal')
add(12,'accept 401 carrying result or unknown',client,'|| e.Result != nil || e.OutcomeUnknown','','./internal/coordclient','TestStructured401')
add(13,'ignore unreadable previous file','internal/internalauth/wiring.go','if err != nil && !errors.Is(err, os.ErrNotExist) {','if false && err != nil && !errors.Is(err, os.ErrNotExist) {','./internal/internalauth','TestUnreadablePrevious')
for binary,var in [('coordinator','nodeAuth'),('storagenode','loaded')]:
 add('14-'+binary,'ignore node receiver load error','cmd/'+binary+'/main.go',var+''', err := cfg.NodeAuthFiles.Load()
		if err != nil {
			return err
		}''',var+''', err := cfg.NodeAuthFiles.Load()
        _ = err''','./cmd/'+binary,'TestEachCredential')
add(15,'force sender to receiver current','deploy/helm/storage/templates/internal-auth-secret.yaml','default $token $cfg.senderToken','$token','./scripts','TestInternalAuthChartSecrets')
for binary in ['coordinator','storagenode']:
 def move(s,binary=binary):
  # Move the enabled credential-loading block after the first Listen call.
  start=s.index('\tif cfg.InternalAuthEnabled {',s.index('func run('))
  end=start; depth=0; opened=False
  for i in range(start,len(s)):
   if s[i]=='{':depth+=1;opened=True
   elif s[i]=='}':
    depth-=1
    if opened and depth==0:end=i+1;break
  block=s[start:end];s=s[:start]+s[end:]
  needle='ln, err := listen(ctx, "tcp", cfg.HTTPAddr)' if binary=='coordinator' else 'ln, err := listen(context.Background(), "tcp", srv.Addr)'
  a=s.index(needle);ret=s.index('\n\t}',a) if binary=='coordinator' else s.index('\n\t\t}',a)
  # Insert after listen's error handling. Invalid credentials must win over Listen errors.
  ret+=len('\n\t}') if binary=='coordinator' else len('\n\t\t}')
  return s[:ret]+'\n'+block+s[ret:]
 checks.append(('16-'+binary,'load auth after Listen','cmd/'+binary+'/main.go',move,'./cmd/'+binary,'TestEachCredential'))
results=[]
baselines=set()
for num,label,path,mutate,pkg,test in checks:
 command=['go','test',pkg,'-run',test,'-count=1','-timeout=60s']
 if (pkg,test) not in baselines:
  baseline=subprocess.run(command,cwd=root,stdout=subprocess.PIPE,stderr=subprocess.STDOUT,text=True,timeout=110)
  (logs/(num+'-baseline.log')).write_text(baseline.stdout)
  if baseline.returncode!=0:
   print(num,'BASELINE FAILED',flush=True)
   sys.exit(2)
  baselines.add((pkg,test))
 p=root/path;original=p.read_bytes()
 try:
  modified=mutate(original.decode());assert modified!=original.decode();p.write_text(modified)
  command=['go','test',pkg,'-run',test,'-count=1','-timeout=60s']
  run=subprocess.run(command,cwd=root,stdout=subprocess.PIPE,stderr=subprocess.STDOUT,text=True,timeout=110)
  output=run.stdout
  # Killed means actual test assertion failed, not broken compilation/environment.
  red=run.returncode!=0 and '--- FAIL:' in output and '[build failed]' not in output
  status='RED' if red else 'GREEN' if run.returncode==0 else 'INVALID'
  (logs/(num+'.log')).write_text(output)
  results.append(dict(id=num,label=label,baseline='GREEN',status=status,exit=run.returncode))
  print(num,status,label,flush=True)
 finally:p.write_bytes(original)
(logs/'results.json').write_text(json.dumps(results,indent=2)+'\n')
sys.exit(0 if all(r['status']=='RED' for r in results) else 1)
