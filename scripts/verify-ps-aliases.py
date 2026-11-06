#!/usr/bin/env python3
"""Verify seven PS aliases against canonical handlers through real Traefik."""
import datetime as dt
import json
import os
from pathlib import Path
import re
import subprocess
import tempfile
import time
import urllib.request
import urllib.error
import uuid

root=Path(__file__).resolve().parents[1]
os.chdir(root)
out=Path(os.environ.get('PS_EVIDENCE_DIR','docs/evidence/A0.1-PS/'+dt.datetime.now(dt.timezone.utc).strftime('%Y%m%dT%H%M%SZ')))
out.mkdir(parents=True,exist_ok=False)
report=dict(result='FAIL',operations=[])
context='kind-'+os.environ.get('CLUSTER','reliefgrid')
with (out/'traefik-forward.log').open('w+') as log:
    forward=subprocess.Popen(['kubectl','--context',context,'--request-timeout=10s','-n','reliefgrid','port-forward','svc/reliefgrid-traefik',':80','--address=127.0.0.1'],stdout=log,stderr=subprocess.STDOUT)
    rid=resid=None
    ui_forward=None
    ui_log=None
    try:
        deadline=time.monotonic()+20
        while True:
            log.seek(0);match=re.search(r'Forwarding from 127\.0\.0\.1:(\d+)',log.read())
            if match:break
            assert time.monotonic()<deadline and forward.poll() is None
            time.sleep(.1)
        base='http://127.0.0.1:'+match[1]
        ui_log=(out/'ui-forward.log').open('w+')
        ui_forward=subprocess.Popen(['kubectl','--context',context,'--request-timeout=10s','-n','reliefgrid','port-forward','svc/reliefgrid-ui',':8080','--address=127.0.0.1'],stdout=ui_log,stderr=subprocess.STDOUT)
        deadline=time.monotonic()+20
        while True:
            ui_log.seek(0);m=re.search(r'Forwarding from 127\.0\.0\.1:(\d+)',ui_log.read())
            if m:break
            assert time.monotonic()<deadline and ui_forward.poll() is None
            time.sleep(.1)
        ui='http://127.0.0.1:'+m[1]
        def ui_response(url):
            try:
                with urllib.request.urlopen(url,timeout=10) as response:
                    return response.status,response.headers.get('Content-Type',''),response.read()
            except urllib.error.HTTPError as error:
                return error.code,error.headers.get('Content-Type',''),error.read()

        def call(method,path,body=None,key=None,json_response=True):
            headers={'Content-Type':'application/json'}
            if key:headers['Idempotency-Key']=key
            request=urllib.request.Request(base+path,method=method,headers=headers,data=None if body is None else json.dumps(body).encode())
            try:
                with urllib.request.urlopen(request,timeout=10) as response:
                    status,ctype,raw=response.status,response.headers.get('Content-Type',''),response.read()
            except urllib.error.HTTPError as error:
                status,ctype,raw=error.code,error.headers.get('Content-Type',''),error.read()
            value=json.loads(raw) if json_response and 'application/json' in ctype else None
            report['operations'].append(dict(method=method,path=path,status=status,contentType=ctype))
            return status,value,ctype
        def both(method,path,expected,body=None,key=None):
            results=[call(method,prefix+path,body,key) for prefix in ('','/api/v1')]
            assert all(r[0]==expected and r[1] is not None for r in results),(path,results)
            return results
        now=dt.datetime.now(dt.timezone.utc)
        resource=dict(resourceType='FOOD',name='Synthetic PS alias fixture',location=dict(lat=83,lng=-168),
            windowStart=(now-dt.timedelta(minutes=1)).isoformat(),windowEnd=(now+dt.timedelta(hours=2)).isoformat(),reusable=True)
        first,second=both('POST','/resources',201,resource,str(uuid.uuid4()))
        resid=first[1]['resourceId']
        assert first[1]==second[1], 'cross-prefix keyed resource replay changed the original response'
        both('GET','/resources?type=FOOD&near=83,-168&radiusKm=1&limit=100',200)
        both('PUT','/resources/'+resid+'/status',200,dict(status='OFFLINE'))
        assert call('PUT','/api/v1/resources/'+resid+'/status',dict(status='AVAILABLE'))[0]==200
        body=dict(requestType='FOOD',details='Synthetic PS alias request',peopleAffected=1,urgency='HIGH',location=dict(lat=83,lng=-168))
        first,second=both('POST','/requests',201,body,str(uuid.uuid4()))
        rid=first[1]['requestId']
        assert first[1]==second[1], 'cross-prefix keyed request replay changed the original response'
        both('GET','/requests/'+rid,200)
        deadline=time.monotonic()+30
        while True:
            status,value,_=call('POST','/match/'+rid)
            if status==200 and value['outcome']=='MATCHED':break
            assert status==202 and time.monotonic()<deadline,(status,value)
            time.sleep(.1)
        both('POST','/match/'+rid,200)
        matched=call('GET','/api/v1/requests/'+rid)[1]
        assert matched['assignedResourceId']==resid
        both('PUT','/requests/'+rid+'/status',200,dict(status='IN_PROGRESS'))
        for path in ('/','/requests/new','/resources/register','/match/board','/events'):
            status,_,content=call('GET',path,json_response=False)
            assert ui_response(base+path)==ui_response(ui+path), 'alias shadowed UI response at '+path
            if path=='/':assert status==200 and 'text/html' in content
        report['result']='PASS'
    finally:
        try:
            if rid:
                assert call('PUT','/api/v1/requests/'+rid+'/status',dict(status='CANCELLED'))[0]==200
                end=time.monotonic()+180
                while call('GET','/api/v1/requests/'+rid)[1]['status']!='CANCELLED':
                    assert time.monotonic()<end,'PS cancellation cleanup did not settle'
                    time.sleep(.3)
            if resid:
                status,value,_=call('PUT','/api/v1/resources/'+resid+'/status',dict(status='OFFLINE'))
                assert status==200 and not value.get('activeClaim')
        except BaseException:
            report['result']='FAIL'
            raise
        finally:
            forward.terminate();forward.wait(timeout=10)
            if ui_forward is not None:ui_forward.terminate();ui_forward.wait(timeout=10)
            if ui_log is not None:ui_log.close()
            (out/'results.json').write_text(json.dumps(report,indent=2)+'\n')
print('PS ALIASES LIVE: '+report['result']+' evidence='+str(out))
