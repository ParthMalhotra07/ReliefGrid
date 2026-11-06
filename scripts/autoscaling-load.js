import http from 'k6/http';
import {check, sleep} from 'k6';
import exec from 'k6/execution';

// Closed-loop real lifecycles bound unfinished work to the VU count. CPU and
// workflow peaks are separate, so this measures autoscalers rather than the
// single coordinator's simultaneous read+write saturation limit.
export const options={scenarios:{
  workflows:{executor:'constant-vus',exec:'lifecycle',vus:32,duration:'90s',gracefulStop:'180s'},
  tail:{executor:'constant-vus',exec:'lifecycle',vus:1,startTime:'120s',duration:'300s',gracefulStop:'180s'},
  api:{executor:'constant-vus',exec:'apiReads',vus:48,startTime:'120s',duration:'90s'},
},thresholds:{checks:['rate==1'],http_req_failed:['rate==0']}};
const base=__ENV.API_URL+'/api/v1';
export function lifecycle(){
  const index=exec.scenario.iterationInTest+(exec.scenario.name==='tail'?100000:0);
  const key=__ENV.RUN_ID.slice(0,24)+String(index).padStart(12,'0');
  const body=JSON.stringify({requestType:'SHELTER',details:'Synthetic A4.3 real workflow '+index,peopleAffected:1,urgency:'HIGH',location:{lat:56,lng:-123}});
  const created=http.post(base+'/requests',body,{headers:{'Content-Type':'application/json','Idempotency-Key':key},timeout:'10s',tags:{name:'create-workflow'}});
  if(!check(created,{'request durably created':r=>r.status===201&&!!r.json('requestId')}))return;
  const rid=created.json('requestId'), version=created.json('version');
  console.log('A43_CREATED '+JSON.stringify({requestId:rid,initialVersion:version,key,phase:exec.scenario.name}));
  let current;
  const deadline=Date.now()+30000;
  do{
    current=http.get(base+'/requests/'+rid,{timeout:'10s',tags:{name:'observe-workflow'}});
    if(current.status===200&&current.json('version')!==version&&['WAITING','MATCHED','IN_PROGRESS'].includes(current.json('status')))break;
    sleep(.1);
  }while(Date.now()<deadline);
  const handed=check(current,{'real durable handoff observed':r=>r.status===200&&r.json('version')!==version&&['WAITING','MATCHED','IN_PROGRESS'].includes(r.json('status'))});
  if(!handed)return;
  const status=current.json('status');
  let reciprocal=status==='WAITING';
  if(status==='MATCHED'||status==='IN_PROGRESS'){
    const resources=http.get(base+'/resources?type=SHELTER&near=56,-123&radiusKm=1&limit=100',{timeout:'10s',tags:{name:'reciprocal-claim'}});
    if(resources.status===200){
      const r=resources.json('items').find(r=>r.resourceId===current.json('assignedResourceId'));
      reciprocal=resources.json('partial')===false&&!!r&&r.activeClaim&&r.activeClaim.requestId===rid;
    }
  }
  check(reciprocal,{'current match has reciprocal claim':r=>!!r});
  console.log('A43_HANDOFF '+JSON.stringify({requestId:rid,status,version:current.json('version'),reciprocalVerified:!!reciprocal}));
  const cancelled=http.put(base+'/requests/'+rid+'/status',JSON.stringify({status:'CANCELLED'}),{headers:{'Content-Type':'application/json'},timeout:'10s',tags:{name:'cancel-workflow'}});
  if(cancelled.status!==200)console.error('A43_CANCEL_FAILURE '+JSON.stringify({requestId:rid,status:cancelled.status,body:cancelled.body}));
  if(!check(cancelled,{'cancel accepted':r=>r.status===200}))return;
  // The documented complete reconciler walk is125s; allow one full walk plus
  // margin, keeping the terminal/no-claim assertion unchanged.
  const end=Date.now()+180000;
  do{
    current=http.get(base+'/requests/'+rid,{timeout:'10s',tags:{name:'observe-terminal'}});
    if(current.status===200&&current.json('status')==='CANCELLED')break;
    sleep(.1);
  }while(Date.now()<end);
  check(current,{'terminal state owns no resource':r=>r.status===200&&r.json('status')==='CANCELLED'&&!r.json('assignedResourceId')});
  sleep(1);
}
export function apiReads(){
  const response=http.get(base+'/requests/'+__ENV.READ_REQUEST,{timeout:'10s',tags:{name:'strong-request-read'}});
  check(response,{'strong request read succeeds':r=>r.status===200&&r.json('requestId')===__ENV.READ_REQUEST});
}
