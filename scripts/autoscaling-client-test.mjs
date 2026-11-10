import test from 'node:test';
import assert from 'node:assert/strict';
import {cancelRequest} from './autoscaling-client.js';
const response = (status, body={}) => ({status,json:()=>body});
const conflict = () => response(409,{code:'VERSION_CONFLICT',outcomeUnknown:false});

test('known rejected CAS rereads before retrying cancellation',()=>{
 const order=[];
 const result=cancelRequest(()=>{order.push('put');return order.length===1?conflict():response(200);},()=>{order.push('read');return response(200);});
 assert.equal(result.status,200);
 assert.deepEqual(order,['put','read','put']);
});
test('permanent conflict remains failure after exactly three attempts',()=>{
 let puts=0,reads=0;
 const result=cancelRequest(()=>{puts++;return conflict();},()=>{reads++;return response(200);});
 assert.equal(result.status,409);assert.equal(puts,3);assert.equal(reads,2);
});
test('unknown outcome, domain conflicts and server failures are never retried or accepted',()=>{
 for(const reply of [response(503,{outcomeUnknown:true}),response(409,{code:'DOMAIN_CONFLICT',outcomeUnknown:false}),response(409,{code:'VERSION_CONFLICT',outcomeUnknown:true}),response(500),{status:409,json:()=>{throw Error('malformed');}}]){
  let puts=0;
  assert.equal(cancelRequest(()=>{puts++;return reply;},()=>{throw Error('unsafe retry');}),reply);
  assert.equal(puts,1);
 }
});
test('failed authoritative reread stops the retry',()=>{
 let puts=0;const failed=response(503);
 assert.equal(cancelRequest(()=>{puts++;return conflict();},()=>failed),failed);
 assert.equal(puts,1);
});
test('successful first cancellation is never duplicated',()=>{
 const ok=response(200);let puts=0;
 assert.equal(cancelRequest(()=>{puts++;return ok;},()=>{throw Error('unnecessary read');}),ok);
 assert.equal(puts,1);
});
