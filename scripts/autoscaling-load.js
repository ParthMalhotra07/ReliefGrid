import http from 'k6/http';
import {check, sleep} from 'k6';
import encoding from 'k6/encoding';

// A bounded duplicate-delivery storm for one already-cancelled fixture. Every
// delivery still incurs the consumer's authoritative read; it cannot create
// thousands of persistent WAITING workflows or bypass the production consumer.
export const options = {
  // The queue deliberately rejects publishes above its 10000-message bound.
  // Pace the same 20000 hints so this tests autoscaling, not broker overflow.
  scenarios: {redeliveries: {executor: 'shared-iterations', vus: 32,
    iterations: 20000, maxDuration: '180s'}},
  thresholds: {checks: ['rate==1'], http_req_failed: ['rate==0'], dropped_iterations: ['count==0']},
};
const headers = {'Content-Type': 'application/json',
  Authorization: 'Basic ' + encoding.b64encode(__ENV.BROKER_USER + ':' + __ENV.BROKER_PASSWORD)};
const body = JSON.stringify({properties: {delivery_mode: 2, content_type: 'application/json'},
  routing_key: 'request.created.high', payload: __ENV.REQUEST_EVENT, payload_encoding: 'string'});
export default function () {
  const r = http.post(__ENV.BROKER_URL + '/api/exchanges/%2F/relief.requests/publish', body,
    {headers, timeout: '10s', tags: {name: 'matching-redelivery'}});
  check(r, {'persistent hint routed': response => response.status === 200 && response.json('routed') === true});
  // Offered-load pacing, not a correctness wait: at most 250 hints/s with
  // 32 VUs, plus measured HTTP latency. Exact shared iterations bound the run.
  sleep(0.128);
}
