// Shared k6 code for the S5.2 scenarios. Each iteration replays the next
// operation of the seeded dataset (loadtest/dataset, written by
// `go run ./loadtest/cmd/gendata`): the dataset fixes the payloads and their
// order, the scenario fixes the arrival rate.
//
// Environment:
//   DATASET  path to the dataset JSON (required)
//   API_URL  public API base, default http://127.0.0.1:8080 (Traefik on kind)
//
// Every request is tagged class=write|read|list and op=<dataset kind>, with a
// fixed name per route, so the summary has bounded series. Creates also add
// a point to created_request tagged request_id: that goes to the raw output
// only (never a threshold or summary key), for the post-run collector that
// reads each request's final status (matched or waiting).
import http from 'k6/http';
import exec from 'k6/execution';
import { check } from 'k6';
import { Counter } from 'k6/metrics';
import { SharedArray } from 'k6/data';

const API = (__ENV.API_URL || 'http://127.0.0.1:8080') + '/api/v1';
if (!__ENV.DATASET) {
  throw new Error('DATASET is required: go run ./loadtest/cmd/gendata -run <run> -out <file>');
}
// k6 runs this module's init code once per VU, so the dataset is read and
// parsed only inside the SharedArray callback, which runs once; every VU then
// shares it. Element 0 is the dataset's metadata, elements 1..N its
// operations (a SharedArray decodes one element per access, so each stays
// small). Parsed at module scope, every VU held its own copy: a 15 MB
// dataset grew k6 to 22.4 GB at 116 VUs in the exploratory 10->160 run.
const dataset = new SharedArray('dataset', () => {
  const file = JSON.parse(open(__ENV.DATASET));
  return [{ seed: file.seed, run: file.run, counts: file.counts, mix: file.mix }].concat(file.ops);
});
export const meta = dataset[0];
const opCount = dataset.length - 1;

export const createdRequest = new Counter('created_request');
export const apiErrors = new Counter('api_errors');
export const datasetExhausted = new Counter('dataset_exhausted');
// A match or read before this VU has created any request is skipped.
export const skipped = new Counter('skipped_ops');

const json = { 'Content-Type': 'application/json' };
const writeKinds = new Set(['create_request', 'register_resource', 'match']);
const listKinds = new Set(['list_requests', 'list_resources']);

// classOf: write (creates, registrations, matches), read (one key by ID) or
// list (a listing: a scan of every shard), each with its own threshold.
function classOf(op) {
  if (writeKinds.has(op)) return 'write';
  return listKinds.has(op) ? 'list' : 'read';
}

// Per VU: the IDs of the requests it created, newest last (at most 64).
const mine = [];

function remember(id) {
  mine.push(id);
  if (mine.length > 64) mine.shift();
}

function ref(n) {
  return mine[Math.max(0, mine.length - n)];
}

function errorCode(res) {
  try {
    return (res.json() || {}).code || 'NONE';
  } catch (_) {
    return 'NOT_JSON';
  }
}

function send(method, url, body, op, name, extraHeaders) {
  const tags = { class: classOf(op), op: op, name: name };
  const params = { headers: Object.assign({}, json, extraHeaders || {}), tags: tags,
    responseCallback: http.expectedStatuses({ min: 200, max: 299 }) };
  // Dataset bodies are JSON objects; k6 would form-encode an object.
  const payload = body === null || typeof body === 'string' ? body : JSON.stringify(body);
  const res = http.request(method, url, payload, params);
  if (res.status < 200 || res.status > 299) {
    apiErrors.add(1, { status: String(res.status), code: res.status === 0 ? 'TRANSPORT' : errorCode(res), op: op });
  }
  return res;
}

function createRequest(op) {
  const res = send('POST', `${API}/requests`, op.body, 'create_request', 'POST /requests', { 'Idempotency-Key': op.key });
  const ok = check(res, { 'create_request 201': (r) => r.status === 201 });
  if (ok) {
    const id = res.json('requestId');
    if (id) {
      remember(id);
      createdRequest.add(1, { request_id: id });
    }
  }
}

// run replays dataset operation i (the scenario's global iteration).
export function run() {
  const i = exec.scenario.iterationInTest;
  if (i >= opCount) {
    datasetExhausted.add(1);
    return;
  }
  const op = dataset[i + 1];
  switch (op.op) {
    case 'create_request':
      createRequest(op);
      return;
    case 'register_resource': {
      const res = send('POST', `${API}/resources`, op.body, op.op, 'POST /resources', { 'Idempotency-Key': op.key });
      check(res, { 'register_resource 201': (r) => r.status === 201 });
      return;
    }
    case 'match':
    case 'read_request': {
      const id = ref(op.ref);
      if (!id) {
        skipped.add(1, { op: op.op });
        return; // nothing of ours to read or match yet
      }
      if (op.op === 'match') {
        const res = send('POST', `${API}/match/${id}`, null, op.op, 'POST /match/{id}');
        check(res, { 'match 200/202': (r) => r.status === 200 || r.status === 202 });
      } else {
        const res = send('GET', `${API}/requests/${id}`, null, op.op, 'GET /requests/{id}');
        check(res, { 'read_request 200': (r) => r.status === 200 });
      }
      return;
    }
    case 'list_requests': {
      const res = send('GET', `${API}/requests?${op.query}`, null, op.op, 'GET /requests');
      check(res, { 'list_requests 200': (r) => r.status === 200 });
      return;
    }
    case 'list_resources': {
      const res = send('GET', `${API}/resources?${op.query}`, null, op.op, 'GET /resources');
      check(res, { 'list_resources 200': (r) => r.status === 200 });
      return;
    }
    default:
      throw new Error(`dataset op ${i}: unknown kind ${op.op}`);
  }
}

// The S5.2 thresholds (decided 2026-11-06). They mark the run; the report
// tool decides the saturation step from the per-step figures.
export const thresholds = {
  'http_req_duration{class:write}': ['p(99)<=1000'],
  'http_req_duration{class:read}': ['p(99)<=250'],
  'http_req_duration{class:list}': ['p(99)<=500'],
  'http_req_failed': ['rate<=0.01'],
  'dropped_iterations': ['count==0'],
};

export const summaryTrendStats = ['count', 'min', 'med', 'p(95)', 'p(99)', 'max', 'avg'];
