// S5.2 scenario 0, smoke: one VU at 2 operations per second for one minute.
// It checks that the stack answers every kind of operation in the mix and
// calibrates the starting rates of the longer scenarios; its figures are
// not a performance result.
//
//   go run ./loadtest/cmd/gendata -seed 52 -run "$RUN" -ops 200 -out "$DIR/dataset.json"
//   taskset -c "$K6_CPUS" k6 run --out json="$DIR/raw/k6-points.jsonl.gz" \
//     -e DATASET="$DIR/dataset.json" -e SUMMARY="$DIR/k6-summary.json" loadtest/k6/smoke.js
import { run, thresholds, summaryTrendStats, meta } from './lib.js';

export const options = {
  scenarios: {
    smoke: {
      executor: 'constant-arrival-rate',
      rate: 2,
      timeUnit: '1s',
      duration: '1m',
      preAllocatedVUs: 1,
      maxVUs: 1,
    },
  },
  thresholds: thresholds,
  summaryTrendStats: summaryTrendStats,
  // Keep the summary's metric keys bounded: no per-URL series.
  systemTags: ['status', 'method', 'name', 'scenario', 'expected_response', 'error_code'],
};

export default run;

export function handleSummary(data) {
  data.reliefgrid = { scenario: 'smoke', dataset: meta };
  const out = { stdout: textLine(data) + '\n' };
  if (__ENV.SUMMARY) out[__ENV.SUMMARY] = JSON.stringify(data, null, 2) + '\n';
  return out;
}

function textLine(data) {
  const m = data.metrics;
  const p = (k, s) => (m[k] && m[k].values[s] !== undefined ? m[k].values[s].toFixed(1) : '-');
  return `smoke: ${m.http_reqs ? m.http_reqs.values.count : 0} requests; ` +
    `write p99 ${p('http_req_duration{class:write}', 'p(99)')} ms, read p99 ${p('http_req_duration{class:read}', 'p(99)')} ms, ` +
    `list p99 ${p('http_req_duration{class:list}', 'p(99)')} ms; ` +
    `failed ${m.http_req_failed ? (100 * m.http_req_failed.values.rate).toFixed(2) : '-'}%`;
}
