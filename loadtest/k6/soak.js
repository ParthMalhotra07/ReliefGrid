// S5.2 scenario 3, soak: a constant rate (about 70% of the saturation point) for DURATION_S: log growth, snapshots and their cost under the load profile (#33).
//
//   RATE=<req/s> DURATION_S=1800 MAX_VUS=400
import { run, thresholds, summaryTrendStats, meta } from './lib.js';

const n = (k, d) => (__ENV[k] ? Number(__ENV[k]) : d);
if (!__ENV.RATE) throw new Error('RATE is required (req/s; from the step run)');
const rate = n('RATE', 0), duration = n('DURATION_S', 1800);

export const options = {
  scenarios: {
    soak: {
      executor: 'constant-arrival-rate',
      rate: rate,
      timeUnit: '1s',
      duration: `${duration}s`,
      preAllocatedVUs: n('PRE_VUS', 50),
      maxVUs: n('MAX_VUS', 400),
    },
  },
  thresholds: thresholds,
  summaryTrendStats: summaryTrendStats,
  systemTags: ['status', 'method', 'name', 'scenario', 'expected_response', 'error_code'],
};

export default run;

export function handleSummary(data) {
  data.reliefgrid = { scenario: 'soak', dataset: meta, rate, duration };
  const out = { stdout: `soak: ${data.metrics.http_reqs ? data.metrics.http_reqs.values.count : 0} requests at ${rate}/s\n` };
  if (__ENV.SUMMARY) out[__ENV.SUMMARY] = JSON.stringify(data, null, 2) + '\n';
  return out;
}
