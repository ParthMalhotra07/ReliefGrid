// S5.2 scenario 1, baseline: a constant rate (about half the saturation point) for DURATION_S, the first WARMUP_S not measured: the reference every before/after compares.
//
//   RATE=<req/s> DURATION_S=600 MAX_VUS=400
import { run, thresholds, summaryTrendStats, meta } from './lib.js';

const n = (k, d) => (__ENV[k] ? Number(__ENV[k]) : d);
if (!__ENV.RATE) throw new Error('RATE is required (req/s; from the step run)');
const rate = n('RATE', 0), duration = n('DURATION_S', 600);

export const options = {
  scenarios: {
    baseline: {
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
  data.reliefgrid = { scenario: 'baseline', dataset: meta, rate, duration };
  const out = { stdout: `baseline: ${data.metrics.http_reqs ? data.metrics.http_reqs.values.count : 0} requests at ${rate}/s\n` };
  if (__ENV.SUMMARY) out[__ENV.SUMMARY] = JSON.stringify(data, null, 2) + '\n';
  return out;
}
