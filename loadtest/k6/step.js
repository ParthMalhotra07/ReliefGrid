// S5.2 scenario 2, step-to-saturation: the arrival rate climbs in steps,
// each a RAMP_S ramp then a HOLD_S hold, from START_RPS by STEP_RPS for STEPS
// steps (open model: a slow server shows as latency and dropped iterations,
// never as less offered load). loadtest/run.sh measures each hold as one
// step; the saturation point is the last step inside every threshold.
//
//   START_RPS=10 STEP_RPS=10 STEPS=8 RAMP_S=10 HOLD_S=110 MAX_VUS=400
import { run, thresholds, summaryTrendStats, meta } from './lib.js';

const n = (k, d) => (__ENV[k] ? Number(__ENV[k]) : d);
const start = n('START_RPS', 10), step = n('STEP_RPS', 10), steps = n('STEPS', 8);
const ramp = n('RAMP_S', 10), hold = n('HOLD_S', 110);
const stages = [];
for (let i = 0; i < steps; i++) {
  const target = start + i * step;
  stages.push({ target: target, duration: `${ramp}s` }, { target: target, duration: `${hold}s` });
}

export const options = {
  scenarios: {
    step: {
      executor: 'ramping-arrival-rate',
      startRate: start,
      timeUnit: '1s',
      stages: stages,
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
  data.reliefgrid = { scenario: 'step', dataset: meta, start, step, steps, ramp, hold };
  const out = { stdout: `step: ${data.metrics.http_reqs ? data.metrics.http_reqs.values.count : 0} requests\n` };
  if (__ENV.SUMMARY) out[__ENV.SUMMARY] = JSON.stringify(data, null, 2) + '\n';
  return out;
}
