// Waiting plus retrying requests over the last 15 minutes, sampled from the
// reads the dispatch view already makes: no extra endpoint or polling.
export const TREND_MS = 15 * 60000;
export const TREND_LIMIT = 90;
// A read within this long of the previous sample replaces it, so 15 minutes
// hold at most about 90 samples.
const MERGE_MS = 10000;

export class QueueTrend {
  constructor() {
    this.samples = [];
  }
  add(at, count) {
    const last = this.samples.at(-1);
    const kept =
      last && at - last.at < MERGE_MS
        ? this.samples.slice(0, -1)
        : this.samples;
    this.samples = [...kept, { at, count }]
      .filter((v) => at - v.at <= TREND_MS)
      .slice(-TREND_LIMIT);
    return this.samples;
  }
  summary(now) {
    const recent = this.samples.filter((v) => now - v.at <= TREND_MS);
    if (!recent.length) return "Queue: collecting samples";
    const peak = Math.max(...recent.map((v) => v.count));
    return `Queue: ${recent.at(-1).count} now, peak ${peak} in last 15 min`;
  }
}

const round = (value) => Math.round(value * 10) / 10;
// SVG polyline points for samples across the window ending at `now`, scaled
// to the window's peak; one sample draws a flat line.
export function sparkline(samples, now, width, height, pad = 2) {
  const recent = samples.filter((v) => now - v.at <= TREND_MS);
  if (!recent.length) return "";
  const peak = Math.max(...recent.map((v) => v.count));
  const y = (count) =>
    round(pad + (peak ? 1 - count / peak : 1) * (height - 2 * pad));
  if (recent.length === 1) {
    const level = y(recent[0].count);
    return `0,${level} ${width},${level}`;
  }
  const start = now - TREND_MS;
  return recent
    .map((v) => `${round(((v.at - start) / TREND_MS) * width)},${y(v.count)}`)
    .join(" ");
}
