// A simple latency histogram: a bounded ring buffer of raw samples, with
// exact percentiles computed by sorting on read. Called "HDR-style" in the
// plan, but this is deliberately simpler than a true HDR histogram (which
// buckets values on a log scale to bound memory at any sample count) — at
// this project's scale (tens of thousands of samples, not millions/sec), a
// bounded buffer of raw values is simpler, exact, and cheap enough.
const MAX_SAMPLES = 10_000;

export type Percentiles = { p50: number; p95: number; p99: number; p999: number };

export class Histogram {
  private samples: number[] = [];
  private nextIndex = 0;

  record(value: number): void {
    if (this.samples.length < MAX_SAMPLES) {
      this.samples.push(value);
    } else {
      this.samples[this.nextIndex] = value;
      this.nextIndex = (this.nextIndex + 1) % MAX_SAMPLES;
    }
  }

  count(): number {
    return this.samples.length;
  }

  percentiles(): Percentiles {
    if (this.samples.length === 0) return { p50: 0, p95: 0, p99: 0, p999: 0 };
    const sorted = [...this.samples].sort((a, b) => a - b);
    const at = (p: number) => sorted[Math.min(sorted.length - 1, Math.floor(p * sorted.length))];
    return { p50: at(0.5), p95: at(0.95), p99: at(0.99), p999: at(0.999) };
  }
}
