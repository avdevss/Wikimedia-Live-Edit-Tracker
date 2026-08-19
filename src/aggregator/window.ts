export const WINDOW_SECONDS = 300;

export type SlidingWindow = {
  buckets: Array<Map<string, number>>;
  totals: Map<string, number>;
  lastBucketTs: number;
};

export function createWindow(): SlidingWindow {
  return {
    buckets: Array.from({ length: WINDOW_SECONDS }, () => new Map<string, number>()),
    totals: new Map(),
    lastBucketTs: -1,
  };
}

function expireBucket(w: SlidingWindow, idx: number) {
  for (const [member, count] of w.buckets[idx]) {
    const left = (w.totals.get(member) ?? 0) - count;
    if (left > 0) w.totals.set(member, left);
    else w.totals.delete(member);
  }
  w.buckets[idx].clear();
}

// Walk forward one second at a time, expiring whichever bucket just fell
// out of the 300s window at each step. Capped at WINDOW_SECONDS steps:
// after a gap that long, every bucket is stale, so there is no point
// walking further than that. Never moves lastBucketTs backward, so it is
// safe to call this on its own, independent of any particular event's
// timestamp, purely to age the window forward on the wall clock.
export function advance(w: SlidingWindow, bucketTs: number) {
  if (w.lastBucketTs < 0) {
    w.lastBucketTs = bucketTs;
    return;
  }
  if (bucketTs <= w.lastBucketTs) return;

  const from = Math.max(w.lastBucketTs + 1, bucketTs - WINDOW_SECONDS + 1);
  for (let ts = from; ts <= bucketTs; ts++) {
    expireBucket(w, ts % WINDOW_SECONDS);
  }
  w.lastBucketTs = bucketTs;
}

export function ingest(w: SlidingWindow, member: string, bucketTs: number) {
  advance(w, bucketTs);

  if (bucketTs <= w.lastBucketTs - WINDOW_SECONDS) {
    // Arrived out of order and its bucket slot has already been recycled
    // for a newer second. Counting it now would corrupt that slot, so drop it.
    return;
  }

  const idx = bucketTs % WINDOW_SECONDS;
  const bucket = w.buckets[idx];
  bucket.set(member, (bucket.get(member) ?? 0) + 1);
  w.totals.set(member, (w.totals.get(member) ?? 0) + 1);
}

export function topK(w: SlidingWindow, k: number): Array<[string, number]> {
  return [...w.totals.entries()].sort((a, b) => b[1] - a[1]).slice(0, k);
}
