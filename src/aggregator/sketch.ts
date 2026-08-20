// FNV-1a, seeded differently per row to approximate `depth` independent
// hash functions without pulling in a hashing library — CMS doesn't need
// cryptographic properties, just reasonable distribution across cells.
function hash(str: string, seed: number): number {
  let h = seed >>> 0;
  for (let i = 0; i < str.length; i++) {
    h ^= str.charCodeAt(i);
    h = Math.imul(h, 16777619);
  }
  return h >>> 0;
}

export class CountMinSketch {
  private width: number;
  private depth: number;
  private table: Uint32Array;
  private seeds: number[];

  constructor(width: number, depth: number) {
    this.width = width;
    this.depth = depth;
    this.table = new Uint32Array(width * depth);
    this.seeds = Array.from({ length: depth }, (_, i) => 0x9e3779b1 * (i + 1));
  }

  private indexFor(row: number, key: string): number {
    return row * this.width + (hash(key, this.seeds[row]) % this.width);
  }

  increment(key: string): void {
    for (let row = 0; row < this.depth; row++) {
      this.table[this.indexFor(row, key)]++;
    }
  }

  // Always >= the true count — collisions only inflate a cell, never
  // deflate it, so the minimum across rows is the tightest available bound.
  estimate(key: string): number {
    let min = Infinity;
    for (let row = 0; row < this.depth; row++) {
      min = Math.min(min, this.table[this.indexFor(row, key)]);
    }
    return min;
  }

  sizeBytes(): number {
    return this.table.byteLength;
  }
}

type SpaceSavingEntry = { count: number; error: number };

// Unlike CMS (pure frequency estimation for a key you already have), this
// directly tracks a bounded set of the current heaviest keys — natively
// gives you a top-K without needing a separate candidate list. When a new
// key arrives and the table is full, it evicts the currently-lightest
// tracked key and takes over its slot, inheriting its count (so the new
// key's count can never fall below what was already there) plus an error
// bound recording how much that inherited count could be an overestimate.
//
// Retention is only guaranteed for a key whose true count exceeds
// totalIncrements / (capacity + 1) — below that threshold, whether a key
// survives depends on stream order, not just its frequency. A stream that
// processes one key's occurrences in a contiguous block before moving to
// the next is an adversarial worst case for this; a naturally interleaved
// stream (many distinct keys interspersed, e.g. real edits arriving from
// many editors) is what the guarantee is designed around and behaves well.
export class SpaceSaving {
  private capacity: number;
  private items = new Map<string, SpaceSavingEntry>();

  constructor(capacity: number) {
    this.capacity = capacity;
  }

  increment(key: string): void {
    const existing = this.items.get(key);
    if (existing) {
      existing.count++;
      return;
    }

    if (this.items.size < this.capacity) {
      this.items.set(key, { count: 1, error: 0 });
      return;
    }

    let minKey: string | null = null;
    let minEntry: SpaceSavingEntry | null = null;
    for (const [k, e] of this.items) {
      if (minEntry === null || e.count < minEntry.count) {
        minKey = k;
        minEntry = e;
      }
    }
    if (minKey !== null && minEntry !== null) {
      this.items.delete(minKey);
      this.items.set(key, { count: minEntry.count + 1, error: minEntry.count });
    }
  }

  topK(k: number): Array<[string, number]> {
    return [...this.items.entries()]
      .sort((a, b) => b[1].count - a[1].count)
      .slice(0, k)
      .map(([key, e]) => [key, e.count]);
  }

  sizeBytes(): number {
    let total = 0;
    for (const key of this.items.keys()) {
      total += key.length * 2 + 8; // UTF-16 chars + count/error as two uint32s
    }
    return total;
  }
}
