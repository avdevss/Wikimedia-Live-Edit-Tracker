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
