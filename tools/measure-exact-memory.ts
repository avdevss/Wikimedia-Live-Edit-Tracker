import { createReadStream } from "node:fs";
import { createInterface } from "node:readline";

// Deliberately a standalone script, run as its own fresh process per
// sample size (see run-exact-memory.sh), rather than a loop inside a
// longer-running script. Measuring heapUsed deltas across multiple
// iterations in one process is unreliable — prior allocations, V8 heap
// growth, and GC timing all leak into later measurements. One clean
// process per measurement avoids that entirely.
function parseArgs() {
  const args = process.argv.slice(2);
  const get = (flag: string) => {
    const idx = args.indexOf(flag);
    return idx !== -1 ? args[idx + 1] : undefined;
  };
  return {
    file: get("--file") ?? "data/capture-2026-08-19.jsonl",
    sampleSize: Number(get("--sample-size") ?? "525000"),
  };
}

async function loadSample(file: string, sampleSize: number): Promise<string[]> {
  const rl = createInterface({ input: createReadStream(file) });
  const members: string[] = [];
  for await (const line of rl) {
    if (members.length >= sampleSize) break;
    if (!line.trim()) continue;
    let raw: any;
    try {
      raw = JSON.parse(line);
    } catch {
      continue;
    }
    if (!raw.user) continue;
    members.push(raw.user);
  }
  return members;
}

async function main() {
  const { file, sampleSize } = parseArgs();
  const gc = (global as any).gc;
  if (typeof gc !== "function") {
    throw new Error("run with NODE_OPTIONS=--expose-gc for a clean measurement");
  }

  const members = await loadSample(file, sampleSize);

  gc();
  const before = process.memoryUsage().heapUsed;

  const exact = new Map<string, number>();
  for (const m of members) exact.set(m, (exact.get(m) ?? 0) + 1);

  gc();
  const after = process.memoryUsage().heapUsed;

  const bytes = Math.max(0, after - before);
  console.log(
    JSON.stringify({
      sample_events: members.length,
      cardinality: exact.size,
      exact_map_bytes: bytes,
      bytes_per_distinct_key: +(bytes / exact.size).toFixed(1),
    }),
  );
}

main().catch((e) => {
  console.error("fatal:", e);
  process.exit(1);
});
