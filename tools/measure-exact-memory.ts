import { createReadStream } from "node:fs";
import { createInterface } from "node:readline";

// Deliberately a standalone script, run as its own fresh process per
// sample size, rather than a loop inside a longer-running script.
// Measuring heapUsed deltas across multiple iterations in one process is
// unreliable — prior allocations, V8 heap growth, and GC timing all leak
// into later measurements. One clean process per measurement avoids that.
//
// Also deliberately does NOT pre-load all sample strings into an array
// before measuring: Map.set(key, ...) stores a reference, not a copy, so
// if every key string already exists (held alive by a separate array)
// before the "before" snapshot, the measured delta only ever captures the
// Map's own bookkeeping overhead — never the actual string content. An
// earlier version of this script had exactly that bug and reported a
// physically-implausible ~30 bytes/entry. Streaming the file directly
// into the Map, one line at a time, means each key string's allocation
// genuinely happens inside the measured before/after window, and only
// what the Map actually retains survives the closing gc().
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

async function main() {
  const { file, sampleSize } = parseArgs();
  const gc = (global as any).gc;
  if (typeof gc !== "function") {
    throw new Error("run with NODE_OPTIONS=--expose-gc for a clean measurement");
  }

  gc();
  const before = process.memoryUsage().heapUsed;

  const exact = new Map<string, number>();
  let sampleEvents = 0;

  const rl = createInterface({ input: createReadStream(file) });
  for await (const line of rl) {
    if (sampleEvents >= sampleSize) break;
    if (!line.trim()) continue;
    let raw: any;
    try {
      raw = JSON.parse(line);
    } catch {
      continue;
    }
    if (!raw.user) continue;
    exact.set(raw.user, (exact.get(raw.user) ?? 0) + 1);
    sampleEvents++;
    // raw (and everything in it except the .user string we extracted) is
    // now unreferenced and eligible for GC — nothing here holds the full
    // parsed object or a separate array of every event's username alive.
  }

  gc();
  const after = process.memoryUsage().heapUsed;

  const bytes = Math.max(0, after - before);
  console.log(
    JSON.stringify({
      sample_events: sampleEvents,
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
