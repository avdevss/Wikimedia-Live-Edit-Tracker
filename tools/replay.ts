import { createReadStream } from "node:fs";
import { createInterface } from "node:readline";
import { Kafka } from "kafkajs";
import { normalize } from "../src/lib/normalize.ts";

const KAFKA_TOPIC = "edits";

function parseArgs() {
  const args = process.argv.slice(2);
  const get = (flag: string) => {
    const idx = args.indexOf(flag);
    return idx !== -1 ? args[idx + 1] : undefined;
  };
  const file = get("--file");
  const rate = Number(get("--rate") ?? "1");
  if (!file) throw new Error("usage: replay.ts --file <path> --rate <N>");
  return { file, rate };
}

function originalTimestampMs(raw: any): number {
  const fromMeta = Date.parse(raw?.meta?.dt ?? "");
  if (!Number.isNaN(fromMeta)) return fromMeta;
  return raw.timestamp * 1000; // fallback: only second-granular
}

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));
const BATCH_LIMIT = 500; // safety cap so a long gap-free run doesn't buffer unboundedly

// The capture file has real multi-minute gaps in it from our own testing
// history (every time the producer was stopped and restarted, e.g. for the
// drain test or warm-start checks), not just genuine Wikipedia quiet
// periods. Faithfully honoring a gap that large, even compressed by a
// modest rate, can stall a load test for longer than the whole test
// window. Capping the max single-event sleep trades perfect timing
// fidelity for a load test that actually delivers consistent amplified
// throughput, which is what section 6.3 needs.
const MAX_GAP_MS = 2000;

async function main() {
  const { file, rate } = parseArgs();

  const kafka = new Kafka({ clientId: "replay", brokers: ["localhost:9092"] });
  const producer = kafka.producer();
  await producer.connect();

  const rl = createInterface({ input: createReadStream(file) });

  let prevOriginalTs: number | null = null;
  let sent = 0;

  // Accumulates events with (compressed) zero gap between them — a batch
  // of one send() instead of one round trip per event — and flushes
  // whenever the next event actually needs a real sleep, or the buffer
  // hits BATCH_LIMIT. This is exactly the back-to-back-burst case that was
  // the measured bottleneck: at high multipliers most inter-event gaps
  // compress to ~0ms, so the old per-event await was paying a full Kafka
  // round trip for events that had no real pacing reason to be separate.
  let batch: Array<{ key: string; value: string }> = [];

  async function flush() {
    if (batch.length === 0) return;
    const toSend = batch;
    batch = [];
    await producer.send({ topic: KAFKA_TOPIC, messages: toSend });
  }

  process.on("SIGINT", async () => {
    await flush();
    console.log(`\nstopped after ${sent} events replayed at ${rate}x`);
    await producer.disconnect();
    process.exit(0);
  });

  for await (const line of rl) {
    if (!line.trim()) continue;
    let raw: any;
    try {
      raw = JSON.parse(line);
    } catch {
      continue;
    }

    const originalTs = originalTimestampMs(raw);
    if (prevOriginalTs !== null) {
      const gap = Math.min(MAX_GAP_MS, Math.max(0, (originalTs - prevOriginalTs) / rate));
      if (gap > 0) {
        await flush();
        await sleep(gap);
      }
    }
    prevOriginalTs = originalTs;

    // ingest_ts is stamped fresh here, at actual replay time — not carried
    // over from the original capture — so the aggregator's sliding window
    // buckets replayed events by real elapsed wall-clock time during this
    // run, which is what makes the amplified load test meaningful.
    const event = normalize(raw);
    batch.push({ key: event.wiki, value: JSON.stringify(event) });
    if (batch.length >= BATCH_LIMIT) await flush();

    if (++sent % 500 === 0) console.log(`${sent} events replayed at ${rate}x`);
  }

  await flush();
  console.log(`done: ${sent} events replayed at ${rate}x`);
  await producer.disconnect();
}

main().catch((e) => {
  console.error("fatal:", e);
  process.exit(1);
});
