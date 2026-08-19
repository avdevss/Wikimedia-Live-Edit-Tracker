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

async function main() {
  const { file, rate } = parseArgs();

  const kafka = new Kafka({ clientId: "replay", brokers: ["localhost:9092"] });
  const producer = kafka.producer();
  await producer.connect();

  const rl = createInterface({ input: createReadStream(file) });

  let prevOriginalTs: number | null = null;
  let sent = 0;

  process.on("SIGINT", async () => {
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
      const gap = Math.max(0, (originalTs - prevOriginalTs) / rate);
      if (gap > 0) await sleep(gap);
    }
    prevOriginalTs = originalTs;

    // ingest_ts is stamped fresh here, at actual replay time — not carried
    // over from the original capture — so the aggregator's sliding window
    // buckets replayed events by real elapsed wall-clock time during this
    // run, which is what makes the amplified load test meaningful.
    const event = normalize(raw);
    await producer.send({
      topic: KAFKA_TOPIC,
      messages: [{ key: event.wiki, value: JSON.stringify(event) }],
    });

    if (++sent % 500 === 0) console.log(`${sent} events replayed at ${rate}x`);
  }

  console.log(`done: ${sent} events replayed at ${rate}x`);
  await producer.disconnect();
}

main().catch((e) => {
  console.error("fatal:", e);
  process.exit(1);
});
