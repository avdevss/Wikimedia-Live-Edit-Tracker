import { Kafka } from "kafkajs";
import { createWriteStream, mkdirSync } from "node:fs";
import { Histogram } from "../lib/histogram.ts";

const STREAM_URL = "https://stream.wikimedia.org/v2/stream/recentchange";
const KAFKA_TOPIC = "edits";

const kafka = new Kafka({ clientId: "producer", brokers: ["localhost:9092"] });
const producer = kafka.producer();

// Source-to-ingest lag: Wikimedia's own event timestamp (seconds) to our
// ingest_ts (ms), both known together right here. Indicative only — mixes
// in Wikimedia's own propagation delay and any clock skew on this machine,
// and their timestamp is only second-granular. Kept deliberately separate
// from pipeline latency (producer receipt to API frame emit), which is a
// different measurement recorded in the API server.
const sourceToIngestLag = new Histogram();

mkdirSync("data", { recursive: true });
const captureDate = new Date().toISOString().slice(0, 10);
const capture = createWriteStream(`data/capture-${captureDate}.jsonl`, { flags: "a" });

type EditEvent = {
  id: string;
  ts: number;
  ingest_ts: number;
  wiki: string;
  user: string;
  title: string;
  type: string;
  bot: boolean;
  delta_bytes: number;
};

function deltaBytes(len: any): number {
  if (!len || typeof len.new !== "number" || typeof len.old !== "number") return 0;
  return len.new - len.old;
}

function normalize(raw: any): EditEvent {
  return {
    id: `${raw.wiki}:${raw.id ?? "none"}`,
    ts: raw.timestamp,
    ingest_ts: Date.now(),
    wiki: raw.wiki,
    user: raw.user,
    title: raw.title,
    type: raw.type,
    bot: Boolean(raw.bot),
    delta_bytes: deltaBytes(raw.length),
  };
}

let lastEventId: string | null = null;
let seen = 0;

async function readStream(onConnected: () => void) {
  const res = await fetch(STREAM_URL, {
    headers: {
      Accept: "text/event-stream",
      "User-Agent":
        "trending-leaderboard/0.1 (https://github.com/avdevss/trending-leaderboard)",
      ...(lastEventId ? { "Last-Event-ID": lastEventId } : {}),
    },
  });

  if (!res.ok || !res.body) {
    throw new Error(`stream connect failed: ${res.status} ${res.statusText}`);
  }
  console.log(`connected${lastEventId ? " (resumed)" : ""}`);
  onConnected();

  const reader = res.body.getReader();
  const decoder = new TextDecoder();
  let buffer = "";

  while (true) {
    const { done, value } = await reader.read();
    if (done) throw new Error("stream ended");

    buffer += decoder.decode(value, { stream: true });

    let boundary: number;
    while ((boundary = buffer.indexOf("\n\n")) !== -1) {
      const block = buffer.slice(0, boundary);
      buffer = buffer.slice(boundary + 2);

      for (const line of block.split("\n")) {
        if (line.startsWith("id: ")) {
          lastEventId = line.slice(4);
        } else if (line.startsWith("data: ")) {
          const payload = line.slice(6);
          let raw: any;
          try {
            raw = JSON.parse(payload);
          } catch {
            continue;
          }
          capture.write(payload + "\n");
          const event = normalize(raw);
          sourceToIngestLag.record(event.ingest_ts - event.ts * 1000);
          await producer.send({
            topic: KAFKA_TOPIC,
            messages: [{ key: event.wiki, value: JSON.stringify(event) }],
          });
          if (++seen % 500 === 0) {
            console.log(`${seen} events produced`);
            console.log("source-to-ingest lag ms (indicative only):", sourceToIngestLag.percentiles());
          }
        }
      }
    }
  }
}

async function main() {
  await producer.connect();

  let attempt = 0;
  while (true) {
    try {
      await readStream(() => {
        attempt = 0;
      });
    } catch (err) {
      const base = Math.min(30_000, 1000 * 2 ** attempt);
      const wait = base / 2 + Math.random() * (base / 2);
      console.error(`disconnected: ${(err as Error).message} — retry in ${Math.round(wait)}ms`);
      await new Promise((r) => setTimeout(r, wait));
      attempt++;
    }
  }
}

process.on("SIGINT", async () => {
  console.log(`\nshutting down after ${seen} events`);
  await producer.disconnect();
  capture.end(() => process.exit(0));
});

main().catch((e) => {
  console.error("fatal:", e);
  process.exit(1);
});
