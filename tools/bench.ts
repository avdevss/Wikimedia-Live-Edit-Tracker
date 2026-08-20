import { existsSync, appendFileSync, writeFileSync, mkdirSync } from "node:fs";
import { dirname } from "node:path";
import { Kafka } from "kafkajs";
import Redis from "ioredis";

const KAFKA_TOPIC = "edits";
const CONSUMER_GROUP = "aggregator";
const SAMPLE_MS = 1000;

function parseArgs() {
  const args = process.argv.slice(2);
  const get = (flag: string) => {
    const idx = args.indexOf(flag);
    return idx !== -1 ? args[idx + 1] : undefined;
  };
  return {
    out: get("--out") ?? "results/bench.csv",
    duration: Number(get("--duration") ?? "60"), // seconds
    label: get("--label") ?? "unlabeled",
  };
}

const HEADER = "label,elapsed_s,eps,consumer_lag,pipeline_latency_ms_sample,rss_bytes\n";

async function main() {
  const { out, duration, label } = parseArgs();

  const kafka = new Kafka({ clientId: "bench", brokers: ["localhost:9092"] });
  const admin = kafka.admin();
  await admin.connect();
  const redis = new Redis();

  mkdirSync(dirname(out), { recursive: true });
  if (!existsSync(out)) writeFileSync(out, HEADER);

  console.log(`benchmarking for ${duration}s, label="${label}", writing to ${out}`);

  let prevHighWatermark: number | null = null;
  let prevSampleTs: number | null = null;
  const startTs = Date.now();
  let elapsed = 0;

  while (elapsed < duration) {
    const [topicOffsets, groupOffsets] = await Promise.all([
      admin.fetchTopicOffsets(KAFKA_TOPIC),
      admin.fetchOffsets({ groupId: CONSUMER_GROUP, topics: [KAFKA_TOPIC] }),
    ]);

    const committed = new Map<number, number>();
    for (const p of groupOffsets[0]?.partitions ?? []) {
      committed.set(p.partition, Number(p.offset));
    }

    let totalHigh = 0;
    let consumerLag = 0;
    for (const p of topicOffsets) {
      const high = Number(p.offset);
      totalHigh += high;
      consumerLag += Math.max(0, high - (committed.get(p.partition) ?? 0));
    }

    const now = Date.now();
    const eps =
      prevHighWatermark === null || prevSampleTs === null
        ? 0
        : (totalHigh - prevHighWatermark) / ((now - prevSampleTs) / 1000);
    prevHighWatermark = totalHigh;
    prevSampleTs = now;

    const [rawLatestIngestTs, rawRss] = await Promise.all([
      redis.get("lb:latest_ingest_ts"),
      redis.get("lb:aggregator_rss_bytes"),
    ]);
    const latencySample = rawLatestIngestTs ? now - Number(rawLatestIngestTs) : "";
    const rssBytes = rawRss ?? "";

    elapsed = Math.round((now - startTs) / 1000);
    const row = `${label},${elapsed},${eps.toFixed(1)},${consumerLag},${latencySample},${rssBytes}\n`;
    appendFileSync(out, row);
    console.log(row.trim());

    await new Promise((r) => setTimeout(r, SAMPLE_MS));
  }

  await admin.disconnect();
  redis.disconnect();
  console.log(`done: ${duration}s recorded to ${out}`);
}

main().catch((e) => {
  console.error("fatal:", e);
  process.exit(1);
});
