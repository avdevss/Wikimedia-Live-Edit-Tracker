import { Kafka } from "kafkajs";
import { createWindow, ingest, topK, type SlidingWindow } from "./window.ts";

const KAFKA_TOPIC = "edits";

const kafka = new Kafka({ clientId: "aggregator", brokers: ["localhost:9092"] });
const consumer = kafka.consumer({ groupId: "aggregator" });

const dims: Record<string, SlidingWindow> = {
  editors: createWindow(),
  pages: createWindow(),
  wikis: createWindow(),
  humans_vs_bots: createWindow(),
};

function memberFor(dim: string, event: any): string {
  switch (dim) {
    case "editors":
      return event.user;
    case "pages":
      return `${event.wiki}:${event.title}`;
    case "wikis":
      return event.wiki;
    case "humans_vs_bots":
      return event.bot ? "bot" : "human";
    default:
      throw new Error(`unknown dimension: ${dim}`);
  }
}

async function main() {
  await consumer.connect();
  await consumer.subscribe({ topic: KAFKA_TOPIC, fromBeginning: false });

  await consumer.run({
    eachMessage: async ({ message }) => {
      if (!message.value) return;
      const event = JSON.parse(message.value.toString());
      const bucketTs = Math.floor(Date.now() / 1000);

      for (const dim of Object.keys(dims)) {
        ingest(dims[dim], memberFor(dim, event), bucketTs);
      }
    },
  });
}

setInterval(() => {
  for (const dim of Object.keys(dims)) {
    console.log(`--- ${dim} ---`, topK(dims[dim], 10));
  }
}, 1000);

main().catch((e) => {
  console.error("fatal:", e);
  process.exit(1);
});
