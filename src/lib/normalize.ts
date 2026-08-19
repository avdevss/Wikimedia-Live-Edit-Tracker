export type EditEvent = {
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

// Shared by the producer (live SSE receipt) and replay.ts (reading the
// capture file back at Nx speed) so both stamp ingest_ts and compute
// delta_bytes identically. ingest_ts is always "now" — the moment this
// event actually enters our system through whichever path — not the
// original historical receipt time, which matters for replay: it lets the
// aggregator's sliding window bucket replayed events by real elapsed wall
// clock time, not by their original capture-day timestamps.
export function normalize(raw: any): EditEvent {
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
