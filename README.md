# Real-Time Trending Leaderboard

A live leaderboard over the public Wikimedia edit firehose — trending editors, pages, wikis, and humans-vs-bots, updated over a 5-minute sliding window.

> **Status: day 1 skeleton.** The pipeline (producer → Redpanda → aggregator → Redis → WebSocket API → UI) is built, running, and verified against live data. The three headline numbers below, their plots, and the sketch-accuracy comparison are day 2 work and are not filled in yet — see the "Pending" notes in each section rather than treating this as a finished deliverable.

## Quickstart

```bash
docker compose up -d
docker compose exec redpanda rpk topic create edits -p 4 -r 1 -c retention.ms=3600000
npm install
npm run producer     # in its own terminal
npm run aggregator    # in its own terminal
npm run api           # in its own terminal
open src/web/index.html
```

Verified from a clean clone: `docker compose up` brings up both services with no manual fixes (tested by cloning into a fresh directory and confirming Redpanda cluster info and a Redis `PING` both succeed).

## Architecture

```
   Wikimedia EventStreams (public SSE, ~30-45 events/sec, varies by time of day)
                    |
                    | one long-lived HTTPS GET
                    v
        +-------------------------+
        |  producer (Node)        |   normalize, stamp ingest_ts
        |  - SSE reader           |   tee raw events to capture file
        |  - backoff reconnect    |
        +-----------+-------------+
                    | kafkajs produce, key = entity
                    v
        +-------------------------+
        |  Redpanda (1 container) |   topic: edits, 4 partitions
        |  Kafka protocol         |   retention: 1 hour
        +-----------+-------------+
                    | consumer group: aggregator
                    v
        +-------------------------------------------+
        |  aggregator (Node)                        |
        |  - 300 x 1s bucket ring per dimension     |
        |  - exact counters + optional CMS          |
        |  - seals buckets, writes snapshots        |
        +-----------+-------------------+-----------+
                    | ZSET writes, once per second
                    v
        +-------------------------+
        |  Redis                  |
        |  lb:<dim>:win  ZSET     |
        +-----------+-------------+
                    | read side, polled at 4 Hz
                    v
        +-------------------------+
        |  api (Node, ws)         |  snapshot on connect
        |  - 250ms tick coalesce  |  deltas thereafter
        |  - latency histogram    |  measured here, server side
        +-----------+-------------+
                    | WebSocket
                    v
        +-------------------------+
        |  browser UI             |  4 panels, plain text rows,
        |  static HTML + JS       |  eps + latency badges
        +-------------------------+

   Offline tooling (day 2):
     replay.ts    reads capture file, produces at Nx for load tests
     bench.ts     collects latency and lag into CSV for the results tables
```

## The three headline numbers

**Pending — day 2.** Method for each, once measured properly via `tools/bench.ts` and recorded in `results/results.md`:

- **p99 producer-to-frame-emit latency, with histogram plot.** Already measured live (`src/api/index.ts` records a real per-broadcast latency histogram, console-dumped once/sec), but not yet captured into `results/` or plotted. Ad-hoc runs today showed p50 in the low hundreds of ms and p99 around 1-1.5s under live (unamplified) load — not a formal benchmark, so not quoted here as the final number.
- **Sustained throughput at the highest multiplier held, plus the lag curve.** `tools/replay.ts` is built and verified up to 100x against the real capture file — the aggregator showed no measurable lag at any tested multiplier (10x/50x/100x), meaning the current bottleneck is the replay tool's own unbatched, sequential Kafka produce loop (~200-430 events/sec), not the aggregator. Day 2's batching optimization (section 6.3) targets exactly this.
- **Sketch memory reduction and top-20 accuracy, with distinct cardinality per window.** Not started — Count-Min Sketch and Space-Saving are day 2 work.

## Design decisions

**Bucket-ring subtraction instead of recomputing the window.** Each dimension keeps a ring of 300 one-second buckets. Expiring a bucket costs work proportional to what's *in* that bucket, not the whole window — recomputing the full 5-minute total on every tick would get more expensive as the window fills, for no reason.

**Tick coalescing instead of a push per event.** At live rates a per-event WebSocket push would send ~40+ frames/sec to every client, far faster than a browser can usefully render. The API server instead polls Redis at 4 Hz and only sends what actually changed — cuts frame volume by roughly 10x with no perceptible difference to a viewer.

**Redis persistence is off.** Redis here is a read-side cache and checkpoint store, not the source of truth — that's the Kafka log. If Redis is lost, the aggregator's warm start (below) rebuilds it from Kafka in a few seconds.

**Warm start replays the last 5 minutes on boot.** On startup, the aggregator seeks every partition to "now minus 300 seconds" and replays forward, refilling the window in a few seconds instead of either waiting 5 minutes of live traffic or resuming from a possibly-stale committed offset. Verified two ways: the immediate refill (a few seconds to a full 5-minute window after restart), and — the test that actually matters — polling the window total every 30s for 7 minutes after a restart and confirming no cliff around the 5-minute mark, which is where a version that bucketed by wall-clock read time instead of `ingest_ts` would show a sudden mass-expiry as the wrongly-crowded catch-up buckets all fell out of the window at once. The total moved smoothly through that window (11172 → 11004 → 10846 at the 270s/300s/330s marks) with no discontinuity anywhere across the full run. Warm start also turned up a real bug along the way — a missing graceful-shutdown handler was adding 20+ seconds of Kafka consumer-group rebalance delay to every restart — which is now fixed and confirmed via debug-level Kafka logs showing rejoin time drop from ~23s to single-digit milliseconds.

**Bucketing by `ingest_ts`, not by when the aggregator happens to read a message.** Events are bucketed by the timestamp the producer stamped at receipt, not by wall-clock time at consumption. Those two differ during backlog catch-up or fast replay, and bucketing by consumption time would cram minutes of events into one or two buckets — correct-looking until they all expire simultaneously 300 seconds later.

## Limitations

- **Single node, at-least-once delivery.** A clean restart cannot double-count (warm start replays into freshly-cleared buckets), but a mid-run consumer-group rebalance could double-count events within a single bucket. This is bounded and rare on a single-consumer setup, and deliberately not engineered around — see the plan's own scope decision on crash-recovery proving.
- **No crash-recovery verification.** Cut from scope on purpose (see `realtime-leaderboard-plan.md` section 1) — this is a 2-day project, not a durability audit.
- **5-minute window only.** Nothing beyond the trailing 300 seconds is tracked or queryable.
- **Two fixes are correct by code reasoning, not yet exercised by a real failure.** The empty-dimension Redis cleanup and the per-command Redis transaction error surfacing have never actually been triggered in testing (no dimension has gone empty; no Redis command has actually failed). Noted here rather than silently assumed correct. (The `ingest_ts`-based warm-start bucketing was in this category too, until a 7-minute post-restart check specifically ruled out the delayed-cliff failure mode — see Design decisions.)
- **`replay.ts` currently caps around 200-430 events/sec** regardless of requested multiplier, due to its unbatched, sequential produce loop — a known, measured characteristic, not a bug, and exactly what day 2's batching optimization is meant to address.
