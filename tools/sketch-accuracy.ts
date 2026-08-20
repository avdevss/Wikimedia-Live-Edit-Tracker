import { createReadStream, mkdirSync, writeFileSync } from "node:fs";
import { createInterface } from "node:readline";
import { CountMinSketch, SpaceSaving } from "../src/aggregator/sketch.ts";

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

type Budget = { label: string; cmsWidth: number; cmsDepth: number; ssCapacity: number };

const BUDGETS: Budget[] = [
  { label: "small", cmsWidth: 256, cmsDepth: 4, ssCapacity: 50 },
  { label: "medium", cmsWidth: 1024, cmsDepth: 4, ssCapacity: 200 },
  { label: "large", cmsWidth: 4096, cmsDepth: 4, ssCapacity: 800 },
];

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

type Metrics = {
  structure: string;
  bytes: number;
  top20Overlap: number;
  meanAbsRankError: number;
  meanRelativeCountError: number;
};

// exactTop20: the ground-truth top 20, already sorted, index = trueRank-1.
// estimateFor/rankFor let CMS and Space-Saving plug in their own notion of
// "what does this structure think this key's count/rank is," including the
// case where a key isn't tracked at all (Space-Saving) or ranked against
// the full known key universe (CMS, which has no key enumeration of its
// own — it only answers "what's the count for a key I already have").
function evaluate(
  structure: string,
  bytes: number,
  exactTop20: Array<[string, number]>,
  estimateFor: (key: string) => number,
  rankFor: (key: string) => number | null,
  trackedUniverseSize: number,
): Metrics {
  let overlap = 0;
  let rankErrSum = 0;
  let countErrSum = 0;

  exactTop20.forEach(([key, trueCount], i) => {
    const trueRank = i + 1;
    const rank = rankFor(key);
    if (rank !== null && rank <= 20) overlap++;

    const observedRank = rank ?? trackedUniverseSize + 1;
    rankErrSum += Math.abs(observedRank - trueRank);

    const est = estimateFor(key);
    countErrSum += trueCount === 0 ? 0 : Math.abs(est - trueCount) / trueCount;
  });

  return {
    structure,
    bytes,
    top20Overlap: overlap,
    meanAbsRankError: rankErrSum / exactTop20.length,
    meanRelativeCountError: countErrSum / exactTop20.length,
  };
}

async function main() {
  const { file, sampleSize } = parseArgs();
  console.log(`loading up to ${sampleSize} events from ${file}...`);
  const members = await loadSample(file, sampleSize);
  console.log(`loaded ${members.length} events`);

  const exact = new Map<string, number>();
  for (const m of members) exact.set(m, (exact.get(m) ?? 0) + 1);
  const cardinality = exact.size;
  const exactTop20 = [...exact.entries()].sort((a, b) => b[1] - a[1]).slice(0, 20);

  console.log(`distinct editors (cardinality): ${cardinality}`);
  console.log("exact top 5:", exactTop20.slice(0, 5));

  const rows: string[] = [
    "dimension,sample_events,cardinality,budget,structure,bytes,top20_overlap,mean_abs_rank_error,mean_relative_count_error",
  ];

  for (const budget of BUDGETS) {
    const cms = new CountMinSketch(budget.cmsWidth, budget.cmsDepth);
    for (const m of members) cms.increment(m);

    const cmsRanking = [...exact.keys()]
      .map((k) => [k, cms.estimate(k)] as [string, number])
      .sort((a, b) => b[1] - a[1]);
    const cmsRankOf = new Map(cmsRanking.map(([k], i) => [k, i + 1]));

    const cmsResult = evaluate(
      "count_min_sketch",
      cms.sizeBytes(),
      exactTop20,
      (k) => cms.estimate(k),
      (k) => cmsRankOf.get(k) ?? null,
      cmsRanking.length,
    );
    console.log(`[${budget.label}] CMS:`, cmsResult);
    rows.push(
      [
        "editors",
        members.length,
        cardinality,
        budget.label,
        cmsResult.structure,
        cmsResult.bytes,
        cmsResult.top20Overlap,
        cmsResult.meanAbsRankError.toFixed(3),
        cmsResult.meanRelativeCountError.toFixed(4),
      ].join(","),
    );

    const ss = new SpaceSaving(budget.ssCapacity);
    for (const m of members) ss.increment(m);

    const ssRanking = ss.topK(budget.ssCapacity);
    const ssRankOf = new Map(ssRanking.map(([k], i) => [k, i + 1]));
    const ssEstimateOf = new Map(ssRanking);

    const ssResult = evaluate(
      "space_saving",
      ss.sizeBytes(),
      exactTop20,
      (k) => ssEstimateOf.get(k) ?? 0,
      (k) => ssRankOf.get(k) ?? null,
      ssRanking.length,
    );
    console.log(`[${budget.label}] Space-Saving:`, ssResult);
    rows.push(
      [
        "editors",
        members.length,
        cardinality,
        budget.label,
        ssResult.structure,
        ssResult.bytes,
        ssResult.top20Overlap,
        ssResult.meanAbsRankError.toFixed(3),
        ssResult.meanRelativeCountError.toFixed(4),
      ].join(","),
    );
  }

  mkdirSync("results", { recursive: true });
  writeFileSync("results/sketch-accuracy.csv", rows.join("\n") + "\n");
  console.log("wrote results/sketch-accuracy.csv");
}

main().catch((e) => {
  console.error("fatal:", e);
  process.exit(1);
});
