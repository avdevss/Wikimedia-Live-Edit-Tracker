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
    baselineRate: Number(get("--baseline-rate") ?? "35"), // events/sec, real measured live rate
  };
}

const WINDOW_SECONDS = 300;
const MULTIPLIERS = [25, 50, 75];

type Budget = { label: string; cmsWidth: number; cmsDepth: number; ssCapacity: number };

const BUDGETS: Budget[] = [
  { label: "xsmall", cmsWidth: 256, cmsDepth: 4, ssCapacity: 50 },
  { label: "small", cmsWidth: 512, cmsDepth: 4, ssCapacity: 100 },
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

function runBudget(
  budget: Budget,
  members: string[],
  exact: Map<string, number>,
  exactTop20: Array<[string, number]>,
): Metrics[] {
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

  return [cmsResult, ssResult];
}

async function main() {
  const { file, baselineRate } = parseArgs();

  const sampleSizeFor = (mult: number) => Math.round(baselineRate * mult * WINDOW_SECONDS);
  const largestMultiplier = Math.max(...MULTIPLIERS);
  const maxSampleSize = sampleSizeFor(largestMultiplier);

  console.log(`loading up to ${maxSampleSize} events from ${file} (covers all multipliers up to ${largestMultiplier}x)...`);
  const allMembers = await loadSample(file, maxSampleSize);
  console.log(`loaded ${allMembers.length} events`);

  const rows: string[] = [
    "dimension,multiplier,sample_events,cardinality,budget,structure,bytes,top20_overlap,mean_abs_rank_error,mean_relative_count_error",
  ];

  // Ascending order: each multiplier's sample is a prefix of the largest
  // one already loaded, since both start reading from the same file's
  // beginning — no need to re-read the file per multiplier.
  for (const mult of [...MULTIPLIERS].sort((a, b) => a - b)) {
    const sampleSize = Math.min(sampleSizeFor(mult), allMembers.length);
    const members = allMembers.slice(0, sampleSize);

    const exact = new Map<string, number>();
    for (const m of members) exact.set(m, (exact.get(m) ?? 0) + 1);
    const cardinality = exact.size;
    const exactTop20 = [...exact.entries()].sort((a, b) => b[1] - a[1]).slice(0, 20);

    console.log(`\n=== ${mult}x (${members.length} events, ${cardinality} distinct editors) ===`);

    for (const budget of BUDGETS) {
      const [cmsResult, ssResult] = runBudget(budget, members, exact, exactTop20);
      console.log(`[${budget.label}] CMS:`, cmsResult);
      console.log(`[${budget.label}] Space-Saving:`, ssResult);

      for (const result of [cmsResult, ssResult]) {
        rows.push(
          [
            "editors",
            mult,
            members.length,
            cardinality,
            budget.label,
            result.structure,
            result.bytes,
            result.top20Overlap,
            result.meanAbsRankError.toFixed(3),
            result.meanRelativeCountError.toFixed(4),
          ].join(","),
        );
      }
    }
  }

  mkdirSync("results", { recursive: true });
  writeFileSync("results/sketch-accuracy.csv", rows.join("\n") + "\n");
  console.log("\nwrote results/sketch-accuracy.csv");
}

main().catch((e) => {
  console.error("fatal:", e);
  process.exit(1);
});
