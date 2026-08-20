import csv
from collections import defaultdict
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.ticker import LogLocator, NullFormatter

RESULTS = "results"


def read_csv(path):
    with open(path, newline="") as f:
        return list(csv.DictReader(f))


def plot_latency_histogram():
    rows = read_csv(f"{RESULTS}/bench.csv")
    # Post-optimization samples only ("after-*"), across every multiplier we
    # tested — the current, real state of the system, not a single
    # cherry-picked load level.
    samples = [
        float(r["pipeline_latency_ms_sample"])
        for r in rows
        if r["label"].startswith("after-") and r["pipeline_latency_ms_sample"]
    ]
    samples.sort()

    def pct(p):
        return samples[min(len(samples) - 1, int(p * len(samples)))]

    p50, p95, p99 = pct(0.50), pct(0.95), pct(0.99)

    fig, ax = plt.subplots(figsize=(8, 5))
    ax.hist(samples, bins=40, color="#15803d", edgecolor="white")
    for label, val, style in [("p50", p50, "--"), ("p95", p95, "-."), ("p99", p99, ":")]:
        ax.axvline(val, color="#b91c1c", linestyle=style, linewidth=1.5)
        ax.text(val, ax.get_ylim()[1] * 0.95, f"{label}={val:.0f}ms", rotation=90, va="top", ha="right", color="#b91c1c")
    ax.set_xlabel("Pipeline latency (ms): producer ingest_ts to API frame emit")
    ax.set_ylabel("Sample count")
    ax.set_title(f"Pipeline latency distribution (post-optimization, n={len(samples)})")
    fig.tight_layout()
    fig.savefig(f"{RESULTS}/latency-histogram.png", dpi=150)
    print(f"wrote {RESULTS}/latency-histogram.png (n={len(samples)}, p50={p50:.0f}ms p95={p95:.0f}ms p99={p99:.0f}ms)")


def plot_lag_under_load():
    rows = read_csv(f"{RESULTS}/bench.csv")
    series = defaultdict(list)
    for r in rows:
        series[r["label"]].append((int(r["elapsed_s"]), int(r["consumer_lag"])))

    multipliers = ["1x", "10x", "50x", "100x"]
    fig, axes = plt.subplots(1, 2, figsize=(11, 5), sharey=True)
    for ax, phase in zip(axes, ["before", "after"]):
        for mult in multipliers:
            label = f"{phase}-{mult}"
            if label not in series:
                continue
            pts = sorted(series[label])
            xs = [p[0] for p in pts]
            ys = [p[1] for p in pts]
            ax.plot(xs, ys, marker="o", markersize=3, label=mult)
        ax.set_title(f"{phase} optimization")
        ax.set_xlabel("Elapsed seconds")
        ax.set_ylim(bottom=-0.5)
        ax.legend(title="replay rate")
    axes[0].set_ylabel("Consumer lag (messages)")
    fig.suptitle("Consumer lag under load, before vs after batching + Redis pipelining")
    fig.tight_layout()
    fig.savefig(f"{RESULTS}/lag-under-load.png", dpi=150)
    print(f"wrote {RESULTS}/lag-under-load.png")


def plot_sketch_accuracy():
    rows = read_csv(f"{RESULTS}/sketch-accuracy.csv")
    multipliers = sorted({r["multiplier"] for r in rows}, key=int)
    structures = [("count_min_sketch", "CMS", "#15803d"), ("space_saving", "Space-Saving", "#6d28d9")]

    fig, axes = plt.subplots(1, len(multipliers), figsize=(5 * len(multipliers), 5), sharey=True)
    if len(multipliers) == 1:
        axes = [axes]

    for ax, mult in zip(axes, multipliers):
        cardinality = next(r["cardinality"] for r in rows if r["multiplier"] == mult)
        for structure, label, color in structures:
            pts = sorted(
                (int(r["bytes"]), float(r["mean_relative_count_error"]))
                for r in rows
                if r["multiplier"] == mult and r["structure"] == structure
            )
            xs = [p[0] for p in pts]
            ys = [max(p[1], 1e-5) for p in pts]  # avoid log(0)
            ax.plot(xs, ys, marker="o", label=label, color=color)
        ax.set_xscale("log")
        ax.set_yscale("log")
        # Default log-scale minor ticks crowd and overlap at this narrow a
        # range (~1KB-64KB); only label the major decade ticks.
        ax.xaxis.set_minor_locator(LogLocator(subs="all"))
        ax.xaxis.set_minor_formatter(NullFormatter())
        ax.set_title(f"{mult} replay ({cardinality} distinct editors)")
        ax.set_xlabel("Bytes of state held")
        ax.legend()
    axes[0].set_ylabel("Mean relative count error (log scale)")
    fig.suptitle("Sketch accuracy vs memory budget, top-20 editors")
    fig.tight_layout()
    fig.savefig(f"{RESULTS}/sketch-accuracy.png", dpi=150)
    print(f"wrote {RESULTS}/sketch-accuracy.png")


if __name__ == "__main__":
    plot_latency_histogram()
    plot_lag_under_load()
    plot_sketch_accuracy()
