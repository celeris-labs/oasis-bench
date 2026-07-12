#!/usr/bin/env python3
"""Plot throughput measurements: one line per (source, hardware decoders)."""

import json
from collections import defaultdict
from pathlib import Path

import matplotlib
matplotlib.use("Agg")  # no GUI window needed; avoids slow X11/GTK init
import matplotlib.patheffects as path_effects
import matplotlib.pyplot as plt
from matplotlib.legend_handler import HandlerTuple

REPO_ROOT = Path(__file__).parent.parent
MEASUREMENTS_DIR = REPO_ROOT / "measurements" / "throughput" / "sf-30-4"
QUERIES_PER_RUN = 22 * 4  # TPC-H runs 22 queries on 4 streams

BASELINE = "parquet"  # parquet read over the network (rdma)
PARQUET_DISK = "parquet_disk"  # parquet read from local disk
MEMORY = "memory"

PHYSICAL_CORES = 16
MAX_THREADS = 32  # show hyperthreading up to here, ignore measurements beyond

PARQUET_COLOR = "#B8860B"  # dark yellow: duckdb
PARQUET_DISK_COLOR = "#7B3294"  # purple: duckdb parquet from local disk
MEMORY_COLOR = "#00695C"  # teal: duckdb in-memory tables

# Blue ramp per number of hardware decoders:
# darkest for the most decoders, progressively lighter for fewer.
DECODER_COLORS = {
    4: "#1B4F8A",
    3: "#3A70AF",
    2: "#6495C9",
    1: "#8AB0DA",
}


def load_measurements():
    """Group runtime by (source, decoders), keyed on thread count.

    Files are named by timestamp; iterating in ascending order lets the
    newest measurement win for each (source, decoders, threads) key."""
    runtime = defaultdict(dict)
    for path in sorted(MEASUREMENTS_DIR.glob("*.json"), key=lambda p: int(p.stem)):
        with open(path) as f:
            m = json.load(f)
        if m["threads"] > MAX_THREADS:
            continue
        source = m["source"]
        if source == BASELINE and not m["ip"]:  # local disk, not rdma
            source = PARQUET_DISK
        # Fall back to the old key name for measurements taken before the rename.
        key = (source, m.get("decoders", m.get("oasis_num_streams", 0)))
        runtime[key][m["threads"]] = m["runtime_sec"]
    return runtime


def series_style(source, decoders):
    if source == BASELINE:
        return f"{source} (duckdb)", PARQUET_COLOR, "dotted", "s", 3
    if source == PARQUET_DISK:
        return f"{source} (duckdb)", PARQUET_DISK_COLOR, "dotted", "s", 3
    if source == MEMORY:
        return f"{source} (duckdb)", MEMORY_COLOR, "solid", "o", 3.5
    label = f"{source} ({decoders} decoder{'s' if decoders != 1 else ''})"
    color = DECODER_COLORS.get(decoders, "#1B4F8A")
    return label, color, "solid", "o", 3.5


def new_figure(max_threads=MAX_THREADS, height=2.5):
    fig, ax = plt.subplots(figsize=(4.5, height), layout="constrained")
    ax.set_xlabel("DuckDB worker threads")
    ax.set_xticks([1] + list(range(4, max_threads + 1, 4)))
    ax.set_xlim(0, max_threads + 1)
    ax.axvspan(PHYSICAL_CORES, max_threads + 1, facecolor="0.95",
               edgecolor="none", zorder=0)  # shade the hyperthreading half
    ax.axvline(PHYSICAL_CORES, color="black", linestyle=":", linewidth=1,
               zorder=0)  # hyperthreading beyond this
    outline = [path_effects.withStroke(linewidth=3, foreground="white")]
    ax.text((1 + PHYSICAL_CORES) / 2, 0.96, "physical cores",
            transform=ax.get_xaxis_transform(), ha="center", va="top",
            path_effects=outline)
    ax.text((PHYSICAL_CORES + max_threads + 1) / 2, 0.04, "hyper-threading",
            transform=ax.get_xaxis_transform(), ha="center", va="bottom",
            path_effects=outline)  # centered between vline and right edge
    ax.grid(True, color="black", linestyle=":", linewidth=0.5)
    return fig, ax


def add_legend(fig, ax, labels=None, ncol=None, handles=None,
               handlelength=1.4):
    ax_handles, ax_labels = ax.get_legend_handles_labels()
    handles = handles or ax_handles
    labels = labels or ax_labels
    fig.legend(handles, labels, loc="outside upper center",
               ncol=ncol or (len(labels) + 2) // 3,
               frameon=True, fancybox=False, edgecolor="black",
               handlelength=handlelength, handletextpad=0.4,
               columnspacing=0.8, borderpad=0.3,
               handler_map={tuple: HandlerTuple(ndivide=None, pad=0.15)})


def add_oasis_legend(fig, ax):
    """One row: parquet baseline, then all oasis lines as a single entry."""
    handles, _ = ax.get_legend_handles_labels()
    add_legend(fig, ax, handles=[handles[0], tuple(handles[1:])],
               labels=["Parquet extension", "Oasis (1-4 decoders)"],
               ncol=2, handlelength=2.8)


def save(fig, name):
    for ax in fig.axes:
        ax.set_ylim(bottom=0)
    plots_dir = REPO_ROOT / "plots"
    plots_dir.mkdir(exist_ok=True)
    out = plots_dir / name
    fig.savefig(out)
    print(f"Wrote {out}")


def plot_series(ax, runtime, key, y_from_points):
    label, color, linestyle, marker, markersize = series_style(*key)
    points = sorted(runtime[key].items())
    threads, values = y_from_points(points)
    ax.plot(threads, values, color=color, linestyle=linestyle,
            marker=marker, markersize=markersize, label=label)


def qph(points):
    return ([t for t, _ in points],
            [QUERIES_PER_RUN / rt * 3600 for _, rt in points])


def main():
    runtime = load_measurements()
    baseline = runtime.get((BASELINE, 0), {})

    # Baseline first, then oasis by ascending decoder count so that
    # more decoders draw on top.
    order = sorted((k for k in runtime if k[0] not in (MEMORY, PARQUET_DISK)),
                   key=lambda k: (k[0] != BASELINE, k[1]))

    # Absolute throughput: parquet baseline and oasis, up to 24 threads.
    fig, ax = new_figure(max_threads=24)

    def qph24(points):
        return qph([(t, rt) for t, rt in points if t <= 24])

    for key in order:
        plot_series(ax, runtime, key, qph24)
    ax.set_ylabel("TPC-H throughput (Q/h)")
    outline = [path_effects.withStroke(linewidth=3, foreground="white")]

    # Oasis with 4 decoders on 5 threads already almost matches the
    # parquet extension at any thread count.
    oasis4_5 = QUERIES_PER_RUN / runtime[("oasis", 4)][5] * 3600
    ax.axhline(oasis4_5, color="black", linestyle="--", linewidth=1, zorder=0)
    ax.text(23.6, oasis4_5 + 60, f"{oasis4_5:,.0f} Q/h",
            ha="right", va="bottom", path_effects=outline)
    ax.annotate("5 threads", xy=(5, oasis4_5), xytext=(3, 4400),
                ha="center", va="bottom", path_effects=outline,
                arrowprops=dict(arrowstyle="->", color="black", linewidth=1))

    # Speedup of oasis (4 decoders) over parquet at 16 threads.
    oasis4_16 = QUERIES_PER_RUN / runtime[("oasis", 4)][16] * 3600
    parquet_16 = QUERIES_PER_RUN / runtime[(BASELINE, 0)][16] * 3600
    ax.annotate("", xy=(16, oasis4_16), xytext=(16, parquet_16),
                arrowprops=dict(arrowstyle="<->", color="black", linewidth=1))
    ax.text(15.6, (oasis4_16 + parquet_16) / 2,
            f"{oasis4_16 / parquet_16:.1f}x",
            ha="right", va="center", path_effects=outline)

    add_oasis_legend(fig, ax)
    save(fig, "throughput.pdf")

    # Throughput relative to the parquet baseline.
    fig, ax = new_figure()
    for key in order:
        def ratio(points):
            return ([t for t, _ in points if t in baseline],
                    [baseline[t] / rt for t, rt in points if t in baseline])
        plot_series(ax, runtime, key, ratio)
    ax.axhline(1.0, color="gray", linestyle="--", alpha=0.5, zorder=0)
    ax.set_ylabel(f"Throughput relative to {BASELINE}")
    ax.set_title(f"TPC-H throughput relative to {BASELINE}")
    add_oasis_legend(fig, ax)
    save(fig, "throughput_relative.pdf")

    # Introduction: memory and parquet baselines only.
    fig, ax = new_figure(height=2.25)
    for key in [(MEMORY, 0), (PARQUET_DISK, 0)]:
        plot_series(ax, runtime, key, qph)
    ax.set_ylabel("TPC-H throughput (Q/h)")

    # Speedup of memory over parquet from disk at 16 threads.
    memory_32 = QUERIES_PER_RUN / runtime[(MEMORY, 0)][32] * 3600
    disk_32 = QUERIES_PER_RUN / runtime[(PARQUET_DISK, 0)][32] * 3600
    ax.annotate("", xy=(32, memory_32), xytext=(32, disk_32),
                arrowprops=dict(arrowstyle="<->", color="black", linewidth=1))
    ax.text(31.6, (memory_32 + disk_32) / 2,
            f"{memory_32 / disk_32:.1f}x",
            ha="right", va="center",
            path_effects=[path_effects.withStroke(linewidth=3,
                                                  foreground="white")])

    add_legend(fig, ax, labels=["In-memory", "Parquet files"], ncol=2)
    save(fig, "introduction.pdf")


if __name__ == "__main__":
    main()
