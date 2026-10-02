#!/usr/bin/env python3
"""
compare_groups.py

Compare two GROUPS of experiment runs (parquet files with columns:
metric, timestamp, value, pod), stored in a directory. Every file whose
name contains PATTERN_A is grouped together, every file whose name
contains PATTERN_B is grouped together, and for every metric shared by
both groups the script plots the group mean over time as a line with a
shaded +/- 1 standard deviation band, so the two groups can be compared
run-to-run-consistency and all.

Runs within a group don't need identical lengths or timestamps: each
run is put on a "seconds since its own start" axis, and its real,
un-interpolated samples are snapped into discrete time buckets (bucket
width = each metric's own sampling interval, auto-inferred from the
data). The mean/std at each bucket is computed only from the actual
values that landed there -- no synthetic/interpolated points are ever
created. Only the time range covered by every run in a group is used,
so mean/std aren't skewed by a run that ended early.

Metrics reported per-pod (e.g. task_manager_cpu, task_manager_memory)
are first averaged across pods within each individual run, so runs with
a different number of task managers stay comparable.

Additionally, from the raw pod names the script derives:
  - task_manager_replicas_regular / _medium / _large: the number of
    active task-manager replicas of each size (regular = 2 task slots,
    medium = 8, large = 16 per replica), as observed directly from which
    pods reported data at each timestamp.
  - task_manager_task_slots: the aggregate task slots those replicas
    provide (replica_count * slots-per-replica, summed across sizes).
These are inherently discrete counts, so -- unlike the other metrics --
they're plotted as step functions (each value held flat until the next
observed change) rather than lines connecting samples, since a straight
line between two counts would imply a gradual change that never
happened.

Summary CSV
-----------
Alongside the plots, a plain per-metric summary is written for EVERY
metric in each group (not just the ones shared by both). No steady-state
window, spike detection, drain time or any other special-casing: each
run's samples (averaged across pods per timestamp) are simply averaged
over the whole run, and the columns are

    group, metric, n_runs,
    mean               mean over runs of each run's mean value
    std_across_runs    std (ddof=1) over runs of each run's mean value
                       (run-to-run consistency; empty if n_runs == 1)
    mean_within_run_std  mean over runs of each run's own std over time
                       (how much the metric fluctuates within a run)

Usage
-----
    python compare_groups.py DIRECTORY PATTERN_A PATTERN_B [--out-dir OUT_DIR] [--bin-seconds SECONDS]

Example
-------
    python compare_groups.py ./runs "8_tm" "64_tm" --out-dir comparison_plots

    -> groups every *.parquet file in ./runs whose filename contains
       "8_tm" into group A, every file containing "64_tm" into group B,
       and plots mean +/- std per metric for each group.

Output
------
One PNG per metric (saved to --out-dir, default "./comparison_plots"),
a combined "all_metrics_overview.png" grid figure, and a
"<category>_<dirname>_summary.csv" with the plain mean/std table.
"""

import argparse
import math
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from matplotlib.ticker import MaxNLocator

COLOR_A = "#f39301"
COLOR_B = "#013261"

# Flink task-manager pods come in three sizes, distinguished by a suffix
# on the pod name after the common prefix below (e.g.
# "flink-application-cluster-taskmanager-7d8f9c6d59-4x2vg" -> regular,
# "flink-application-cluster-taskmanager-medium-..." -> medium,
# "flink-application-cluster-taskmanager-large-..." -> large). Each size
# carries a fixed number of task slots per replica.
TASKMANAGER_POD_PREFIX = "flink-application-cluster-taskmanager"
TASKMANAGER_SLOTS = {
    "regular": 2,
    "medium": 8,
    "large": 16,
}
# Metrics whose values are inherently discrete counts (replica counts,
# aggregate task slots) and should be plotted as step functions instead
# of straight lines connecting samples, since a straight line between two
# distinct counts implies a gradual change that never happened.
DISCRETE_METRIC_PREFIXES = ("task_manager_replicas_",)
DISCRETE_METRIC_NAMES = {"task_manager_task_slots"}


def is_discrete_metric(metric: str) -> bool:
    return metric in DISCRETE_METRIC_NAMES or metric.startswith(
        DISCRETE_METRIC_PREFIXES
    )


# Ordered (substring, y-axis label) rules; the first match wins.
# Adjust the substrings to match your actual metric names.
Y_LABEL_RULES = [
    ("task_manager_replicas_regular", "Regular TM Replicas"),
    ("task_manager_replicas_medium", "Medium TM Replicas"),
    ("task_manager_replicas_large", "Large TM Replicas"),
    ("task_manager_task_slots", "Provisioned Slots"),
    ("backpressure", "Fraction"),
    ("busy", "Fraction"),
    ("idle", "Fraction"),
    ("cpu", "CPU Utilization (%)"),
    ("memory", "Memory Utilization (%)"),
    ("mongo", "MongoDB Output (insert/s)"),
]


def y_label_for_metric(metric: str) -> str:
    m = metric.lower()
    for key, label in Y_LABEL_RULES:
        if key in m:
            return label
    return metric.replace("_", " ").title()  # fallback


# ----------------------------------------------------------------------
# File discovery / loading
# ----------------------------------------------------------------------


def find_group_files(directory: Path, pattern: str) -> list[Path]:
    """All .parquet files directly under `directory` whose filename
    contains `pattern` (case-sensitive substring match)."""
    return sorted(p for p in directory.glob("*.parquet") if pattern in p.name)


def classify_taskmanager_pod(pod: str) -> str | None:
    """Classify a pod name as a task-manager replica size ('regular',
    'medium', 'large'), or return None if it isn't a Flink task-manager
    pod at all (e.g. the job manager)."""
    if not pod.startswith(TASKMANAGER_POD_PREFIX):
        return None
    remainder = pod[len(TASKMANAGER_POD_PREFIX) :]
    if remainder.startswith("-large"):
        return "large"
    if remainder.startswith("-medium"):
        return "medium"
    return "regular"


def taskmanager_derived_rows(df: pd.DataFrame) -> pd.DataFrame:
    """Derive, from the raw per-pod rows of a single run, a
    (timestamp, t_seconds, value) series per task-manager size for the
    number of active replicas of that size, plus one aggregate series
    ("task_manager_task_slots") for the total task slots those replicas
    provide (replica_count * slots-per-replica, summed across sizes). A
    pod counts as "active" at a timestamp if it reported at least one row
    (any metric) at that timestamp -- these are real observed counts, no
    synthetic points are added for gaps between them.

    Returns a dataframe with columns [metric, timestamp, t_seconds,
    value, pod] ready to be concatenated onto the run's own dataframe, or
    an empty dataframe if the run has no task-manager pods.
    """
    pods = df[["pod"]].drop_duplicates()
    pods["variant"] = pods["pod"].map(classify_taskmanager_pod)
    tm_pods = pods.dropna(subset=["variant"])
    if tm_pods.empty:
        return df.iloc[0:0]

    variant_by_pod = dict(zip(tm_pods["pod"], tm_pods["variant"]))
    sub = df[df["pod"].isin(variant_by_pod)].copy()
    sub["variant"] = sub["pod"].map(variant_by_pod)

    # One row per (timestamp, pod) that actually reported something --
    # this is the real, observed presence of that replica.
    presence = sub.drop_duplicates(subset=["timestamp", "pod"])[
        ["timestamp", "t_seconds", "pod", "variant"]
    ]

    replica_counts = (
        presence.groupby(["timestamp", "t_seconds", "variant"])["pod"]
        .nunique()
        .reset_index(name="value")
    )

    derived_frames = []
    for variant in TASKMANAGER_SLOTS:
        v = replica_counts[replica_counts["variant"] == variant]
        if v.empty:
            continue
        rows = v[["timestamp", "t_seconds", "value"]].copy()
        rows["metric"] = f"task_manager_replicas_{variant}"
        rows["pod"] = "derived"
        derived_frames.append(rows)

    # Aggregate task slots: at each timestamp, sum replica_count * slots
    # across whichever sizes were present.
    pivot = replica_counts.pivot_table(
        index=["timestamp", "t_seconds"],
        columns="variant",
        values="value",
        fill_value=0,
    )
    for variant in TASKMANAGER_SLOTS:
        if variant not in pivot.columns:
            pivot[variant] = 0
    pivot["value"] = sum(
        pivot[variant] * slots for variant, slots in TASKMANAGER_SLOTS.items()
    )
    slots_rows = pivot.reset_index()[["timestamp", "t_seconds", "value"]].copy()
    slots_rows["metric"] = "task_manager_task_slots"
    slots_rows["pod"] = "derived"
    derived_frames.append(slots_rows)

    return pd.concat(derived_frames, ignore_index=True)[
        ["metric", "timestamp", "value", "pod", "t_seconds"]
    ]


def load_run(path: Path) -> pd.DataFrame:
    """Load a run parquet file, add a relative-time column (seconds
    since the first timestamp in that run), and append derived
    task-manager replica-count / task-slot metrics computed from the raw
    per-pod rows (see `taskmanager_derived_rows`)."""
    df = pd.read_parquet(path)
    df = df.sort_values("timestamp").copy()
    t0 = df["timestamp"].min()
    df["t_seconds"] = (df["timestamp"] - t0).dt.total_seconds()
    derived = taskmanager_derived_rows(df)
    if not derived.empty:
        df = pd.concat([df, derived], ignore_index=True)
    return df


def series_for_metric(df: pd.DataFrame, metric: str) -> pd.DataFrame:
    """Return a (t_seconds, value) series for a metric within a single
    run, averaging across pods at each timestamp when the metric has
    multiple pods (e.g. one row per task manager)."""
    sub = df[df["metric"] == metric]
    if sub["pod"].nunique() > 1:
        sub = sub.groupby("timestamp", as_index=False).agg(
            value=("value", "mean"), t_seconds=("t_seconds", "first")
        )
    return sub[["t_seconds", "value"]].sort_values("t_seconds")


# ----------------------------------------------------------------------
# Group aggregation
# ----------------------------------------------------------------------


def infer_bin_seconds(per_run_series: list[pd.DataFrame]) -> float:
    """Infer a sampling interval (in seconds) for a metric from the
    actual gaps between consecutive samples, so raw samples can be
    grouped into discrete time buckets without any interpolation."""
    diffs = []
    for s in per_run_series:
        d = np.diff(s["t_seconds"].values)
        d = d[d > 0]
        if len(d):
            diffs.append(np.median(d))
    if not diffs:
        return 15.0  # fallback, shouldn't normally be hit
    return float(np.median(diffs))


def group_mean_std(
    runs: list[pd.DataFrame], metric: str, bin_seconds: float | None = None
):
    """Given several runs (each a full dataframe for one file), compute
    the mean and std of `metric` across those runs at each discrete
    sample time -- no interpolation. Each run's real (t_seconds, value)
    samples are snapped to the nearest time bucket (bucket width =
    `bin_seconds`, auto-inferred from the data's own sampling interval
    if not given) and the mean/std at each bucket is computed only from
    the actual values that landed there. Only the time range common to
    *all* runs that contain this metric is used, so the mean/std are not
    skewed by runs that ended early.

    Returns (bucket_times, mean, std, n_runs_used) or None if no run has
    the metric.
    """
    per_run_series = []
    max_common_t = None
    for df in runs:
        s = series_for_metric(df, metric)
        if s.empty:
            continue
        per_run_series.append(s)
        run_max = s["t_seconds"].max()
        max_common_t = run_max if max_common_t is None else min(max_common_t, run_max)

    if not per_run_series or max_common_t is None or max_common_t <= 0:
        return None

    if bin_seconds is None:
        bin_seconds = infer_bin_seconds(per_run_series)

    # Collect every run's raw, real samples (restricted to the time range
    # every run in the group covers) and snap each to the nearest bucket.
    all_points = pd.concat(
        [s[s["t_seconds"] <= max_common_t] for s in per_run_series],
        ignore_index=True,
    )
    all_points["t_bucket"] = (
        all_points["t_seconds"] / bin_seconds
    ).round() * bin_seconds

    grouped = (
        all_points.groupby("t_bucket")["value"]
        .agg(mean="mean", std="std", count="count")
        .reset_index()
        .sort_values("t_bucket")
    )
    grouped["std"] = grouped["std"].fillna(
        0.0
    )  # a bucket with a single sample has no spread

    return (
        grouped["t_bucket"].to_numpy(),
        grouped["mean"].to_numpy(),
        grouped["std"].to_numpy(),
        len(per_run_series),
    )


# ----------------------------------------------------------------------
# Plain summary statistics
# ----------------------------------------------------------------------


def summarize_metric(runs: list[pd.DataFrame], metric: str) -> dict | None:
    """Plain mean / std of one metric over a group of runs. For each run,
    the metric is averaged across pods per timestamp (same as the plots)
    and then simply averaged over every sample in the run. Returns None
    if no run has the metric."""
    run_means, run_stds = [], []
    for df in runs:
        s = series_for_metric(df, metric)
        if s.empty:
            continue
        run_means.append(s["value"].mean())
        run_stds.append(s["value"].std(ddof=1))  # NaN if the run has 1 sample

    if not run_means:
        return None

    run_means = pd.Series(run_means, dtype=float)
    run_stds = pd.Series(run_stds, dtype=float)
    return {
        "n_runs": len(run_means),
        "mean": run_means.mean(),
        "std_across_runs": run_means.std(ddof=1),  # NaN if only one run
        "mean_within_run_std": run_stds.mean(),  # NaN if all NaN
    }


def write_summary(groups: list[tuple[str, list[pd.DataFrame]]], path: Path):
    """Write the plain mean/std summary CSV: one row per (group, metric),
    for every metric present in that group."""
    rows = []
    for label, runs in groups:
        metrics = sorted(set().union(*(set(df["metric"].unique()) for df in runs)))
        for metric in metrics:
            stats = summarize_metric(runs, metric)
            if stats is None:
                continue
            rows.append({"group": label, "metric": metric, **stats})
    pd.DataFrame(
        rows,
        columns=[
            "group",
            "metric",
            "n_runs",
            "mean",
            "std_across_runs",
            "mean_within_run_std",
        ],
    ).to_csv(path, index=False)
    print(f"Saved {path}")


# ----------------------------------------------------------------------
# Plotting
# ----------------------------------------------------------------------


def plot_metric(
    ax, metric, agg_a, agg_b, label_a, label_b, per_pod_note=False, discrete=False
):
    if discrete:
        # Discrete counts (replica counts, aggregate task slots): a
        # straight line between two samples would imply a gradual change
        # that never happened, so hold each value flat until the next
        # observed sample instead ("steps-post").
        if agg_a is not None:
            grid_a, mean_a, std_a, n_a = agg_a
            ax.step(
                grid_a,
                mean_a,
                where="post",
                color=COLOR_A,
                linewidth=1.6,
                label=f"{label_a} (n={n_a})",
            )
            ax.fill_between(
                grid_a,
                mean_a - std_a,
                mean_a + std_a,
                step="post",
                color=COLOR_A,
                alpha=0.2,
            )
        if agg_b is not None:
            grid_b, mean_b, std_b, n_b = agg_b
            ax.step(
                grid_b,
                mean_b,
                where="post",
                color=COLOR_B,
                linewidth=1.6,
                label=f"{label_b} (n={n_b})",
            )
            ax.fill_between(
                grid_b,
                mean_b - std_b,
                mean_b + std_b,
                step="post",
                color=COLOR_B,
                alpha=0.2,
            )
        ax.yaxis.set_major_locator(MaxNLocator(integer=True))
    else:
        if agg_a is not None:
            grid_a, mean_a, std_a, n_a = agg_a
            ax.plot(
                grid_a,
                mean_a,
                color=COLOR_A,
                linewidth=1.6,
                label=f"{label_a} (n={n_a})",
            )
            ax.fill_between(
                grid_a, mean_a - std_a, mean_a + std_a, color=COLOR_A, alpha=0.2
            )
        if agg_b is not None:
            grid_b, mean_b, std_b, n_b = agg_b
            ax.plot(
                grid_b,
                mean_b,
                color=COLOR_B,
                linewidth=1.6,
                label=f"{label_b} (n={n_b})",
            )
            ax.fill_between(
                grid_b, mean_b - std_b, mean_b + std_b, color=COLOR_B, alpha=0.2
            )

    ax.set_xlabel("Time since run start (s)")
    ax.set_ylabel(y_label_for_metric(metric))
    ax.grid(alpha=0.3)
    ax.legend(fontsize=8)
    ax.grid(alpha=0.3)
    ax.legend(fontsize=8)


def main():
    parser = argparse.ArgumentParser(
        description="Compare two groups of experiment runs (mean +/- std per metric)."
    )
    parser.add_argument("directory", help="Directory to search for .parquet run files")
    parser.add_argument(
        "pattern_a", help="Substring identifying files that belong to group A"
    )
    parser.add_argument(
        "pattern_b", help="Substring identifying files that belong to group B"
    )
    parser.add_argument(
        "--out-dir", default="comparison_plots", help="Directory to save plots into"
    )
    parser.add_argument(
        "--bin-seconds",
        type=float,
        default=None,
        help="Width (in seconds) of the discrete time buckets samples are grouped into. "
        "If omitted, it's auto-inferred per metric from that metric's own sampling interval.",
    )
    args = parser.parse_args()

    directory = Path(args.directory)
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    # Use the target directory's name (last path component) as a filename
    # prefix so plots from different input directories don't collide/overwrite
    # each other when saved into the same --out-dir.
    dir_prefix = directory.resolve().name or "root"

    # Both patterns share a common leading category (e.g. the application
    # name) before their first underscore, e.g. "myapp_8_tm" and
    # "myapp_64_tm" both start with "myapp". Pull that shared piece from
    # pattern_a and prepend it to every output filename too.
    category_prefix = args.pattern_a.split("_", 1)[0]

    files_a = find_group_files(directory, args.pattern_a)
    files_b = find_group_files(directory, args.pattern_b)

    if not files_a:
        raise SystemExit(
            f"No .parquet files in {directory} contain pattern '{args.pattern_a}'"
        )
    if not files_b:
        raise SystemExit(
            f"No .parquet files in {directory} contain pattern '{args.pattern_b}'"
        )

    overlap = set(files_a) & set(files_b)
    if overlap:
        print(
            f"Warning: {len(overlap)} file(s) match BOTH patterns and will be counted in both groups: "
            f"{[p.name for p in overlap]}"
        )

    print(
        f"Group A ('{args.pattern_a}'): {len(files_a)} file(s) -> {[p.name for p in files_a]}"
    )
    print(
        f"Group B ('{args.pattern_b}'): {len(files_b)} file(s) -> {[p.name for p in files_b]}"
    )

    runs_a = [load_run(p) for p in files_a]
    runs_b = [load_run(p) for p in files_b]

    label_a = f"'{args.pattern_a}' group"
    label_b = f"'{args.pattern_b}' group"

    # Plain mean/std table for every metric in each group.
    write_summary(
        [(label_a, runs_a), (label_b, runs_b)],
        out_dir / f"{category_prefix}_{dir_prefix}_summary.csv",
    )

    metrics_a = set().union(*(set(df["metric"].unique()) for df in runs_a))
    metrics_b = set().union(*(set(df["metric"].unique()) for df in runs_b))
    shared_metrics = sorted(metrics_a & metrics_b)

    only_a = metrics_a - metrics_b
    only_b = metrics_b - metrics_a
    if only_a:
        print(f"Note: metrics only in group A: {sorted(only_a)}")
    if only_b:
        print(f"Note: metrics only in group B: {sorted(only_b)}")
    print(f"Comparing {len(shared_metrics)} shared metrics: {shared_metrics}")

    # Individual plots
    metric_aggs = {}
    for metric in shared_metrics:
        agg_a = group_mean_std(runs_a, metric, args.bin_seconds)
        agg_b = group_mean_std(runs_b, metric, args.bin_seconds)
        metric_aggs[metric] = (agg_a, agg_b)

        discrete = is_discrete_metric(metric)
        per_pod = not discrete and any(
            df[df["metric"] == metric]["pod"].nunique() > 1 for df in (runs_a + runs_b)
        )

        fig, ax = plt.subplots(figsize=(9, 4.5))
        plot_metric(
            ax,
            metric,
            agg_a,
            agg_b,
            label_a,
            label_b,
            per_pod_note=per_pod,
            discrete=discrete,
        )
        fig.tight_layout()
        out_path = out_dir / f"{category_prefix}_{dir_prefix}_{metric}.png"
        fig.savefig(out_path, dpi=150)
        plt.close(fig)
        print(f"Saved {out_path}")

    # Combined overview grid
    n = len(shared_metrics)
    ncols = 2
    nrows = math.ceil(n / ncols)
    fig, axes = plt.subplots(nrows, ncols, figsize=(11, 3.6 * nrows))
    axes = axes.flatten() if n > 1 else [axes]

    for i, metric in enumerate(shared_metrics):
        agg_a, agg_b = metric_aggs[metric]
        discrete = is_discrete_metric(metric)
        per_pod = not discrete and any(
            df[df["metric"] == metric]["pod"].nunique() > 1 for df in (runs_a + runs_b)
        )
        plot_metric(
            axes[i],
            metric,
            agg_a,
            agg_b,
            label_a,
            label_b,
            per_pod_note=per_pod,
            discrete=discrete,
        )

    for j in range(len(shared_metrics), len(axes)):
        axes[j].axis("off")

    fig.suptitle(
        f"{label_a} (n={len(files_a)})  vs  {label_b} (n={len(files_b)})",
        fontsize=14,
        fontweight="bold",
        y=1.0,
    )
    fig.tight_layout()
    overview_path = out_dir / f"{category_prefix}_{dir_prefix}_all_metrics_overview.png"
    fig.savefig(overview_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"Saved {overview_path}")


if __name__ == "__main__":
    main()
