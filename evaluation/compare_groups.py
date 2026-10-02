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

Runs don't need identical lengths or timestamps: each run is put on a
"seconds since its own start" axis, and its real samples are snapped
into discrete time buckets (bucket width = each metric's own sampling
interval, auto-inferred from the data; nothing is interpolated between
samples). A run (in either group) that is shorter than the longest run
of that metric is EXTRAPOLATED by holding its last observed value flat
until it reaches the same length as the longest one, so both groups are
always compared over the same time range. The mean/std at each bucket
are taken across runs. Note that the tail of a shortened run is
therefore a held value, not measured data.

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
run's samples (averaged across pods per timestamp, and extended with
its last value if the run is shorter than the longest one, as above) are
simply averaged over the whole run, and the columns are

    group, metric, n_runs,
    mean               mean over runs of each run's mean value
    std_across_runs    std (ddof=1) over runs of each run's mean value
                       (run-to-run consistency; empty if n_runs == 1)
    mean_within_run_std  mean over runs of each run's own std over time
                       (how much the metric fluctuates within a run)

With --backpressure-threshold T, the "backpressure" rows additionally get
    mean_frac_time_above_threshold   mean over runs of the fraction of the
                       run's time buckets with backpressure > T
    std_frac_time_above_threshold    std (ddof=1) of that fraction over runs

Usage
-----
    python compare_groups.py DIRECTORY PATTERN_A PATTERN_B [--out-dir OUT_DIR] [--bin-seconds SECONDS]
                             [--backpressure-threshold T]

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


def run_series_list(runs: list[pd.DataFrame], metric: str) -> list[pd.DataFrame]:
    """Non-empty per-run (t_seconds, value) series for `metric`."""
    out = []
    for df in runs:
        s = series_for_metric(df, metric)
        if not s.empty:
            out.append(s)
    return out


def metric_time_grid(series_lists: list[list[pd.DataFrame]], bin_seconds=None):
    """Bucket width and end time shared by every run of one metric, across
    all groups being compared: the end time is that of the LONGEST run, so
    shorter runs (in either group) get extended to it. Returns
    (bin_seconds, t_end) or None if no run has the metric."""
    all_series = [s for lst in series_lists for s in lst]
    if not all_series:
        return None
    if bin_seconds is None:
        bin_seconds = infer_bin_seconds(all_series)
    t_end = max(s["t_seconds"].max() for s in all_series)
    return bin_seconds, t_end


def bucketed_runs(
    series_list: list[pd.DataFrame], bin_seconds: float, t_end: float
) -> pd.DataFrame:
    """Put every run on the same time grid (0, bin, 2*bin, ... t_end).
    Rows = runs, columns = bucket times in seconds. Each run's real
    samples are snapped to the nearest bucket (several samples in one
    bucket are averaged). Buckets AFTER a run's last sample are filled by
    holding that last value (extrapolation); gaps inside a run are left
    empty (NaN) and simply ignored in the statistics."""
    n_buckets = int(round(t_end / bin_seconds)) + 1
    rows = []
    for s in series_list:
        idx = (s["t_seconds"] / bin_seconds).round().astype(int)
        per_bucket = s["value"].groupby(idx.values).mean()  # sorted by bucket
        row = per_bucket.reindex(range(n_buckets))
        last_idx = int(per_bucket.index[-1])
        if last_idx + 1 < n_buckets:
            row.iloc[last_idx + 1 :] = per_bucket.iloc[-1]
        rows.append(row.to_numpy(dtype=float))
    return pd.DataFrame(np.vstack(rows), columns=np.arange(n_buckets) * bin_seconds)


def group_mean_std(series_list: list[pd.DataFrame], bin_seconds: float, t_end: float):
    """Mean and std (ddof=1) across the group's runs at each time bucket,
    after extending shorter runs to `t_end` by holding their last value
    (see `bucketed_runs`).

    Returns (bucket_times, mean, std, n_runs) or None if no run has the
    metric."""
    if not series_list:
        return None
    m = bucketed_runs(series_list, bin_seconds, t_end)
    mean = m.mean(axis=0)
    std = m.std(axis=0, ddof=1).fillna(0.0)  # a single run has no spread
    keep = mean.notna().to_numpy()
    return (
        m.columns.to_numpy()[keep],
        mean.to_numpy()[keep],
        std.to_numpy()[keep],
        len(series_list),
    )


# ----------------------------------------------------------------------
# Plain summary statistics
# ----------------------------------------------------------------------


def summarize_metric(
    series_list: list[pd.DataFrame],
    bin_seconds: float,
    t_end: float,
    threshold: float | None = None,
) -> dict:
    """Plain mean / std of one metric over a group of runs. Each run is
    put on the shared time grid (short runs extended with their last
    value, see `bucketed_runs`) and simply averaged over the whole run.
    If `threshold` is given, also the fraction of each run's time buckets
    with value > threshold, averaged over runs (mean) and its std across
    runs."""
    m = bucketed_runs(series_list, bin_seconds, t_end)
    run_means = m.mean(axis=1)  # one value per run
    run_stds = m.std(axis=1, ddof=1)  # NaN if a run has a single sample
    stats = {
        "n_runs": len(series_list),
        "mean": run_means.mean(),
        "std_across_runs": run_means.std(ddof=1),  # NaN if only one run
        "mean_within_run_std": run_stds.mean(),
    }
    if threshold is not None:
        frac = (m > threshold).sum(axis=1) / m.notna().sum(axis=1)
        stats["mean_frac_time_above_threshold"] = frac.mean()
        stats["std_frac_time_above_threshold"] = frac.std(ddof=1)
    return stats


def write_summary(
    label_a: str,
    label_b: str,
    metric_data: dict,
    path: Path,
    backpressure_threshold: float | None = None,
):
    """Write the plain mean/std summary CSV: one row per (group, metric),
    for every metric present in that group."""
    rows = []
    for label, key in ((label_a, "a"), (label_b, "b")):
        for metric, d in sorted(metric_data.items()):
            if not d[key]:
                continue
            threshold = backpressure_threshold if metric == "backpressure" else None
            stats = summarize_metric(d[key], d["bin"], d["t_end"], threshold)
            rows.append({"group": label, "metric": metric, **stats})
    columns = [
        "group",
        "metric",
        "n_runs",
        "mean",
        "std_across_runs",
        "mean_within_run_std",
    ]
    if backpressure_threshold is not None:
        columns += ["mean_frac_time_above_threshold", "std_frac_time_above_threshold"]
    pd.DataFrame(rows, columns=columns).to_csv(path, index=False)
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
                label=label_a,
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
                label=label_b,
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
                label=label_a,
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
                label=label_b,
            )
            ax.fill_between(
                grid_b, mean_b - std_b, mean_b + std_b, color=COLOR_B, alpha=0.2
            )

    ax.set_xlabel("Time since run start (s)")
    ax.set_ylabel(y_label_for_metric(metric))
    handles, labels = ax.get_legend_handles_labels()
    order = [labels.index(label_b), labels.index(label_a)]
    ax.legend([handles[i] for i in order], [labels[i] for i in order], fontsize=8)


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
    parser.add_argument(
        "--backpressure-threshold",
        type=float,
        default=None,
        help="If given, the summary CSV also reports, for the 'backpressure' metric, the "
        "mean and std (across runs) of the fraction of time spent above this threshold.",
    )
    parser.add_argument("--label-a", default=None, help="Legend label for group A")
    parser.add_argument("--label-b", default=None, help="Legend label for group B")
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

    label_a = args.label_a or args.pattern_a.split("_", 1)[-1]
    label_b = args.label_b or args.pattern_b.split("_", 1)[-1]

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

    # Per metric: every run's series plus the shared time grid. The end of
    # the grid is the longest run across BOTH groups; shorter runs are
    # extended to it by holding their last value.
    metric_data = {}
    for metric in sorted(metrics_a | metrics_b):
        series_a = run_series_list(runs_a, metric)
        series_b = run_series_list(runs_b, metric)
        grid = metric_time_grid([series_a, series_b], args.bin_seconds)
        if grid is None:
            continue
        bin_s, t_end = grid
        metric_data[metric] = {
            "a": series_a,
            "b": series_b,
            "bin": bin_s,
            "t_end": t_end,
        }
        short_a = sum(s["t_seconds"].max() < t_end - bin_s / 2 for s in series_a)
        short_b = sum(s["t_seconds"].max() < t_end - bin_s / 2 for s in series_b)
        if short_a or short_b:
            print(
                f"Extrapolating {metric} to {t_end:.0f}s (hold last value): "
                f"{short_a}/{len(series_a)} run(s) in A, {short_b}/{len(series_b)} run(s) in B"
            )

    # Plain mean/std table for every metric in each group.
    write_summary(
        label_a,
        label_b,
        metric_data,
        out_dir / f"{category_prefix}_{dir_prefix}_summary.csv",
        args.backpressure_threshold,
    )

    # Individual plots
    metric_aggs = {}
    for metric in shared_metrics:
        d = metric_data[metric]
        agg_a = group_mean_std(d["a"], d["bin"], d["t_end"])
        agg_b = group_mean_std(d["b"], d["bin"], d["t_end"])
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
    axes = np.atleast_1d(axes).flatten()

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
