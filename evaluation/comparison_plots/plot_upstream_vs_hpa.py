#!/usr/bin/env python3
"""
plot_upstream_vs_hpa.py

Reads the "*_summary.csv" evaluation files (one per pipeline x load-profile
combination, each containing an 'UpStream' and an 'HPA' group) and produces
grouped bar plots comparing UpStream against HPA, across pipelines (ETL,
STATS, PRED) and load profiles (static_10000, TAXI_normalized,
exponential_plateau, ...).

The comparison is organised around what each scaler is actually trying to
achieve:
  - TARGET_METRICS: the metrics each scaler is directly (or indirectly)
    trying to hold at a setpoint -- busy/idle time (UpStream's own MPC
    targets) and CPU/memory utilisation (HPA's target). These are the
    primary comparison: how well does each scaler track the thing it is
    supposed to be tracking?
  - INDIRECT_METRICS: backpressure, a proxy for under-provisioning that
    neither scaler targets directly but both should keep low.
  - Pipeline-specific payoff plots: better target tracking only matters if
    it translates into real output. ETL has a directly-measured Kafka output
    metric; ETL and STATS both write to MongoDB, so MONGODB_METRICS plots
    the average MongoDB insert rate for those two pipelines. (PRED is out of
    scope here since its output is not currently collected.)

Usage:
    python plot_upstream_vs_hpa.py [--input-dir DIR] [--output-dir DIR]

By default it looks for *_summary.csv files in the current directory and
writes PNGs to ./plots/.
"""

import argparse
import re
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd

# Primary comparison: metrics with a defined target value that a scaler is
# (directly or indirectly) trying to hold steady. busy_time/idle_time are
# UpStream's own MPC targets; task_manager_cpu/memory are HPA's target.
# Reporting all four for both scalers is the cross-check: it shows not just
# "did each scaler hit its own target" but also "what did the other
# scaler's target metric look like under this scaler's control".
TARGET_METRICS = [
    # metric name in CSV, value column, std column, y-axis label, title
    (
        "busy_time",
        "steady_mean",
        "steady_std_across_runs",
        "busy time (fraction)",
        "Busy Time (UpStream target)",
    ),
    (
        "idle_time",
        "steady_mean",
        "steady_std_across_runs",
        "idle time (fraction)",
        "Idle Time (UpStream target)",
    ),
    (
        "task_manager_cpu",
        "steady_mean",
        "steady_std_across_runs",
        "CPU utilisation (%)",
        "Task Manager CPU Utilisation (HPA target)",
    ),
    (
        "task_manager_memory",
        "steady_mean",
        "steady_std_across_runs",
        "memory utilisation (%)",
        "Task Manager Memory Utilisation (HPA target)",
    ),
]

# Secondary/indirect: not a target either scaler optimises for directly, but
# a proxy signal for under-provisioning that both should keep low.
INDIRECT_METRICS = [
    # Mean backpressure (averaged over the steady-state window per run, std
    # across runs). Peak/spike backpressure is intentionally not plotted.
    # NOTE: requires the summary CSV to contain steady_mean /
    # steady_std_across_runs for the 'backpressure' metric; if those are
    # empty the plot is skipped with a message.
    (
        "backpressure",
        "steady_mean",
        "steady_std_across_runs",
        "mean backpressure",
        "Mean Backpressure (UpStream gate target)",
    ),
    (
        "backpressure",
        "mean_frac_time_above_threshold",
        "std_frac_time_above_threshold",
        "fraction of time backpressured (fraction)",
        "Time Spent Backpressured",
    ),
]

# Supporting cost/responsiveness context, shown after the target comparison.
COST_METRICS = [
    (
        "task_manager_task_slots",
        "steady_mean",
        "steady_std_across_runs",
        "mean task slots provisioned",
        "Provisioned Task Slots",
    ),
    (
        "task_manager_task_slots__drain_time_s",
        "mean_drain_seconds",
        "std_drain_seconds",
        "seconds to drain queue",
        "Backlog Drain Time",
    ),
]

# MongoDB insert rate: real pipeline output for ETL and STATS. The metric
# name differs per pipeline (ETL writes senml_cleaned, STATS writes plots),
# so there is one spec per metric name. make_per_pipeline_plots() skips any
# (metric, pipeline) combination with no data, so each spec automatically
# only produces plots for the pipeline it belongs to.
MONGODB_METRICS = [
    (
        "mongodb_average_inserts_senml_cleaned",  # ETL
        "steady_mean",
        "steady_std_across_runs",
        "average MongoDB inserts (senml-cleaned)",
        "MongoDB Inserts (senml-cleaned)",
    ),
    (
        "mongodb_average_inserts_plots",  # STATS
        "steady_mean",
        "steady_std_across_runs",
        "average MongoDB inserts (plots)",
        "MongoDB Inserts (plots)",
    ),
]

METRICS_WITH_TARGET_LINE = {
    "busy_time",
    "idle_time",
    "task_manager_cpu",
    "task_manager_memory",
}

# The ETL-specific "does better tracking pay off in throughput" plot. ETL has
# a directly measured, unambiguous Kafka output metric here; STATS's Kafka
# output is not the focus and PRED's Kafka output topic is not currently
# collected.
ETL_THROUGHPUT_METRIC = (
    "kafka_senml_cleaned",
    "steady_mean",
    "steady_std_across_runs",
    "messages/sec written to Kafka (senml-cleaned)",
)

# Consistent colours for the two scalers across all plots.

COLORS = {"UpStream": "#013261", "HPA": "#f39301"}


KNOWN_PIPELINES = ("ETL", "STATS", "PRED")


def _fmt_value(v: float) -> str:
    """Plain, readable number for a bar label: thousands separators for
    large values (4,499 rather than 4.5e+03), 3 significant digits else."""
    return f"{v:,.0f}" if abs(v) >= 100 else f"{v:.3g}"


def parse_filename(path: Path):
    """Extract pipeline name and load profile from a summary CSV filename.

    Handles an optional leading numeric upload prefix (e.g. a timestamp)
    and a trailing "_summary" suffix, e.g.:
        1789670140639_ETL_exponential_plateau_summary.csv -> ("ETL", "exponential_plateau")
        PRED_static_10000_summary.csv                     -> ("PRED", "static_10000")
    """
    stem = re.sub(r"_summary$", "", path.stem)
    match = re.search(r"(?:^|_)(" + "|".join(KNOWN_PIPELINES) + r")_(.+)$", stem)
    if not match:
        raise ValueError(
            f"Could not determine pipeline/load profile from filename: {path.name}"
        )
    pipeline, load_profile = match.group(1), match.group(2)
    return pipeline, load_profile


def load_data(input_dir: Path) -> pd.DataFrame:
    files = sorted(input_dir.glob("*_summary.csv"))
    if not files:
        raise SystemExit(f"No *_summary.csv files found in {input_dir}")

    rows = []
    for f in files:
        pipeline, load_profile = parse_filename(f)
        df = pd.read_csv(f)
        # group looks like "'ETL_UpStream' group" -> extract just the scaler name
        # compare_groups.py summaries report plain "mean" / "std_across_runs"
        # columns; treat them as the steady_* columns used by the plots so
        # both summary formats work (existing steady_* values are kept).
        for plain, steady in (
            ("mean", "steady_mean"),
            ("std_across_runs", "steady_std_across_runs"),
        ):
            if plain in df.columns:
                df[steady] = (
                    df[steady].combine_first(df[plain])
                    if steady in df.columns
                    else df[plain]
                )
        df["scaler"] = df["group"].str.extract(r"(UpStream|HPA)")
        df["pipeline"] = pipeline
        df["load_profile"] = load_profile
        rows.append(df)

    return pd.concat(rows, ignore_index=True)


def _draw_bar_chart(
    mean_pivot,
    std_pivot,
    index_order,
    ylabel,
    title,
    target_value,
    out_path,
    summary_rows,
    pipeline,
    metric_name,
    value_field,
):
    """Shared bar-chart drawing logic: grouped bars per scaler, +/- std error
    bars, value labels, optional target reference line. Also appends rows to
    `summary_rows` for the combined summary CSV."""
    mean_pivot = mean_pivot.reindex(index_order)
    if mean_pivot.dropna(how="all").empty:
        return False
    if std_pivot is not None:
        std_pivot = std_pivot.reindex(index_order)

    scalers = [s for s in ("UpStream", "HPA") if s in mean_pivot.columns]
    x = range(len(mean_pivot.index))
    width = 0.8 / max(len(scalers), 1)

    fig, ax = plt.subplots(figsize=(max(7, 1.8 * len(mean_pivot.index)), 4.5))
    for i, scaler in enumerate(scalers):
        offset = (i - (len(scalers) - 1) / 2) * width
        means = mean_pivot[scaler].values
        errs = std_pivot[scaler].values if std_pivot is not None else None
        bar_x = [xi + offset for xi in x]
        ax.bar(
            bar_x,
            means,
            width=width,
            yerr=errs,
            capsize=3,
            error_kw=dict(ecolor="black", elinewidth=1.5, capthick=1.5),
            label=scaler,
            color=COLORS.get(scaler, None),
        )
        # Value label: placed ABOVE the top of the error bar (not just above
        # the bar) so the whisker never runs through the text, on a light
        # background so grid lines don't hurt readability either.
        err_for_label = errs if errs is not None else [None] * len(means)
        for xi, m, e in zip(bar_x, means, err_for_label):
            if pd.notna(m):
                e = 0.0 if (e is None or pd.isna(e)) else abs(e)
                ax.annotate(
                    _fmt_value(m),
                    (xi, max(m, m + e)),
                    textcoords="offset points",
                    xytext=(0, 4),
                    ha="center",
                    va="bottom",
                    fontsize=8,
                    zorder=5,
                    bbox=dict(
                        boxstyle="round,pad=0.15", fc="white", ec="none", alpha=0.85
                    ),
                )
        for combo_label, m, s in zip(
            mean_pivot.index, means, errs if errs is not None else [None] * len(means)
        ):
            label_pipeline = pipeline or combo_label.split("\n")[0]
            label_load = combo_label.split("\n")[-1]
            summary_rows.append(
                {
                    "pipeline": label_pipeline,
                    "load_profile": label_load,
                    "scaler": scaler,
                    "metric": metric_name,
                    "field": value_field,
                    "mean": m,
                    "std": s,
                }
            )

    if target_value is not None:
        ax.axhline(
            target_value,
            color="black",
            linestyle=":",
            linewidth=1.2,
            label=f"target ({target_value:g})",
        )

    # Headroom above the tallest error bar so the value labels are not clipped.
    lo, hi = ax.get_ylim()
    ax.set_ylim(lo, hi + 0.12 * (hi - lo))
    ax.set_xticks(list(x))
    ax.set_xticklabels(mean_pivot.index, fontsize=9)
    ax.set_ylabel(ylabel)
    ax.legend()
    ax.grid(axis="y", linestyle="--", alpha=0.4)
    fig.tight_layout()
    fig.savefig(out_path, dpi=200)
    plt.close(fig)
    print(f"wrote {out_path}")
    return True


def make_per_pipeline_plots(
    data: pd.DataFrame,
    output_dir: Path,
    metric_specs: list,
    summary_rows: list,
    use_target_line: bool,
):
    """One chart per (metric, pipeline) in `metric_specs`, with that
    pipeline's load profiles on the x-axis. Splitting by pipeline keeps
    each chart focused (no mixing of pipelines with different underlying
    workloads) while still comparing UpStream vs. HPA as mean +/- std
    bars within each load profile. Combinations with no data are skipped."""
    pipelines = sorted(data["pipeline"].unique())
    warned_missing_std = {}
    for metric_name, value_field, std_field, ylabel, title in metric_specs:
        sub_metric = data[data["metric"] == metric_name]
        if sub_metric.empty:
            continue
        if value_field not in sub_metric.columns:
            print(
                f"[skip] {metric_name}: no '{value_field}' column in any summary CSV"
                + (
                    " (compare_groups.py only writes it when run with "
                    "--backpressure-threshold T)"
                    if value_field == "mean_frac_time_above_threshold"
                    else ""
                )
            )
            continue

        for pipeline in pipelines:
            sub = sub_metric[sub_metric["pipeline"] == pipeline]
            if sub.empty:
                continue
            if (
                std_field is not None
                and std_field not in sub.columns
                and not warned_missing_std.get((metric_name, value_field))
            ):
                warned_missing_std[(metric_name, value_field)] = True
                print(
                    f"[note] {metric_name} / {value_field}: no '{std_field}' column in "
                    f"the summary CSVs, so these plots have no error bars"
                )
            if sub[value_field].notna().sum() == 0:
                print(
                    f"[skip] {pipeline} {metric_name}: column '{value_field}' "
                    f"is empty in the summary CSVs"
                )
                continue
            load_profiles = sorted(sub["load_profile"].unique())

            mean_pivot = sub.pivot_table(
                index="load_profile", columns="scaler", values=value_field
            )
            std_pivot = None
            if std_field is not None and std_field in sub.columns:
                std_pivot = sub.pivot_table(
                    index="load_profile", columns="scaler", values=std_field
                )

            target_value = None
            if (
                use_target_line
                and metric_name in METRICS_WITH_TARGET_LINE
                and "target" in sub.columns
            ):
                targets = sub["target"].dropna().unique()
                if len(targets) == 1:
                    target_value = targets[0]

            out_path = output_dir / f"{pipeline}_{metric_name}__{value_field}.png"
            _draw_bar_chart(
                mean_pivot,
                std_pivot,
                load_profiles,
                ylabel,
                f"{pipeline} -- {title}: UpStream vs. HPA",
                target_value,
                out_path,
                summary_rows,
                pipeline,
                metric_name,
                value_field,
            )


def make_etl_throughput_plot(data: pd.DataFrame, output_dir: Path, summary_rows: list):
    """The payoff plot: shows that UpStream's better target tracking (see
    the TARGET_METRICS plots) translates into materially higher real
    throughput, using ETL as the one pipeline with a directly measured,
    unambiguous Kafka output metric."""
    metric_name, value_field, std_field, ylabel = ETL_THROUGHPUT_METRIC
    sub = data[(data["pipeline"] == "ETL") & (data["metric"] == metric_name)]
    if sub.empty or value_field not in sub.columns:
        print(f"[skip] ETL throughput plot: no '{metric_name}' data found")
        return

    load_profiles = sorted(sub["load_profile"].unique())
    mean_pivot = sub.pivot_table(
        index="load_profile", columns="scaler", values=value_field
    )
    std_pivot = None
    if std_field and std_field in sub.columns:
        std_pivot = sub.pivot_table(
            index="load_profile", columns="scaler", values=std_field
        )

    out_path = output_dir / f"ETL_output__{metric_name}.png"
    _draw_bar_chart(
        mean_pivot,
        std_pivot,
        load_profiles,
        ylabel,
        "ETL Output Throughput: UpStream vs. HPA (does better tracking pay off?)",
        None,
        out_path,
        summary_rows,
        "ETL",
        metric_name,
        value_field,
    )


def make_plots(data: pd.DataFrame, output_dir: Path):
    output_dir.mkdir(parents=True, exist_ok=True)
    data["combo"] = data["pipeline"] + "\n" + data["load_profile"]

    summary_rows = []  # collected across all metrics, written out as one CSV at the end

    # 1. Primary comparison: target-tracking metrics, split per pipeline.
    make_per_pipeline_plots(
        data, output_dir, TARGET_METRICS, summary_rows, use_target_line=True
    )
    # 2. Indirect signal: backpressure, split per pipeline.
    make_per_pipeline_plots(
        data, output_dir, INDIRECT_METRICS, summary_rows, use_target_line=False
    )
    # 3. Supporting cost/responsiveness context, split per pipeline.
    make_per_pipeline_plots(
        data, output_dir, COST_METRICS, summary_rows, use_target_line=False
    )
    # 4. Payoff: does better target tracking translate to more throughput?
    #    Kafka output shown for ETL only (see ETL_THROUGHPUT_METRIC).
    make_etl_throughput_plot(data, output_dir, summary_rows)
    # 5. Payoff: average MongoDB inserts for ETL (senml_cleaned) and
    #    STATS (plots).
    make_per_pipeline_plots(
        data, output_dir, MONGODB_METRICS, summary_rows, use_target_line=False
    )

    summary = pd.DataFrame(summary_rows)
    summary_path = output_dir / "summary_table.csv"
    summary.to_csv(summary_path, index=False)
    print(f"wrote {summary_path}")

    # Print target-tracking accuracy directly to the console: how close was
    # each scaler's own target metric to its target, on average, across all
    # pipeline/load-profile combos it appears in.
    if "mean_abs_deviation_from_target" not in data.columns:
        return
    print(
        "\nTarget-tracking accuracy (mean absolute deviation from target, all pipelines/loads):"
    )
    targets_raw = data[
        data["metric"].isin(METRICS_WITH_TARGET_LINE)
        & data["mean_abs_deviation_from_target"].notna()
    ]
    if not targets_raw.empty:
        agg = (
            targets_raw.groupby(["metric", "scaler"])["mean_abs_deviation_from_target"]
            .mean()
            .reset_index()
        )
        for _, row in agg.sort_values(["metric", "scaler"]).iterrows():
            print(
                f"  {row['metric']:<20} {row['scaler']:<9} {row['mean_abs_deviation_from_target']:.4f}"
            )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--input-dir",
        type=Path,
        default=Path("."),
        help="Directory containing *_summary.csv files",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("plots"),
        help="Directory to write PNG plots to",
    )
    args = parser.parse_args()

    data = load_data(args.input_dir)
    make_plots(data, args.output_dir)


if __name__ == "__main__":
    main()
