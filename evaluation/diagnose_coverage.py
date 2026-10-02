#!/usr/bin/env python3
"""
diagnose_coverage.py

For a group of run parquet files, report -- per metric -- how long
(in seconds since that run's own start) each run's data for that metric
lasts, and flag the run that is the shortest (the one that determines
`max_common_t` / where the comparison line gets cut off in
compare_groups.py).

Usage
-----
    python diagnose_coverage.py DIRECTORY PATTERN [--metric METRIC_NAME]

Example
-------
    python diagnose_coverage.py ./runs "PRED_UpStream"
    python diagnose_coverage.py ./runs "PRED_UpStream" --metric task_manager_cpu
"""

import argparse
from pathlib import Path

import pandas as pd


def load_run(path: Path) -> pd.DataFrame:
    df = pd.read_parquet(path)
    df = df.sort_values("timestamp").copy()
    t0 = df["timestamp"].min()
    df["t_seconds"] = (df["timestamp"] - t0).dt.total_seconds()
    return df


def main():
    parser = argparse.ArgumentParser(
        description="Report per-run metric coverage (in seconds) within a group."
    )
    parser.add_argument("directory")
    parser.add_argument("pattern")
    parser.add_argument(
        "--metric", default=None, help="Only report this metric (default: all metrics)"
    )
    args = parser.parse_args()

    directory = Path(args.directory)
    files = sorted(p for p in directory.glob("*.parquet") if args.pattern in p.name)
    if not files:
        raise SystemExit(
            f"No .parquet files in {directory} contain pattern '{args.pattern}'"
        )

    runs = {p.name: load_run(p) for p in files}

    all_metrics = sorted(
        set().union(*(set(df["metric"].unique()) for df in runs.values()))
    )
    metrics = [args.metric] if args.metric else all_metrics

    for metric in metrics:
        rows = []
        for name, df in runs.items():
            sub = df[df["metric"] == metric]
            if sub.empty:
                rows.append((name, None, 0))
            else:
                rows.append((name, sub["t_seconds"].max(), len(sub)))

        present = [r for r in rows if r[1] is not None]
        if not present:
            continue

        min_cov = min(r[1] for r in present)
        print(f"\n=== {metric} ===")
        for name, cov, n in sorted(
            rows, key=lambda r: (r[1] is None, r[1] if r[1] is not None else -1)
        ):
            if cov is None:
                flag = "  (metric missing)"
            elif cov == min_cov:
                flag = "  <-- SHORTEST (sets the group cutoff)"
            else:
                flag = ""
            cov_str = f"{cov:8.1f}s" if cov is not None else "     n/a "
            print(f"  {name:45s} {cov_str}  ({n:5d} samples){flag}")


if __name__ == "__main__":
    main()
