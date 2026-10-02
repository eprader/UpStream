import argparse
import os

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd


def load_timestamps(path: str, chunksize: int) -> np.ndarray:
    """Read every line's timestamp from a pipe-delimited
    `<timestamp>| <payload...>` file, ignoring the payload.

    Skips a header row automatically if the first field isn't numeric.
    Robust to malformed / non-numeric lines (skipped). Returns them in
    file order (not sorted, not aggregated).
    """
    parts = []
    for chunk in pd.read_csv(
        path,
        sep="|",
        header=None,
        usecols=[0],
        names=["ts"],
        dtype=str,
        chunksize=chunksize,
        engine="c",
        on_bad_lines="skip",
    ):
        ts = pd.to_numeric(chunk["ts"].str.strip(), errors="coerce").dropna()
        if len(ts):
            parts.append(ts.to_numpy(dtype=np.int64))
    if not parts:
        return np.array([], dtype=np.int64)
    return np.concatenate(parts)


def detect_unit(sample_max: int) -> str:
    """Guess the timestamp unit from magnitude."""
    if sample_max > 1e14:
        return "us"
    elif sample_max > 1e11:
        return "ms"
    elif sample_max > 1e8:
        return "s"
    else:
        return "relative_ms"  # small numbers -> not a real epoch, treat as ms offset


def main():
    parser = argparse.ArgumentParser(
        description="Plot every individual event timestamp from a pipe-delimited "
        "timestamp|payload file, ignoring the payload. No bucketing/binning — "
        "one point per line."
    )
    parser.add_argument("target_file", help="Path to the CSV/senML file")
    parser.add_argument(
        "--unit",
        choices=["auto", "s", "ms", "us", "relative_ms"],
        default="auto",
        help="Timestamp unit. 'auto' inspects the magnitude of the values.",
    )
    parser.add_argument(
        "--chunksize",
        type=int,
        default=1_000_000,
        help="Rows per chunk while reading the file",
    )
    parser.add_argument(
        "--start-ts",
        type=int,
        default=None,
        help="Only include events with timestamp >= this raw value.",
    )
    parser.add_argument(
        "--end-ts",
        type=int,
        default=None,
        help="Only include events with timestamp <= this raw value.",
    )
    args = parser.parse_args()

    timestamps = load_timestamps(args.target_file, args.chunksize)
    if timestamps.size == 0:
        print("No valid timestamps found in file.")
        return

    total_count = len(timestamps)
    print(f"Total events read: {total_count}")

    if args.start_ts is not None or args.end_ts is not None:
        mask = np.ones(total_count, dtype=bool)
        if args.start_ts is not None:
            mask &= timestamps >= args.start_ts
        if args.end_ts is not None:
            mask &= timestamps <= args.end_ts
        timestamps = timestamps[mask]
        print(
            f"Filtered to range [{args.start_ts}, {args.end_ts}]: "
            f"{len(timestamps)} events"
        )
        if timestamps.size == 0:
            print("No events left after filtering.")
            return

    ts_min, ts_max = timestamps.min(), timestamps.max()
    print(f"Raw timestamp range: {ts_min} -> {ts_max}")

    unit = args.unit if args.unit != "auto" else detect_unit(ts_max)
    print(f"Detected/using unit: {unit}")

    # x = each *unique* timestamp value that appears in the file,
    # y = how many events share that exact timestamp value.
    # This is NOT arbitrary-width binning — it's an exact groupby/count on
    # the raw timestamp values themselves, so no time range gets merged
    # together unless multiple events truly share the same timestamp.
    unique_ts, counts = np.unique(timestamps, return_counts=True)

    if unit in ("s", "ms", "us"):
        x = pd.to_datetime(unique_ts, unit=unit)
        x_label = "Time"
        print(
            f"Date range: {pd.to_datetime(ts_min, unit=unit)} -> "
            f"{pd.to_datetime(ts_max, unit=unit)}"
        )
    else:
        x = unique_ts / 1000.0
        x_label = "Timestamp [s, relative]"

    plt.figure(figsize=(12, 5))
    plt.plot(x, counts, color="steelblue", linewidth=1)
    plt.xlabel(x_label)
    plt.xlabel(x_label)
    plt.ylabel("Number of events at that timestamp")
    plt.ylim(bottom=0, top=counts.max() + 1)
    plt.title(
        f"Events per exact timestamp ({len(timestamps)} events, "
        f"{len(unique_ts)} unique timestamps)"
    )
    if unit in ("s", "ms", "us"):
        plt.gcf().autofmt_xdate()
    plt.tight_layout()

    base, _ = os.path.splitext(args.target_file)
    suffix = (
        f"_{args.start_ts}_{args.end_ts}"
        if (args.start_ts is not None or args.end_ts is not None)
        else ""
    )
    plot_file = f"{base}_timestamps_counts{suffix}.png"
    plt.savefig(plot_file, dpi=150, bbox_inches="tight")
    plt.close()
    print(f"Saved plot at {plot_file}")


if __name__ == "__main__":
    main()
