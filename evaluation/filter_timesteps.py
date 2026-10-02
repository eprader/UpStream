import argparse
import os


def main():
    parser = argparse.ArgumentParser(
        description="Keep only the lines of a pipe-delimited "
        "`<timestamp>| <payload>` file whose timestamp falls within "
        "[start, end] (inclusive)."
    )
    parser.add_argument("target_file", help="Path to the CSV/senML file")
    parser.add_argument("start_ts", type=int, help="Start timestamp (inclusive)")
    parser.add_argument("end_ts", type=int, help="End timestamp (inclusive)")
    parser.add_argument(
        "--output",
        default=None,
        help="Output file path (default: <input>_<start>_<end>.csv)",
    )
    args = parser.parse_args()

    if args.output:
        output_file = args.output
    else:
        base, ext = os.path.splitext(args.target_file)
        output_file = f"{base}_{args.start_ts}_{args.end_ts}{ext}"

    kept = 0
    total = 0
    with (
        open(args.target_file, "r", encoding="utf-8") as infile,
        open(output_file, "w", encoding="utf-8") as outfile,
    ):
        for line in infile:
            total += 1
            ts_str = line.split("|", 1)[0].strip()
            try:
                ts = int(ts_str)
            except ValueError:
                # Not a numeric timestamp (e.g. a header row) — keep it as-is
                outfile.write(line)
                continue

            if args.start_ts <= ts <= args.end_ts:
                outfile.write(line)
                kept += 1

    print(f"Read {total} lines, kept {kept} within [{args.start_ts}, {args.end_ts}]")
    print(f"Wrote result to {output_file}")


if __name__ == "__main__":
    main()
