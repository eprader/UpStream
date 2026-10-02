from collections import defaultdict

INPUT_FILE = "riot_events_TAXI_original_period.csv"
OUTPUT_FILE = "riot_events_TAXI_15min_normalized.csv"
TARGET_DURATION_MS = 15 * 60 * 1000  # 15 minutes in milliseconds
MAX_EPS = 10000  # Target peak events per second


def normalize_dataset():
    events = []
    print("Reading input dataset...")
    with open(INPUT_FILE, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or "|" not in line:
                continue
            ts_str, json_payload = line.split("|", 1)
            ts = int(ts_str.strip())
            events.append((ts, json_payload.strip()))

    if not events:
        print("No valid events found.")
        return

    events.sort(key=lambda x: x[0])
    orig_start = events[0][0]
    orig_end = events[-1][0]
    orig_duration = max(1, orig_end - orig_start)
    print(
        f"Original dataset: {len(events)} events over {orig_duration / 1000:.2f} seconds."
    )

    # 2. Time-compression: Map timestamps linearly to [0, TARGET_DURATION_MS]
    scaled_events = []
    bucket_counts = defaultdict(int)
    for orig_ts, payload in events:
        new_ts = int(((orig_ts - orig_start) / orig_duration) * TARGET_DURATION_MS)
        sec_bucket = new_ts // 1000
        bucket_counts[sec_bucket] += 1
        scaled_events.append((new_ts, sec_bucket, payload))

    max_orig_eps = max(bucket_counts.values()) if bucket_counts else 1
    print(f"Peak throughput in scaled timeline before scaling: {max_orig_eps} EPS")

    # 3. Scale every bucket by the SAME factor so peak bucket hits MAX_EPS.
    # No longer capped at 1.0 -> allows upsampling (duplication) as well as downsampling.
    scale_factor = MAX_EPS / max_orig_eps
    print(f"Scale factor: {scale_factor:.4f}x (peak {max_orig_eps} -> {MAX_EPS})")

    bucket_records = defaultdict(list)
    for new_ts, sec_bucket, payload in scaled_events:
        bucket_records[sec_bucket].append((new_ts, payload))

    final_events = []
    for sec_bucket, records in bucket_records.items():
        n = len(records)
        target_count = int(round(n * scale_factor))
        if target_count == 0 and n > 0:
            target_count = 1

        if target_count < n:
            # Downsample: uniformly pick a subset
            step = n / target_count
            sampled_records = [records[int(i * step)] for i in range(target_count)]
        elif target_count > n:
            # Upsample: duplicate existing events evenly to reach target_count,
            # cycling through the bucket's records to preserve their relative order/pattern.
            step = n / target_count
            sampled_records = [records[int(i * step) % n] for i in range(target_count)]
        else:
            sampled_records = records

        final_events.extend(sampled_records)

    final_events.sort(key=lambda x: x[0])

    print(f"Writing {len(final_events)} normalized records to '{OUTPUT_FILE}'...")
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        for new_ts, payload in final_events:
            f.write(f"{new_ts}| {payload}\n")

    print("Normalization complete.")


if __name__ == "__main__":
    normalize_dataset()
