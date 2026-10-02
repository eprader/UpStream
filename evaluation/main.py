import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

import pandas as pd
from prometheus_api_client import PrometheusConnect

prometheus_queries = {
    "busy_time": "avg(flink_taskmanager_job_task_busyTimeMsPerSecond) / 1000",
    "idle_time": "avg(flink_taskmanager_job_task_idleTimeMsPerSecond) / 1000",
    "backpressure": "avg(flink_taskmanager_job_task_backPressuredTimeMsPerSecond) / 1000",
    "queue_length": "avg(flink_taskmanager_job_task_Shuffle_Netty_Input_Buffers_inputQueueLength)",
    "kafka_senml_source": "sum(kafka_server_BrokerTopicMetrics_OneMinuteRate{name='MessagesInPerSec', topic='senml-source'})",
    "kafka_senml_cleaned": "sum(kafka_server_BrokerTopicMetrics_OneMinuteRate{name='MessagesInPerSec', topic='senml-cleaned'})",
    "number_of_replicas": "kube_deployment_spec_replicas{deployment='flink-application-cluster-taskmanager'}",
    "task_manager_cpu": (
        "(sum(rate(container_cpu_usage_seconds_total{namespace='default', "
        "pod=~'flink-application-cluster-taskmanager-.*', container!=''}[2m])) by (pod) "
        "/ ignoring(resource) sum(kube_pod_container_resource_requests{namespace='default', "
        "pod=~'flink-application-cluster-taskmanager-.*', resource='cpu'}) by (pod)) * 100"
    ),
    "task_manager_memory": (
        "(sum(container_memory_working_set_bytes{namespace='default', "
        "pod=~'flink-application-cluster-taskmanager-.*', container!=''}) by (pod) "
        "/ ignoring(resource) sum(kube_pod_container_resource_requests{namespace='default', "
        "pod=~'flink-application-cluster-taskmanager-.*', resource='memory'}) by (pod)) * 100"
    ),
    "mongodb_average_inserts_senml_cleaned": "rate(mongodb_top_insert_count{collection='senml-cleaned'}[10s])",
    "mongodb_average_inserts_plots": "rate(mongodb_top_insert_count{collection='plots'}[10s])",
}

DATETIME_FORMAT = "%Y-%m-%dT%H:%M:%S"


def parse_args():
    parser = argparse.ArgumentParser(
        description="Fetch Flink metrics from Prometheus and store as Parquet."
    )
    parser.add_argument("start", help=f"Start datetime, format: {DATETIME_FORMAT}")
    parser.add_argument("end", help=f"End datetime, format: {DATETIME_FORMAT}")
    parser.add_argument("output", help="Output path for the Parquet file")
    return parser.parse_args()


def fetch_metric(
    prometheus: PrometheusConnect, name: str, query: str, start: datetime, end: datetime
) -> pd.DataFrame:
    """Fetch a single metric as a range query and return a tidy DataFrame."""
    try:
        result = prometheus.custom_query_range(
            query=query,
            start_time=start,
            end_time=end,
            step="15s",
        )
    except Exception as e:
        print(f"  [error] Query failed for '{name}': {e}")
        return pd.DataFrame()

    if not result:
        print(f"  [warn] No data returned for '{name}'")
        return pd.DataFrame()

    rows = []
    for series in result:
        labels = series.get("metric", {})
        # Extract specific labels safely to avoid dynamic schema explosions in Parquet
        pod_name = labels.get("pod", "cluster")

        for ts, value in series["values"]:
            rows.append(
                {
                    "metric": name,
                    "timestamp": pd.Timestamp(ts, unit="s", tz="UTC"),
                    "value": float(value),
                    "pod": pod_name,
                }
            )

    return pd.DataFrame(rows)


def main():
    args = parse_args()

    start_dt = datetime.strptime(args.start, DATETIME_FORMAT)
    end_dt = datetime.strptime(args.end, DATETIME_FORMAT)

    if end_dt <= start_dt:
        raise ValueError("end datetime must be after start datetime")

    print(f"Querying Prometheus from {start_dt} to {end_dt}")
    prometheus = PrometheusConnect(url="http://localhost:9090", disable_ssl=True)

    frames = []
    # Execute Prometheus queries in parallel for performance
    with ThreadPoolExecutor(max_workers=5) as executor:
        future_to_metric = {
            executor.submit(
                fetch_metric, prometheus, name, query, start_dt, end_dt
            ): name
            for name, query in prometheus_queries.items()
        }
        for future in as_completed(future_to_metric):
            name = future_to_metric[future]
            df = future.result()
            if not df.empty:
                print(f"  Fetched: {name} ({len(df)} rows)")
                frames.append(df)

    if not frames:
        print("No data fetched. Parquet file not written.")
        return

    combined = pd.concat(frames, ignore_index=True)

    # Cast timestamp to pyarrow compatible datetime
    combined["timestamp"] = pd.to_datetime(combined["timestamp"])

    combined.to_parquet(args.output, index=False)
    print(f"\nSuccessfully saved {len(combined)} rows to {args.output}")


if __name__ == "__main__":
    main()
