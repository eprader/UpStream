import logging
import time
from datetime import datetime
from math import ceil
from typing import List

import numpy as np
from kubernetes import client, config
from prometheus_api_client import PrometheusConnect

from mpc_scaler_flink.mpc_controller import MPCController

from .deployment import Deployment
from .pod_allocator import PodAllocator

logging.basicConfig(
    level=logging.WARNING,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# TODO: Replace with kubernetes svc url
prometheus = PrometheusConnect(url="http://localhost:9090", disable_ssl=True)


def get_flink_metric(query_str: str) -> float:
    try:
        result = prometheus.custom_query(query=query_str)
        if not result:
            logger.warning("No data returned for query: %s", query_str)
            return 0.0
        # Prometheus returns a list of dicts; for 'avg(...)' there is only 1 series.
        return float(result[0]["value"][1])
    except Exception as e:
        logger.error("Error fetching metric for query '%s': %s", query_str, e)
        return 0.0


def get_taskmanager_deployments():

    name_to_taskslots_map = {
        "flink-application-cluster-taskmanager": 2,
        "flink-application-cluster-taskmanager-medium": 8,
        "flink-application-cluster-taskmanager-large": 16,
    }

    config.load_kube_config()

    apps_v1 = client.AppsV1Api()

    logger.info("Fetching Task Manager deployments")

    k8s_deployments = apps_v1.list_namespaced_deployment(
        namespace="default", label_selector="component=taskmanager"
    )

    return [
        Deployment(
            name=d.metadata.name,
            number_of_taskslots=name_to_taskslots_map[d.metadata.name],
            replica_count=d.spec.replicas,
        )
        for d in k8s_deployments.items
    ]


def scale_deployment(deployment_name: str, replicas: int, namespace: str = "default"):
    config.load_kube_config()
    apps_v1 = client.AppsV1Api()
    try:
        response = apps_v1.patch_namespaced_deployment_scale(
            name=deployment_name,
            namespace=namespace,
            body={"spec": {"replicas": replicas}},
        )
        logger.info("Deployment '%s' scaled to %d replicas.", deployment_name, replicas)
        return response
    except client.ApiException as e:
        logger.error("Error scaling deployment '%s': %s", deployment_name, e)


prometheus_queries = {
    "busy_time": "avg(flink_taskmanager_job_task_busyTimeMsPerSecond) / 1000",
    "idle_time": "avg(flink_taskmanager_job_task_idleTimeMsPerSecond) / 1000",
    "backpressure": "avg(flink_taskmanager_job_task_backPressuredTimeMsPerSecond) / 1000",
}


def _fetch_metrics_array() -> np.ndarray:
    """Query Prometheus and return a ``[busy, idle, backpressure]`` float64 array."""
    metrics = {name: get_flink_metric(q) for name, q in prometheus_queries.items()}
    logger.debug(
        "Fetched metrics: busy=%.4f idle=%.4f backpressure=%.4f",
        metrics["busy_time"],
        metrics["idle_time"],
        metrics["backpressure"],
    )
    return np.array(
        [metrics["busy_time"], metrics["idle_time"], metrics["backpressure"]],
        dtype=np.float64,
    )


def main():
    sync_period_sec: int = 120

    initial_metrics = _fetch_metrics_array()
    logger.info(
        f"Initial metrics at {datetime.now().isoformat()}: busy=%.4f idle=%.4f backpressure=%.4f",
        *initial_metrics,
    )

    controller = MPCController()
    controller.initial_measurement(initial_metrics)

    allocator: PodAllocator = PodAllocator(utilisation_factor=0.8)

    taskmanager_deployments = get_taskmanager_deployments()
    logger.info("Initial deployment configurations: %s", taskmanager_deployments)

    iteration = 0
    while True:
        logger.debug(f"Sleeping for {sync_period_sec} seconds")
        time.sleep(sync_period_sec)
        iteration += 1
        logger.debug(f"--- Sync iteration {iteration} ---")

        metrics = _fetch_metrics_array()
        scaling_factor = controller.measurement_step(metrics)

        current_task_slot_count: int = sum(
            deployment.replica_count * deployment.number_of_taskslots
            for deployment in taskmanager_deployments
        )
        # NOTE:
        # We need to ensure the number of taskslots is at least 1.
        # Otherwise the job will halt.
        new_task_slot_count: int = ceil(current_task_slot_count * scaling_factor)

        if new_task_slot_count < 1:
            logger.warning(
                "The predicted new number of task slots is below 1, keeping one replica active."
            )

        new_allocation = allocator.allocate_pods(
            max(new_task_slot_count, 1), taskmanager_deployments
        )

        logger.info(
            f"Iteration {iteration} | scaling_factor={scaling_factor} | slots: {current_task_slot_count} → {new_task_slot_count}"
        )
        logger.info(f"New deployment configureation: {new_allocation}")

        for deployment in new_allocation:
            scale_deployment(
                deployment_name=deployment.name, replicas=deployment.replica_count
            )
