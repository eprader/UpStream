import logging
import time
from math import ceil

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
logger.setLevel(logging.INFO)

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

    logger.debug("Fetching Task Manager deployments")

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
    SYNC_PERIOD_SEC: int = 120
    BACKPRESSURE_THRESHOLD = 0.1

    initial_metrics = _fetch_metrics_array()
    logger.debug(
        "Initial metrics: busy=%.4f idle=%.4f backpressure=%.4f",
        *initial_metrics,
    )

    controller = MPCController()
    controller.initial_measurement(initial_metrics[:2])

    allocator: PodAllocator = PodAllocator(utilisation_factor=0.8)

    iteration = 0
    while True:
        if iteration > 0:
            logger.debug(f"Sleeping for {SYNC_PERIOD_SEC} seconds")
            time.sleep(SYNC_PERIOD_SEC)

        iteration += 1
        logger.debug(f"--- Sync iteration {iteration} ---")

        busy_time, idle_time, backpressure = _fetch_metrics_array()

        taskmanager_deployments = get_taskmanager_deployments()
        logger.info("Deployment configurations: %s", taskmanager_deployments)

        current_task_slot_count: int = sum(
            deployment.replica_count * deployment.number_of_taskslots
            for deployment in taskmanager_deployments
        )

        # Hard override: any backpressure immediate scale up proportional to the metric
        # Backpressure will be between 0 and 100%
        if backpressure > BACKPRESSURE_THRESHOLD:
            scaling_factor = 1.0 + backpressure
            new_task_slot_count = ceil(current_task_slot_count * scaling_factor)
            logger.info(
                "Backpressure override: backpressure=%.4f → scale_factor=%.4f",
                backpressure,
                scaling_factor,
            )
        else:
            # No backpressure — let MPC balance busy/idle
            scaling_factor = controller.measurement_step(
                np.array([busy_time, idle_time], dtype=np.float64)
            )
            new_task_slot_count = round(current_task_slot_count * scaling_factor)

        new_task_slot_count = max(new_task_slot_count, 1)

        new_allocation = allocator.allocate_pods(
            max(new_task_slot_count, 1), taskmanager_deployments
        )

        logger.info(
            f"Iteration {iteration} | scaling_factor={scaling_factor} | slots: {current_task_slot_count} → {new_task_slot_count}"
        )
        logger.info(f"New deployment configuration: {new_allocation}")

        for deployment in new_allocation:
            scale_deployment(
                deployment_name=deployment.name, replicas=deployment.replica_count
            )
