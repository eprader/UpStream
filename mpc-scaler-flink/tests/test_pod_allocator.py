from typing import List

import pytest

from mpc_scaler_flink.deployment import Deployment
from mpc_scaler_flink.pod_allocator import PodAllocator

# Assuming Pod and PodAllocator classes are already imported


@pytest.mark.parametrize(
    "value_to_allocate, expected_allocation",
    [
        (17, [1, 0, 1]),
        (16, [1, 0, 0]),
        (15, [1, 0, 0]),
        (14, [1, 0, 0]),
        (13, [1, 0, 0]),
        (12, [0, 1, 1]),
        (11, [0, 1, 1]),
        (10, [0, 1, 1]),
        (9, [0, 1, 1]),
        (8, [0, 1, 0]),
        (7, [0, 1, 0]),
        (6, [0, 1, 0]),
        (5, [0, 0, 2]),
    ],
)
def test_allocate_pods_with_utilisation_0_8(value_to_allocate, expected_allocation):
    pods: List[Deployment] = [
        Deployment(capacity, 0) for capacity in ["Small", "Medium", "Large"]
    ]
    allocator: PodAllocator = PodAllocator(0.8)

    result = allocator.allocate_pods(value_to_allocate, pods)

    sorted_result: List[Deployment] = sorted(
        result, key=lambda p: p.number_of_taskslots.value, reverse=True
    )
    result_replica_counts = [pod.replica_count for pod in sorted_result]

    assert result_replica_counts == expected_allocation


@pytest.mark.parametrize(
    "value_to_allocate, expected_allocation",
    [
        (17, [1, 0, 1]),
        (16, [1, 0, 0]),
        (15, [0, 1, 2]),
        (14, [0, 1, 2]),
        (13, [0, 1, 2]),
        (12, [0, 1, 1]),
        (11, [0, 1, 1]),
        (10, [0, 1, 1]),
        (9, [0, 1, 1]),
        (8, [0, 1, 0]),
        (7, [0, 0, 2]),
        (6, [0, 0, 2]),
        (5, [0, 0, 2]),
    ],
)
def test_allocate_pods_with_utilisation_1(value_to_allocate, expected_allocation):
    pods: List[Deployment] = [
        Deployment(capacity, 0) for capacity in ["Small", "Medium", "Large"]
    ]
    allocator: PodAllocator = PodAllocator(1)

    result = allocator.allocate_pods(value_to_allocate, pods)

    sorted_result: List[Deployment] = sorted(
        result, key=lambda p: p.number_of_taskslots.value, reverse=True
    )
    result_replica_counts = [pod.replica_count for pod in sorted_result]

    assert result_replica_counts == expected_allocation
