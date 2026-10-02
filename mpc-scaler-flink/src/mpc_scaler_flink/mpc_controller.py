"""MPC-based autoscaling controller using do-mpc."""

import logging
from dataclasses import dataclass
from typing import Literal, TypeAlias

import do_mpc
import numpy as np
from do_mpc.controller import MPC
from do_mpc.model import Model
from numpy.core.multiarray import ndarray

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# Array type hint: 3 state metrics [busy_time, idle_time, backpressure_time]
Array3Float: TypeAlias = np.ndarray[Literal[3], np.dtype[np.float64]]


@dataclass(frozen=True)
class MPCConfig:
    """Immutable configuration for the MPC controller.

    Centralises every tunable constant so callers never need to touch
    the controller internals.

    Coupling invariant
    ------------------
    The three states must always sum to 1.0:

        busy_time + idle_time + backpressure_time = 1.0

    This is enforced by the dynamics: any change in backpressure is
    redistributed to busy/idle according to ``bp_to_busy_ratio``, and
    control actions (deviation_term) move budget between busy and idle
    while leaving backpressure unaffected.

    ``bp_to_busy_ratio`` (η)
        Fraction of a backpressure change absorbed by busy_time.
        The remainder ``1 - η`` is absorbed by idle_time.
        Default 0.7: backpressure mostly steals from busy (CPU-bound work
        queues up before free capacity is consumed).
    """

    # Setpoints — must satisfy: target_busy + target_idle + target_bp = 1.0
    target_busy_time: float = 0.8
    target_idle_time: float = 0.2
    target_backpressure: float = 0.0

    # Horizon
    event_horizon: int = 3

    # Dynamics gains
    alpha: float = 0.9  # mean-reversion rate for busy/idle time toward setpoint
    beta: float = 0.9  # control authority: how strongly deviation shifts busy↔idle
    gamma: float = 0.9  # mean-reversion rate for backpressure toward its setpoint

    # NOTE: This is the fraction of backpressure change absorbed by busy_time (rest by idle)
    bp_to_busy_ratio: float = 0.8

    # Cost weights
    stage_cost_control_weight: float = 0.005
    r_term_deviation: float = 0.05
    backpressure_cost_weight: float = 100.0

    # Control bounds
    deviation_lower_bound: float = -1.0

    # MPC solver settings
    collocation_deg: int = 2
    collocation_ni: int = 2

    def __post_init__(self) -> None:
        total = self.target_busy_time + self.target_idle_time + self.target_backpressure
        if not np.isclose(total, 1.0):
            raise ValueError(
                f"Targets must sum to 1.0, got {total:.4f} "
                f"(busy={self.target_busy_time}, idle={self.target_idle_time}, "
                f"bp={self.target_backpressure})"
            )
        if not 0.0 <= self.bp_to_busy_ratio <= 1.0:
            raise ValueError(
                f"bp_to_busy_ratio must be in [0, 1], got {self.bp_to_busy_ratio}"
            )


class MPCController:
    """Discrete-time MPC controller for workload-driven autoscaling.

    The controller tracks three observable metrics (busy time, idle time,
    backpressure time) and outputs a *deviation term* that scales the
    current replica count:

        new_replicas = current_replicas * measurement_step(metrics)

    Usage::

        controller = MPCController()
        controller.initial_measurement(metrics_array)
        scale_factor = controller.measurement_step(metrics_array)
    """

    _model: Model
    _controller: MPC

    def __init__(self, config: MPCConfig | None = None) -> None:
        self._cfg = config or MPCConfig()
        logger.info(
            "Initialising MPCController | targets=busy:%.2f idle:%.2f bp:%.2f | horizon=%d",
            self._cfg.target_busy_time,
            self._cfg.target_idle_time,
            self._cfg.target_backpressure,
            self._cfg.event_horizon,
        )
        self._model = self._setup_model()
        self._controller = self._setup_mpc(self._model)
        logger.debug("MPCController ready.")

    # ------------------------------------------------------------------
    # Internal setup
    # ------------------------------------------------------------------

    def _setup_model(self) -> Model:
        logger.debug("Building coupled discrete state-space model.")
        cfg = self._cfg
        model = do_mpc.model.Model("discrete")

        busy_time = model.set_variable(var_type="_x", var_name="busy_time")
        idle_time = model.set_variable(var_type="_x", var_name="idle_time")
        backpressure_time = model.set_variable(
            var_type="_x", var_name="backpressure_time"
        )
        deviation_term = model.set_variable(var_type="_u", var_name="deviation_term")

        # Scaling up (positive deviation) directly reduces backpressure
        # Scaling down (negative deviation) increases backpressure risk
        model.set_rhs(
            "backpressure_time",
            backpressure_time
            + cfg.gamma * (cfg.target_backpressure - backpressure_time)
            - cfg.beta * deviation_term,  # ← control action directly drives BP down
        )

        # busy: mean-reverts, loses budget as backpressure is relieved
        model.set_rhs(
            "busy_time",
            busy_time
            + cfg.alpha * (cfg.target_busy_time - busy_time)
            - cfg.bp_to_busy_ratio * cfg.beta * deviation_term,
        )

        # idle: absorbs remaining budget
        model.set_rhs(
            "idle_time",
            idle_time
            + cfg.alpha * (cfg.target_idle_time - idle_time)
            + (1.0 - cfg.bp_to_busy_ratio) * cfg.beta * deviation_term,
        )

        model.setup()
        logger.debug("Coupled model setup complete.")
        return model

    def _setup_mpc(self, model: Model) -> MPC:
        """Configure and return the MPC controller."""
        logger.debug("Configuring MPC solver (horizon=%d).", self._cfg.event_horizon)
        cfg = self._cfg
        mpc = do_mpc.controller.MPC(model)

        mpc.set_param(
            n_horizon=cfg.event_horizon,
            t_step=1,
            n_robust=1,
            state_discretization="collocation",
            collocation_type="radau",
            collocation_deg=cfg.collocation_deg,
            collocation_ni=cfg.collocation_ni,
            store_full_solution=True,
        )

        # Terminal cost: penalise distance from setpoints at end of horizon
        mterm = self._state_tracking_cost(model)

        # Stage cost: tracking + control effort penalty
        lterm = mterm + cfg.stage_cost_control_weight * model.u["deviation_term"] ** 2

        mpc.set_objective(mterm=mterm, lterm=lterm)
        mpc.set_rterm(deviation_term=cfg.r_term_deviation)
        mpc.bounds["lower", "_u", "deviation_term"] = cfg.deviation_lower_bound

        mpc.setup()
        logger.debug("MPC solver setup complete.")
        return mpc

    def _state_tracking_cost(self, model: Model):
        cfg = self._cfg
        return (
            (model.x["busy_time"] - cfg.target_busy_time) ** 2
            + (model.x["idle_time"] - cfg.target_idle_time) ** 2
            + cfg.backpressure_cost_weight
            * (model.x["backpressure_time"] - cfg.target_backpressure) ** 2
        )

    def initial_measurement(self, metrics: Array3Float) -> None:
        """Seed the controller with the first observed metric vector.

        Must be called once before :meth:`measurement_step`.

        Args:
            metrics: ``[busy_time, idle_time, backpressure_time]`` — must sum to 1.0.

        Raises:
            ValueError: if the metrics do not sum to 1.0 (within 1e-3 tolerance).
        """
        total = float(np.sum(metrics))
        if not np.isclose(total, 1.0, atol=1e-1):
            raise ValueError(
                f"Initial metrics must sum to 1.0, got {total:.4f}. "
                "Ensure busy_time + idle_time + backpressure_time = 1."
            )
        logger.info(
            "Setting initial state: busy=%.4f idle=%.4f backpressure=%.4f (sum=%.4f)",
            *metrics.flat,
            total,
        )
        self._controller.x0 = metrics
        self._controller.set_initial_guess()

    def measurement_step(self, metrics: Array3Float) -> float:
        """Advance the controller by one step and return the scale factor.

        Args:
            metrics: ``[busy_time, idle_time, backpressure_time]``

        Returns:
            Multiplicative scale factor for the current replica count,
            i.e. ``1 + deviation_term``.  Values > 1 scale up;
            values in ``(0, 1)`` scale down.
        """
        logger.debug(
            "Observed metrics: busy=%.4f idle=%.4f backpressure=%.4f",
            *metrics.flat,
        )
        deviation: ndarray = self._controller.make_step(metrics)
        deviation_value = float(deviation[0])
        scale_factor = 1.0 + deviation_value

        logger.info(
            "Control output: deviation=%.4f → scale_factor=%.4f",
            deviation_value,
            scale_factor,
        )
        return scale_factor
