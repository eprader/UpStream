import logging
from dataclasses import dataclass
from typing import Literal, TypeAlias

import do_mpc
import numpy as np
from do_mpc.controller import MPC
from do_mpc.model import Model

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

Array2Float: TypeAlias = np.ndarray[Literal[2], np.dtype[np.float64]]


@dataclass(frozen=True)
class MPCConfig:
    """Immutable configuration optimized for single-step deadbeat control."""

    # Setpoints — must satisfy: target_busy + target_idle = 1.0
    target_busy_time: float = 0.8
    target_idle_time: float = 0.2

    # System control authority
    beta: float = 1

    # Force action immediately by setting lookahead horizon to 1
    event_horizon: int = 3

    # Weights: State errors are highly penalized, control effort is free (0.0)
    busy_cost_weight: float = 1.0
    idle_cost_weight: float = 1.0
    stage_cost_control_weight: float = 0.0
    r_term_deviation: float = 0.5

    # Control bounds (Allowed range for deviation_term)
    deviation_lower_bound: float = -1.0
    deviation_upper_bound: float = 3.0

    # MPC solver settings
    collocation_deg: int = 2
    collocation_ni: int = 2


class MPCController:
    """Discrete-time MPC controller driving system states to targets in 1 step."""

    _model: Model
    _controller: MPC

    def __init__(self, config: MPCConfig | None = None) -> None:
        self._cfg = config or MPCConfig()
        logger.info(
            "Initialising MPCController | targets=busy:%.2f idle:%.2f | horizon=%d",
            self._cfg.target_busy_time,
            self._cfg.target_idle_time,
            self._cfg.event_horizon,
        )
        self._model = self._setup_model()
        self._controller = self._setup_mpc(self._model)
        logger.debug("MPCController ready.")

    def _setup_model(self) -> Model:
        logger.debug("Building discrete state-space model.")
        cfg = self._cfg
        model = do_mpc.model.Model("discrete")

        busy_time = model.set_variable(var_type="_x", var_name="busy_time")
        idle_time = model.set_variable(var_type="_x", var_name="idle_time")
        deviation_term = model.set_variable(var_type="_u", var_name="deviation_term")

        # State transitions:
        # Scaling UP (positive deviation) drops busy time and increases idle time
        model.set_rhs("busy_time", busy_time - cfg.beta * deviation_term)
        model.set_rhs("idle_time", idle_time + cfg.beta * deviation_term)

        model.setup()
        logger.debug("Model setup complete.")
        return model

    def _setup_mpc(self, model: Model) -> MPC:
        logger.debug("Configuring MPC solver (horizon=%d).", self._cfg.event_horizon)
        cfg = self._cfg
        mpc = do_mpc.controller.MPC(model)

        mpc.set_param(
            n_horizon=cfg.event_horizon,
            t_step=1,
            n_robust=0,  # Zero for deterministic single-step tracking
            state_discretization="collocation",
            collocation_type="radau",
            collocation_deg=cfg.collocation_deg,
            collocation_ni=cfg.collocation_ni,
            store_full_solution=True,
        )

        # Objective function setup
        mterm = self._state_tracking_cost(model)
        lterm = mterm + cfg.stage_cost_control_weight * model.u["deviation_term"] ** 2

        mpc.set_objective(mterm=mterm, lterm=lterm)
        mpc.set_rterm(deviation_term=cfg.r_term_deviation)
        
        # Symmetrical bounds to ensure scaling up isn't artificially choked
        mpc.bounds["lower", "_u", "deviation_term"] = cfg.deviation_lower_bound
        mpc.bounds["upper", "_u", "deviation_term"] = cfg.deviation_upper_bound

        mpc.setup()
        logger.debug("MPC solver setup complete.")
        return mpc

    def _state_tracking_cost(self, model: Model):
        cfg = self._cfg
        return (
            cfg.busy_cost_weight * (model.x["busy_time"] - cfg.target_busy_time) ** 2
            + cfg.idle_cost_weight * (model.x["idle_time"] - cfg.target_idle_time) ** 2
        )

    def initial_measurement(self, metrics: Array2Float) -> None:
        total = float(np.sum(metrics))
        if not np.isclose(total, 1.0, atol=1e-1):
            raise ValueError(f"Initial metrics must sum to 1.0, got {total:.4f}.")
        
        self._controller.x0 = metrics
        self._controller.set_initial_guess()

    def measurement_step(self, metrics: Array2Float) -> float:
        # Update the current state of the optimizer before calculating the step
        self._controller.x0 = metrics
        
        deviation = self._controller.make_step(metrics)
        deviation_value = float(deviation[0, 0])
        scale_factor = 1 + deviation_value

        logger.info(
            "Control output: deviation=%.4f → scale_factor=%.4f",
            deviation_value,
            scale_factor,
        )
        return scale_factor
