# -*- coding: utf-8 -*-
"""Grid search service — auto-generate experiments for weight/threshold combos."""

import datetime
import itertools
import logging

from app.lib.scoring_engine.config import SCORING_CONFIG, SUPPORTED_HORIZONS
from app.model.scoring import ScoreExperiment

logger = logging.getLogger(__name__)

# Tolerance for weight-sum equality checks (handles float rounding).
WEIGHT_SUM_TOLERANCE = 0.1


class GridSearchService:
    """Auto-generate ScoreExperiment records from parameter grids."""

    def __init__(self, experiment_model=ScoreExperiment):
        self.experiment_model = experiment_model

    def create_experiments(
        self,
        name_prefix: str,
        start_date: datetime.datetime,
        end_date: datetime.datetime,
        weight_grid: dict | None = None,
        threshold_grid: dict | None = None,
        horizons: list | None = None,
        baseline_model_version: str | None = None,
        dry_run: bool = False,
        weight_sum_target: int = 100,
        vary_enabled_only: bool = True,
    ) -> dict:
        """Generate N ScoreExperiment records from grid combinations.

        weight_grid: {"signal_strength": [15, 20, 25], "momentum": [20, 25]}
        threshold_grid: {"buy_threshold": [60, 70], "watch_threshold": [40, 50]}

        weight_sum_target: expected sum of all component weights (default 100).
            Combinations whose sum deviates more than a small tolerance are
            normalised and a warning is recorded.

        vary_enabled_only: when True, components *not* listed in weight_grid
            keep their baseline weights from SCORING_CONFIG for this horizon.
            The full merged weight set is validated against weight_sum_target.
            When False, only the components in weight_grid are considered.
        """
        horizons = horizons or list(SUPPORTED_HORIZONS)
        weight_grid = weight_grid or {}
        threshold_grid = threshold_grid or {}

        # Generate all weight combinations per horizon
        experiments_created = 0
        errors = []

        for horizon in horizons:
            horizon_count = self._generate_horizon_combos(
                name_prefix=name_prefix,
                horizon=horizon,
                start_date=start_date,
                end_date=end_date,
                weight_grid=weight_grid,
                threshold_grid=threshold_grid,
                baseline_model_version=baseline_model_version,
                dry_run=dry_run,
                errors=errors,
                weight_sum_target=weight_sum_target,
                vary_enabled_only=vary_enabled_only,
            )
            experiments_created += horizon_count

        return {
            "total_experiments": experiments_created,
            "horizons": horizons,
            "dry_run": dry_run,
            "errors": errors,
        }

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _baseline_weights_for_horizon(horizon: int) -> dict[str, float]:
        """Return the default weight map for *horizon* from SCORING_CONFIG.

        Returns an empty dict when the horizon has no config entry.
        """
        cfg = SCORING_CONFIG.get(horizon, {})
        return dict(cfg.get("weights", {}))

    @staticmethod
    def _merge_weights(
        varied: dict[str, float],
        baseline: dict[str, float],
    ) -> dict[str, float]:
        """Start with *baseline*, then update with *varied* so that grid
        values override.
        """
        merged = dict(baseline)
        merged.update(varied)
        return merged

    @staticmethod
    def _normalize_weights(
        weights: dict[str, float],
        target: float,
    ) -> dict[str, float]:
        """Scale every value so that sum(weights) == target.

        Returns the unmodified dict when the current sum is zero.
        """
        total = sum(weights.values())
        if total == 0:
            return dict(weights)
        factor = target / total
        return {k: round(v * factor, 1) for k, v in weights.items()}

    def _generate_horizon_combos(
        self,
        name_prefix: str,
        horizon: int,
        start_date: datetime.datetime,
        end_date: datetime.datetime,
        weight_grid: dict,
        threshold_grid: dict,
        baseline_model_version: str | None,
        dry_run: bool,
        errors: list,
        weight_sum_target: int = 100,
        vary_enabled_only: bool = True,
    ) -> int:
        """Generate one experiment per (weight_combo × threshold_combo).

        Empty sub-grids produce a single no-op entry (default weights/thresholds).

        Applies weight-sum validation and optional baseline-weight merging
        via *vary_enabled_only*.
        """
        weight_keys = list(weight_grid.keys())
        weight_values = [weight_grid[k] for k in weight_keys] if weight_keys else []
        threshold_keys = list(threshold_grid.keys())
        threshold_values = (
            [threshold_grid[k] for k in threshold_keys] if threshold_keys else []
        )

        # Build combo lists (at least one entry for product to work)
        weight_combos_raw = (
            list(itertools.product(*weight_values)) if weight_values else [()]
        )
        threshold_combos = (
            list(itertools.product(*threshold_values)) if threshold_values else [()]
        )

        # Pre-load the baseline weight map for this horizon (used only when
        # vary_enabled_only is True).
        baseline_weights = (
            self._baseline_weights_for_horizon(horizon) if vary_enabled_only else {}
        )

        created = 0
        suffix = 0

        for w_combo_raw in weight_combos_raw:
            for t_combo in threshold_combos:
                suffix += 1

                # --- Build final weight map ----------------------------------
                varied_weights = (
                    dict(zip(weight_keys, w_combo_raw)) if weight_keys else {}
                )
                if baseline_weights:
                    full_weights = self._merge_weights(varied_weights, baseline_weights)
                else:
                    full_weights = dict(varied_weights)

                # --- Validate / normalise weight sum --------------------------
                weight_total = sum(full_weights.values())
                weight_ok = abs(weight_total - weight_sum_target) < WEIGHT_SUM_TOLERANCE

                if not weight_ok and weight_total > 0:
                    msg = (
                        f"Horizon {horizon}: weight sum {weight_total:.1f} ≠ "
                        f"target {weight_sum_target}; normalising ("
                        + ",".join(f"{k}:{v}" for k, v in full_weights.items())
                        + ")"
                    )
                    logger.warning(msg)
                    errors.append(msg)
                    full_weights = self._normalize_weights(
                        full_weights, weight_sum_target
                    )

                # --- Build config entry ---------------------------------------
                config_entry: dict = {}
                if full_weights:
                    config_entry["weights"] = full_weights
                if threshold_keys:
                    config_entry.update(dict(zip(threshold_keys, t_combo)))

                config = {str(horizon): config_entry}
                model_version = f"{name_prefix}_h{horizon}_c{suffix}"
                name_parts = []
                if weight_keys:
                    name_parts.append(
                        "w="
                        + ",".join(f"{k}:{v}" for k, v in zip(weight_keys, w_combo_raw))
                    )
                if threshold_keys:
                    name_parts.append(
                        "t="
                        + ",".join(f"{k}:{v}" for k, v in zip(threshold_keys, t_combo))
                    )
                name = f"{name_prefix} h={horizon} {' '.join(name_parts)}"

                if not dry_run:
                    try:
                        exp = self.experiment_model(
                            name=name,
                            model_version=model_version,
                            baseline_model_version=baseline_model_version,
                            start_date=start_date,
                            end_date=end_date,
                            horizons=[horizon],
                            config=config,
                        )
                        exp.save()
                    except Exception as exc:
                        msg = f"Failed to save experiment '{name}': {exc}"
                        logger.error(msg)
                        errors.append(msg)
                        continue

                created += 1

        return created
