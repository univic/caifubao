# -*- coding: utf-8 -*-
"""Grid search service — auto-generate experiments for weight/threshold combos."""

import datetime
import itertools
import logging

from app.lib.scoring_engine.config import SUPPORTED_HORIZONS
from app.model.scoring import ScoreExperiment

logger = logging.getLogger(__name__)


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
    ) -> dict:
        """Generate N ScoreExperiment records from grid combinations.

        weight_grid: {"signal_strength": [15, 20, 25], "momentum": [20, 25]}
        threshold_grid: {"buy_threshold": [60, 70], "watch_threshold": [40, 50]}
        """
        horizons = horizons or list(SUPPORTED_HORIZONS)
        weight_grid = weight_grid or {}
        threshold_grid = threshold_grid or {}

        # Generate all weight combinations per horizon
        experiments_created = 0
        errors = []

        for horizon in horizons:
            horizon_experiments = self._generate_horizon_combos(
                name_prefix=name_prefix,
                horizon=horizon,
                start_date=start_date,
                end_date=end_date,
                weight_grid=weight_grid,
                threshold_grid=threshold_grid,
                baseline_model_version=baseline_model_version,
                dry_run=dry_run,
            )
            experiments_created += horizon_experiments

        return {
            "total_experiments": experiments_created,
            "horizons": horizons,
            "dry_run": dry_run,
            "errors": errors,
        }

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
    ) -> int:
        """Generate experiments for one horizon from grid combinations."""
        created = 0
        # Generate weight combinations
        weight_keys = list(weight_grid.keys())
        weight_values_lists = [weight_grid[k] for k in weight_keys]

        for weight_combo in itertools.product(*weight_values_lists):
            # Build config with overridden weights
            config = {
                str(horizon): {
                    "weights": dict(zip(weight_keys, weight_combo)),
                }
            }

            # Add threshold overrides directly
            if threshold_grid:
                config[str(horizon)].update(threshold_grid)

            model_version = f"{name_prefix}_h{horizon}_w{created+1}"
            combo_label = "_".join(
                f"{k[:3]}{v}" for k, v in zip(weight_keys, weight_combo)
            )
            name = f"{name_prefix} horizon={horizon} weights={combo_label}"

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
                    logger.error("Failed to create experiment: %s", exc)
                    continue

            created += 1

        # Generate threshold-only combinations (no weight change)
        if threshold_grid:
            threshold_keys = list(threshold_grid.keys())
            threshold_values_lists = [
                threshold_grid[k] for k in threshold_keys
            ]
            for threshold_combo in itertools.product(*threshold_values_lists):
                config = {
                    str(horizon): dict(
                        zip(threshold_keys, threshold_combo)
                    ),
                }
                model_version = f"{name_prefix}_h{horizon}_t{created+1}"
                combo_label = "_".join(
                    f"{k[:3]}{v}" for k, v in zip(threshold_keys, threshold_combo)
                )
                name = f"{name_prefix} horizon={horizon} thresholds={combo_label}"

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
                        logger.error("Failed to create experiment: %s", exc)
                        continue

                created += 1

        return created
