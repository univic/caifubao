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
            )
            experiments_created += horizon_count

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
        errors: list,
    ) -> int:
        """Generate one experiment per (weight_combo × threshold_combo).

        Empty sub-grids produce a single no-op entry (default weights/thresholds).
        """
        weight_keys = list(weight_grid.keys())
        weight_values = [weight_grid[k] for k in weight_keys] if weight_keys else []
        threshold_keys = list(threshold_grid.keys())
        threshold_values = (
            [threshold_grid[k] for k in threshold_keys] if threshold_keys else []
        )

        # Build combo lists (at least one entry for product to work)
        weight_combos = (
            list(itertools.product(*weight_values)) if weight_values else [()]
        )
        threshold_combos = (
            list(itertools.product(*threshold_values)) if threshold_values else [()]
        )

        created = 0
        suffix = 0

        for w_combo in weight_combos:
            for t_combo in threshold_combos:
                suffix += 1
                config_entry: dict = {}

                if weight_keys:
                    config_entry["weights"] = dict(zip(weight_keys, w_combo))
                if threshold_keys:
                    config_entry.update(dict(zip(threshold_keys, t_combo)))

                config = {str(horizon): config_entry}
                model_version = f"{name_prefix}_h{horizon}_c{suffix}"
                name_parts = []
                if weight_keys:
                    name_parts.append(
                        "w="
                        + ",".join(f"{k}:{v}" for k, v in zip(weight_keys, w_combo))
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
