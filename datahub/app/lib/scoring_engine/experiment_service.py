# -*- coding: utf-8 -*-

import datetime

from app.lib.scoring_engine.calibration_report import ScoreCalibrationReport
from app.lib.scoring_engine.replay_service import ScoreReplayService
from app.lib.scoring_engine.verification_service import ScoreVerificationService
from app.model.scoring import ScoreExperiment


class ScoreExperimentService:
    """Run a stored score experiment through replay, verification, and reporting."""

    def __init__(self, experiment_model=ScoreExperiment):
        self.experiment_model = experiment_model

    def run_experiment(
        self,
        experiment_id: str,
        backfill: bool = True,
        verify: bool = True,
        replace: bool = False,
        dry_run: bool = False,
    ) -> dict:
        experiment = self.experiment_model.objects(id=experiment_id).first()
        if experiment is None:
            raise ValueError(f"ScoreExperiment not found: {experiment_id}")

        experiment.status = "RUNNING"
        experiment.error_msg = None
        experiment.save()

        try:
            result = {
                "experiment_id": str(experiment.id),
                "model_version": experiment.model_version,
                "horizons": list(experiment.horizons or []),
                "backfill": {},
                "verification": {},
                "reports": {},
                "dry_run": dry_run,
            }
            if backfill:
                replay = ScoreReplayService(
                    model_version=experiment.model_version,
                    scoring_config=experiment.config or {},
                )
                for horizon in experiment.horizons or []:
                    result["backfill"][str(horizon)] = replay.backfill_predictions(
                        start_date=experiment.start_date,
                        end_date=experiment.end_date,
                        horizon=horizon,
                        dry_run=dry_run,
                        replace=replace,
                    )

            if verify and not dry_run:
                verifier = ScoreVerificationService(
                    model_version=experiment.model_version
                )
                for horizon in experiment.horizons or []:
                    result["verification"][str(horizon)] = verifier.verify_predictions(
                        start_date=experiment.start_date,
                        end_date=experiment.end_date,
                        horizon=horizon,
                    )

            if not dry_run:
                for horizon in experiment.horizons or []:
                    report = ScoreCalibrationReport(
                        model_version=experiment.model_version
                    ).generate(
                        start_date=experiment.start_date,
                        end_date=experiment.end_date,
                        horizon=horizon,
                    )
                    result["reports"][str(horizon)] = report
                experiment.report = result
                experiment.status = "COMPLETED"
                experiment.completed_at = datetime.datetime.now(datetime.UTC)
                experiment.save()

            return result
        except Exception as exc:
            experiment.status = "FAILED"
            experiment.error_msg = str(exc)
            experiment.save()
            raise
