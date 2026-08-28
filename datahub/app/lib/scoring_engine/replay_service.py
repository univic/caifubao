# -*- coding: utf-8 -*-

import datetime
import logging

from app.lib.scoring_engine.config import DEFAULT_MODEL_VERSION, SUPPORTED_HORIZONS
from app.lib.scoring_engine.scoring_service import StockScoringService, normalize_date

logger = logging.getLogger(__name__)


class ScoreReplayService:
    """Historical scoring replay for date ranges."""

    def __init__(
        self,
        scoring_service: StockScoringService | None = None,
        model_version: str = DEFAULT_MODEL_VERSION,
        scoring_config: dict | None = None,
    ):
        self.scoring_service = scoring_service or StockScoringService(
            model_version=model_version,
            scoring_config=scoring_config,
        )

    def backfill_predictions(
        self,
        start_date: datetime.datetime,
        end_date: datetime.datetime,
        horizon: int | None = None,
        stock_code: str | None = None,
        dry_run: bool = False,
        replace: bool = False,
    ) -> dict:
        dates = self._trading_dates(start_date, end_date)
        horizons = [horizon] if horizon else list(SUPPORTED_HORIZONS)
        scored_count = 0
        for date in dates:
            for current_horizon in horizons:
                if stock_code:
                    stock = self.scoring_service.stock_model.objects(
                        code=stock_code
                    ).first()
                    if not stock:
                        continue
                    self.scoring_service.score_single_stock(
                        stock,
                        date,
                        current_horizon,
                        dry_run=dry_run,
                        replace=replace,
                    )
                    scored_count += 1
                    # Do NOT re-rank a single-stock cohort: with one stock the
                    # percentile is degenerate (always 1.0), which would turn
                    # percentile-driven recommendations into a blanket BUY.
                    # score_single_stock already applied absolute-threshold
                    # recommendations; leave them intact.
                else:
                    result = self.scoring_service.score_all_stocks(
                        date=date,
                        horizon=current_horizon,
                        dry_run=dry_run,
                        replace=replace,
                    )
                    scored_count += result["scored_count"]
        return {
            "from": normalize_date(start_date),
            "to": normalize_date(end_date),
            "horizons": horizons,
            "date_count": len(dates),
            "scored_count": scored_count,
            "dry_run": dry_run,
            "replace": replace,
        }

    def _trading_dates(self, start_date, end_date):
        start = normalize_date(start_date)
        end = normalize_date(end_date)
        if self.scoring_service.calendar:
            return [
                normalize_date(day)
                for day in sorted(self.scoring_service.calendar)
                if start <= normalize_date(day) <= end
            ]

        dates = []
        current = start
        while current <= end:
            if current.weekday() < 5:
                dates.append(current)
            current += datetime.timedelta(days=1)
        return dates
