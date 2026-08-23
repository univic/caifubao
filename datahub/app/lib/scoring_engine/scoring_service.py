# -*- coding: utf-8 -*-

import datetime
import logging

from app.lib.scoring_engine.components import (
    aggregate_industry_metrics,
    breakout_or_position_component,
    industry_momentum_component,
    momentum_component,
    quote_price,
    real_relative_strength_component,
    relative_strength_component,
    risk_penalty,
    signal_strength_component,
    trend_alignment_component,
)
from app.lib.scoring_engine.config import (
    DEFAULT_MODEL_VERSION,
    SUPPORTED_HORIZONS,
    get_effective_horizon_config,
)
from app.model.factor import StockFactorDaily
from app.model.scoring import StockScorePrediction
from app.model.signal import StockSignalDaily
from app.model.stock import FinanceMarket, IndividualStock, StockDailyQuote
from app.lib.utilities import trading_day_helper

logger = logging.getLogger(__name__)


def normalize_date(value: datetime.datetime) -> datetime.datetime:
    return value.replace(hour=0, minute=0, second=0, microsecond=0)


class StockScoringService:
    """Generate multi-horizon score predictions from existing datahub data."""

    def __init__(
        self,
        stock_model=IndividualStock,
        quote_model=StockDailyQuote,
        factor_model=StockFactorDaily,
        signal_model=StockSignalDaily,
        prediction_model=StockScorePrediction,
        model_version: str = DEFAULT_MODEL_VERSION,
        scoring_config: dict | None = None,
    ):
        self.stock_model = stock_model
        self.quote_model = quote_model
        self.factor_model = factor_model
        self.signal_model = signal_model
        self.prediction_model = prediction_model
        self.model_version = model_version
        self.scoring_config = scoring_config or {}
        self.market = FinanceMarket.objects(name="ChinaAStock").first()
        self.calendar = self.market.trade_calendar if self.market else []

    def get_t_plus_n_day(
        self, start_date: datetime.datetime, n: int
    ) -> datetime.datetime:
        """Find the N-th trading day after start_date."""
        start_date = normalize_date(start_date)
        if not self.calendar:
            return start_date + datetime.timedelta(days=round(n * 1.5))

        sorted_cal = sorted(normalize_date(day) for day in self.calendar)
        try:
            start_idx = sorted_cal.index(start_date)
            target_idx = start_idx + n
            if target_idx < len(sorted_cal):
                return sorted_cal[target_idx]
            return sorted_cal[-1]
        except ValueError:
            future_days = [day for day in sorted_cal if day > start_date]
            if len(future_days) >= n:
                return future_days[n - 1]
            return sorted_cal[-1]

    def score_all_stocks(
        self,
        date: datetime.datetime | None = None,
        horizon: int | None = None,
        dry_run: bool = False,
        replace: bool = False,
    ) -> dict:
        """Run scoring for all active stocks on one evaluation date."""
        if date is None:
            date = trading_day_helper.determine_closest_trading_date(self.calendar)
        date = normalize_date(date)

        horizons = [horizon] if horizon else list(SUPPORTED_HORIZONS)
        stocks = list(self.stock_model.objects(active_status=0))
        results = []
        for current_horizon in horizons:
            for stock in stocks:
                try:
                    prediction = self.score_single_stock(
                        stock,
                        date,
                        current_horizon,
                        dry_run=dry_run,
                        replace=replace,
                    )
                    results.append(prediction)
                except Exception as exc:
                    logger.exception(
                        "Failed to score %s horizon=%s date=%s: %s",
                        getattr(stock, "code", None),
                        current_horizon,
                        date,
                        exc,
                    )

            if not dry_run:
                self.assign_ranks(date, current_horizon)

                # Apply hybrid recommendation logic using percentile rankings
                try:
                    self._upgrade_recommendations(date, current_horizon)
                except Exception as exc:
                    logger.exception(
                        "Failed to upgrade recommendations for h=%d: %s",
                        current_horizon,
                        exc,
                    )

                # Aggregate industry metrics per horizon
                horizon_predictions = list(
                    self.prediction_model.objects(
                        date=date,
                        horizon=current_horizon,
                        model_version=self.model_version,
                    )
                )
                try:
                    aggregate_industry_metrics(
                        date=date,
                        predictions=horizon_predictions,
                        model_version=self.model_version,
                    )
                    logger.info(
                        "Industry metrics aggregated for date=%s horizon=%d predictions=%d",
                        date,
                        current_horizon,
                        len(horizon_predictions),
                    )
                except Exception as exc:
                    logger.warning(
                        "Failed to aggregate industry metrics for horizon=%d: %s",
                        current_horizon,
                        exc,
                    )

        return {
            "date": date,
            "horizons": horizons,
            "scored_count": len(results),
            "dry_run": dry_run,
        }

    def score_single_stock(
        self,
        stock,
        date: datetime.datetime,
        horizon: int,
        dry_run: bool = False,
        replace: bool = False,
    ):
        """Calculate one horizon-specific prediction for a stock."""
        date = normalize_date(date)
        config = self._get_horizon_config(horizon)
        existing = self._find_existing_prediction(stock.code, date, horizon)
        if existing is not None and not replace and not dry_run:
            return existing

        quote = self._get_quote_on_date(stock.code, date)
        target_date = self.get_t_plus_n_day(date, horizon)
        if not quote:
            payload = self._build_blocked_prediction(
                stock=stock,
                date=date,
                horizon=horizon,
                target_date=target_date,
                reason="missing_quote",
            )
            return self._persist_prediction(payload, existing, dry_run)

        factors = self._get_factor_on_date(stock.code, date)
        signals = self._get_signals_on_date(stock.code, date)
        history_quotes = self._get_previous_quotes(
            stock.code,
            date,
            max(
                config["minimum_quote_count"],
                config["breakout_lookback"],
                config["risk_lookback"],
            ),
        )
        components, penalties = self._build_components(
            quote, factors, signals, history_quotes, date, horizon, config, stock.code
        )
        score = self._calculate_score(components, penalties)
        recommendation = self._recommendation(score, config)
        input_snapshot = self._build_input_snapshot(
            quote=quote,
            factors=factors,
            signals=signals,
            history_quotes=history_quotes,
            config=config,
            blocked_reason=None,
        )
        explanation = self._build_explanation(
            horizon=horizon,
            score=score,
            components=components,
            penalties=penalties,
            config=config,
        )

        payload = {
            "stock": stock,
            "stock_code": stock.code,
            "stock_name": stock.name,
            "date": date,
            "horizon": horizon,
            "score": score,
            "recommendation": recommendation,
            "base_price": quote_price(quote),
            "target_date": target_date,
            "status": "PENDING",
            "explanation": explanation,
            "verification": {
                "status": "PENDING",
                "target_date": target_date.isoformat(),
                "expected_quote_count": horizon,
                "verified_quote_count": 0,
                "effective_threshold": config["effective_threshold"],
                "stop_loss_threshold": config["stop_loss_threshold"],
            },
            "input_snapshot": input_snapshot,
            "model_version": self.model_version,
        }
        return self._persist_prediction(payload, existing, dry_run)

    def assign_ranks(self, date: datetime.datetime, horizon: int) -> None:
        predictions = list(
            self.prediction_model.objects(
                date=normalize_date(date),
                horizon=horizon,
                model_version=self.model_version,
                status__ne="BLOCKED",
            ).order_by("-score")
        )
        total = len(predictions)
        for idx, prediction in enumerate(predictions, start=1):
            prediction.rank = idx
            prediction.percentile = round(1 - ((idx - 1) / total), 4) if total else None
            prediction.save()

    def _upgrade_recommendations(self, date: datetime.datetime, horizon: int) -> None:
        """Re-compute recommendations using hybrid logic after ranks are assigned.

        Called after assign_ranks() so that percentiles are available.
        Updates the recommendation field in-place for all predictions on this
        date/horizon/model_version.
        """
        predictions = list(
            self.prediction_model.objects(
                date=normalize_date(date),
                horizon=horizon,
                model_version=self.model_version,
                status__ne="BLOCKED",
            )
        )
        if not predictions:
            return

        config = self._get_horizon_config(horizon)
        bulk_ops = []
        for p in predictions:
            new_rec = self._recommendation(
                score=p.score,
                config=config,
                percentile=p.percentile,
            )
            if new_rec != p.recommendation:
                from pymongo import UpdateOne

                bulk_ops.append(
                    UpdateOne(
                        {"_id": p.id},
                        {"$set": {"recommendation": new_rec}},
                    )
                )

        if bulk_ops:
            result = self.prediction_model._get_collection().bulk_write(
                bulk_ops, ordered=False
            )
            updated = result.modified_count
        else:
            updated = 0

        logger.info(
            "Hybrid recommendations updated for %s h=%d: %d/%d changed",
            date.strftime("%Y-%m-%d"),
            horizon,
            updated,
            len(predictions),
        )

    def _find_existing_prediction(self, stock_code, date, horizon):
        return self.prediction_model.objects(
            stock_code=stock_code,
            date=date,
            horizon=horizon,
            model_version=self.model_version,
        ).first()

    def _persist_prediction(self, payload: dict, existing, dry_run: bool):
        if dry_run:
            return payload

        if existing is None:
            prediction = self.prediction_model(**payload)
        else:
            prediction = existing
            for key, value in payload.items():
                setattr(prediction, key, value)
        prediction.save()
        return prediction

    def _build_blocked_prediction(self, stock, date, horizon, target_date, reason):
        config = self._get_horizon_config(horizon)
        return {
            "stock": stock,
            "stock_code": stock.code,
            "stock_name": stock.name,
            "date": date,
            "horizon": horizon,
            "score": 0.0,
            "recommendation": "NONE",
            "target_date": target_date,
            "status": "BLOCKED",
            "explanation": {
                "summary": f"Scoring blocked: {reason}",
                "horizon": horizon,
                "score": 0.0,
                "components": [],
                "penalties": [],
                "thresholds": self._thresholds(config),
            },
            "verification": {
                "status": "BLOCKED",
                "target_date": target_date.isoformat(),
                "expected_quote_count": horizon,
                "verified_quote_count": 0,
            },
            "input_snapshot": {
                "status": "BLOCKED",
                "blocked_reason": reason,
                "quote": {"status": "missing"},
                "factor": {"status": "unknown"},
                "signals": {"status": "unknown"},
            },
            "model_version": self.model_version,
        }

    def _build_components(
        self, quote, factors, signals, history_quotes, date, horizon, config, stock_code
    ):
        import datetime as dt

        weights = config["weights"]
        momentum_quotes = history_quotes[: config["momentum_lookback"]]
        breakout_quotes = history_quotes[: config["breakout_lookback"]]
        risk_quotes = history_quotes[: config["risk_lookback"]]

        # Signal persistence decay: when today has no bullish signal, look back
        decay_max_days = config.get("signal_decay_max_days", 5)
        decay_factor = config.get("signal_decay_factor", 0.7)
        days_since_signal: int | None = None
        last_signal_strengths: list[float] | None = None
        last_signal_names: list[str] | None = None

        has_bullish_today = any(
            getattr(s, "direction", None) == "BULLISH"
            and getattr(s, "signal_name", None)
            for s in signals
        )

        if not has_bullish_today and decay_max_days > 0:
            lookback_start = date - dt.timedelta(days=decay_max_days + 1)
            recent = list(
                self.signal_model.objects(
                    stock_code=stock_code,
                    date__gte=normalize_date(lookback_start),
                    date__lt=normalize_date(date),
                ).order_by("-date")
            )
            # Group by date, find most recent date with bullish signals
            by_date: dict = {}
            for sig in recent:
                if getattr(sig, "direction", None) == "BULLISH" and getattr(
                    sig, "signal_name", None
                ):
                    d = getattr(sig, "date", None)
                    if d:
                        d_norm = d.replace(hour=0, minute=0, second=0, microsecond=0)
                        by_date.setdefault(d_norm, []).append(sig)

            if by_date:
                most_recent_date = max(by_date.keys())
                days_since_signal = (normalize_date(date) - most_recent_date).days
                if days_since_signal <= decay_max_days:
                    last_signal_strengths = [
                        float(getattr(s, "strength", 1.0) or 1.0)
                        for s in by_date[most_recent_date]
                    ]
                    last_signal_names = [
                        getattr(s, "signal_name", None)
                        for s in by_date[most_recent_date]
                    ]
                else:
                    days_since_signal = None

        components = [
            signal_strength_component(
                signals,
                weights["signal_strength"],
                days_since_signal=days_since_signal,
                last_signal_strengths=last_signal_strengths,
                last_signal_names=last_signal_names,
                decay_factor=decay_factor,
            ),
            trend_alignment_component(
                quote, factors, horizon, weights["trend_alignment"]
            ),
            momentum_component(
                quote,
                momentum_quotes,
                config["momentum_lookback"],
                weights["momentum"],
            ),
            breakout_or_position_component(
                quote, breakout_quotes, weights["breakout_or_position"]
            ),
            relative_strength_component(
                quote,
                history_quotes[: config["momentum_lookback"]],
                weights["relative_strength"],
            ),
            real_relative_strength_component(
                stock_code=stock_code,
                quote=quote,
                history_quotes=history_quotes,
                weight=weights.get("real_relative_strength", 0.0),
                lookback=config.get("momentum_lookback", 10),
            ),
            industry_momentum_component(
                stock_code=stock_code,
                date=date,
                horizon=horizon,
                weight=weights.get("industry_momentum", 0.0),
                model_version=self.model_version,
            ),
        ]
        penalties = [risk_penalty(quote, risk_quotes, weights["risk_penalty"])]
        return components, penalties

    def _get_horizon_config(self, horizon: int) -> dict:
        return get_effective_horizon_config(horizon, self.scoring_config)

    def _calculate_score(self, components: list, penalties: list) -> float:
        score = sum(item["contribution"] for item in components)
        score += sum(item["contribution"] for item in penalties)
        return round(max(0.0, min(100.0, score)), 2)

    def _recommendation(
        self, score: float, config: dict, percentile: float | None = None
    ) -> str:
        """Determine recommendation using hybrid absolute + percentile thresholds.

        When percentile is available (post-ranking), uses percentile as the primary
        driver with absolute thresholds as guards. Without percentile, falls back
        to pure absolute thresholds.
        """
        buy_abs = config["buy_threshold"]
        watch_abs = config["watch_threshold"]
        avoid_abs = config.get("avoid_threshold", 20.0)

        if percentile is not None and percentile > 0:
            buy_pct = config.get("buy_percentile", 0.95)
            watch_pct = config.get("watch_percentile", 0.80)
            avoid_pct = config.get("avoid_percentile", 0.20)

            # BUY: must pass BOTH absolute guard AND percentile threshold
            if score >= buy_abs and percentile >= buy_pct:
                return "BUY"
            # WATCH: either passes absolute guard with weaker percentile, or
            #        passes BUY absolute but not BUY percentile
            if score >= watch_abs and percentile >= watch_pct:
                return "WATCH"
            if score >= buy_abs and percentile < buy_pct:
                return "WATCH"
            # AVOID: bottom percentile OR below absolute floor
            if score <= avoid_abs or percentile <= avoid_pct:
                return "AVOID"
            return "NONE"
        else:
            # Fallback: pure absolute thresholds (used before ranking)
            if score >= buy_abs:
                return "WATCH"  # placeholder — will be upgraded post-ranking
            if score >= watch_abs:
                return "WATCH"
            if score <= avoid_abs:
                return "AVOID"
            return "NONE"

    def _build_explanation(self, horizon, score, components, penalties, config):
        positive = [
            item["label"]
            for item in components
            if item["contribution"] > 0 and item["weight"] > 0
        ]
        if positive:
            summary = "; ".join(positive[:3])
        else:
            summary = "No strong positive scoring evidence."
        return {
            "summary": summary,
            "horizon": horizon,
            "score": score,
            "components": components,
            "penalties": penalties,
            "thresholds": self._thresholds(config),
            "model_version": self.model_version,
        }

    def _thresholds(self, config):
        return {
            "buy": config["buy_threshold"],
            "watch": config["watch_threshold"],
            "avoid": config.get("avoid_threshold", 20.0),
            "effective_return": config["effective_threshold"],
            "stop_loss": config["stop_loss_threshold"],
        }

    def _build_input_snapshot(
        self, quote, factors, signals, history_quotes, config, blocked_reason
    ):
        quote_date = getattr(quote, "date", None)
        factor_date = getattr(factors, "date", None)
        signal_dates = [getattr(signal, "date", None) for signal in signals]
        has_enough_history = len(history_quotes) >= config["minimum_quote_count"]
        status = "READY" if quote and has_enough_history else "PARTIAL"
        if blocked_reason:
            status = "BLOCKED"
        return {
            "status": status,
            "blocked_reason": blocked_reason,
            "quote": {
                "status": "ready" if quote else "missing",
                "date": quote_date.isoformat() if quote_date else None,
            },
            "factor": {
                "status": "ready" if factors else "missing",
                "date": factor_date.isoformat() if factor_date else None,
            },
            "signals": {
                "status": "ready" if signals else "missing",
                "count": len(signals),
                "dates": [date.isoformat() for date in signal_dates if date],
            },
            "history": {
                "status": "ready" if has_enough_history else "partial",
                "quote_count": len(history_quotes),
                "minimum_quote_count": config["minimum_quote_count"],
            },
        }

    def _get_quote_on_date(self, stock_code, date):
        return self.quote_model.objects(
            code=stock_code, date=normalize_date(date)
        ).first()

    def _get_factor_on_date(self, stock_code, date):
        return self.factor_model.objects(
            stock_code=stock_code, date=normalize_date(date)
        ).first()

    def _get_signals_on_date(self, stock_code, date):
        return list(
            self.signal_model.objects(stock_code=stock_code, date=normalize_date(date))
        )

    def _get_previous_quotes(self, stock_code, date, limit):
        return list(
            self.quote_model.objects(code=stock_code, date__lt=normalize_date(date))
            .order_by("-date")
            .limit(limit)
        )
