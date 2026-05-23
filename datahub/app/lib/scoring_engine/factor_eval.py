# -*- coding: utf-8 -*-
"""Factor evaluation service — IC, ICIR, quintile analysis, correlation, decay."""

import datetime
from collections import defaultdict
from statistics import correlation, mean, pstdev

from app.lib.market_regime import MarketRegimeService
from app.model.scoring import StockScorePrediction


class FactorEvaluationService:
    """Evaluate a candidate factor's predictive power."""

    def __init__(self, quote_model=None, factor_model=None):
        if quote_model is None:
            from app.model.stock import StockDailyQuote

            self.quote_model = StockDailyQuote
        else:
            self.quote_model = quote_model

        if factor_model is None:
            from app.model.factor import StockFactorDaily

            self.factor_model = StockFactorDaily
        else:
            self.factor_model = factor_model

    def evaluate(
        self,
        factor_values,
        start_date,
        end_date,
        forward_horizons=None,
        regime_split=False,
    ):
        """Run full evaluation pipeline on a factor.

        factor_values format: {stock_code: {date.isoformat(): float_value}}

        If regime_split=True, compute IC separately for bull/bear/sideways
        regimes using MarketRegimeService.  Adds a "regime_ic" key to the
        returned dict.
        """
        if forward_horizons is None:
            forward_horizons = [5, 20, 60]

        # Build aligned dataset: for each (stock, date) with a factor value,
        # get the forward return at each horizon
        dataset = self._build_dataset(
            factor_values, start_date, end_date, forward_horizons
        )

        ic_results = self._compute_ic(dataset, forward_horizons)
        quintile_results = self._compute_quintiles(dataset, forward_horizons)
        correlation_results = self._compute_component_correlation(
            factor_values, start_date, end_date
        )
        decay_results = self._compute_decay(factor_values, start_date, end_date)

        results = {
            "ic": ic_results,
            "icir": {
                str(h): round(
                    ic_results[str(h)]["ic_mean"]
                    / max(ic_results[str(h)]["ic_std"], 1e-6),
                    4,
                )
                for h in forward_horizons
                if ic_results.get(str(h), {}).get("ic_mean") is not None
            },
            "quintiles": quintile_results,
            "correlation": correlation_results,
            "decay": decay_results,
            "observation_count": len(dataset),
        }

        # Regime-split IC computation
        if regime_split:
            results["regime_ic"] = self._compute_regime_ic(dataset, forward_horizons)

        return results

    def _build_dataset(self, factor_values, start_date, end_date, horizons):
        """Build list of {factor_value, {horizon: forward_return}} dicts."""
        dataset = []
        date_start = start_date.replace(hour=0, minute=0, second=0, microsecond=0)
        date_end = end_date.replace(hour=0, minute=0, second=0, microsecond=0)

        for stock_code, date_values in factor_values.items():
            for date_str, factor_val in date_values.items():
                try:
                    date = datetime.datetime.fromisoformat(date_str)
                except (ValueError, TypeError):
                    continue
                date = date.replace(hour=0, minute=0, second=0, microsecond=0)
                if date < date_start or date > date_end:
                    continue
                if factor_val is None:
                    continue

                # Get base close price
                base_quote = self.quote_model.objects(
                    code=stock_code, date=date
                ).first()
                if not base_quote:
                    continue
                base_close = base_quote.close_hfq or base_quote.close
                if not base_close or base_close <= 0:
                    continue

                # Get forward close at each horizon
                forward_returns = {}
                for h in horizons:
                    target_date = date + datetime.timedelta(days=int(h * 1.5))
                    future_quotes = list(
                        self.quote_model.objects(
                            code=stock_code,
                            date__gt=date,
                            date__lte=target_date,
                        )
                        .order_by("date")
                        .limit(h)
                    )
                    if len(future_quotes) >= h:
                        target_quote = future_quotes[-1]
                        target_close = target_quote.close_hfq or target_quote.close
                        if target_close and target_close > 0:
                            forward_returns[str(h)] = (
                                target_close - base_close
                            ) / base_close
                            continue
                    forward_returns[str(h)] = None

                dataset.append(
                    {
                        "stock_code": stock_code,
                        "date": date,
                        "factor_value": float(factor_val),
                        "forward_returns": forward_returns,
                    }
                )

        return dataset

    def _compute_ic(self, dataset, horizons):
        """Compute rank IC per horizon (Spearman rank correlation)."""
        results = {}
        for h in horizons:
            h_str = str(h)
            valid = [d for d in dataset if d["forward_returns"].get(h_str) is not None]
            if len(valid) < 10:
                results[h_str] = {
                    "ic_mean": None,
                    "ic_std": None,
                    "ic_values": [],
                    "count": len(valid),
                }
                continue

            # Group by date for IC time-series
            by_date = defaultdict(list)
            for d in valid:
                by_date[d["date"]].append(d)

            ic_values = []
            for _date, day_data in sorted(by_date.items()):
                if len(day_data) < 5:
                    continue
                day_factors = [dd["factor_value"] for dd in day_data]
                day_returns = [dd["forward_returns"][h_str] for dd in day_data]
                day_ic = _spearman_rank(day_factors, day_returns)
                if day_ic is not None:
                    ic_values.append(
                        {"date": _date.isoformat(), "ic": round(day_ic, 6)}
                    )

            ic_vals = [v["ic"] for v in ic_values]
            results[h_str] = {
                "ic_mean": round(mean(ic_vals), 6) if ic_vals else None,
                "ic_std": (round(pstdev(ic_vals), 6) if len(ic_vals) > 1 else 0.0),
                "ic_values": ic_vals[-30:],  # last 30 days
                "count": len(valid),
            }

        return results

    def _compute_regime_ic(self, dataset, horizons):
        """Compute IC split by market regime (bull / bear / sideways)."""
        regime_svc = MarketRegimeService()

        # Tag each observation with its regime
        for d in dataset:
            d["regime"] = regime_svc.classify_date(d["date"])

        regime_ic = {}
        all_regimes = ["bull", "bear", "sideways", "unknown"]
        for regime_name in all_regimes:
            regime_dataset = [d for d in dataset if d.get("regime") == regime_name]
            if len(regime_dataset) >= 10:
                regime_ic[regime_name] = self._compute_ic(regime_dataset, horizons)
            else:
                regime_ic[regime_name] = {
                    str(h): {
                        "ic_mean": None,
                        "ic_std": None,
                        "ic_values": [],
                        "count": len(regime_dataset),
                    }
                    for h in horizons
                }

        return regime_ic

    def _compute_quintiles(self, dataset, horizons):
        """Group by factor quintile and compute mean forward return."""
        results = {}
        for h in horizons:
            h_str = str(h)
            valid = [d for d in dataset if d["forward_returns"].get(h_str) is not None]
            if len(valid) < 20:
                results[h_str] = []
                continue

            sorted_data = sorted(valid, key=lambda d: d["factor_value"])
            n = len(sorted_data)
            quintile_size = n // 5

            quintile_results = []
            for q in range(5):
                start_idx = q * quintile_size
                end_idx = (q + 1) * quintile_size if q < 4 else n
                group = sorted_data[start_idx:end_idx]
                group_returns = [
                    d["forward_returns"][h_str]
                    for d in group
                    if d["forward_returns"][h_str] is not None
                ]
                avg_factor = mean(d["factor_value"] for d in group) if group else 0
                quintile_results.append(
                    {
                        "quintile": q + 1,
                        "count": len(group),
                        "avg_factor_value": round(avg_factor, 6),
                        "avg_return": (
                            round(mean(group_returns), 6) if group_returns else None
                        ),
                    }
                )

            results[h_str] = quintile_results

        return results

    def _compute_component_correlation(self, factor_values, start_date, end_date):
        """Correlate this factor with existing scoring components.

        Queries StockScorePrediction explanation for component scores.
        Returns pairwise Pearson correlations.
        """
        # Get all predictions in date range
        predictions = list(
            StockScorePrediction.objects(
                date__gte=start_date.replace(hour=0, minute=0, second=0, microsecond=0),
                date__lte=end_date.replace(hour=0, minute=0, second=0, microsecond=0),
            )
        )

        # Build component scores per stock/date: component_id -> {key: value}
        component_scores = defaultdict(
            lambda: {}
        )  # component_id -> {stock_date_key: normalized_value}
        for pred in predictions:
            exp = pred.explanation or {}
            for comp in exp.get("components", []):
                comp_id = comp.get("id")
                if comp_id:
                    key = f"{pred.stock_code}:{pred.date.isoformat()}"
                    norm = comp.get("normalized_value", 0) or 0
                    component_scores[comp_id][key] = norm

        # For each component, correlate with factor values
        correlations = {}
        for comp_id, comp_map in component_scores.items():
            paired = []
            for key, comp_val in comp_map.items():
                stock_code, date_str = key.split(":", 1)
                fv = factor_values.get(stock_code, {}).get(date_str)
                if fv is not None:
                    paired.append((float(fv), float(comp_val)))

            if len(paired) >= 10:
                fv_list = [p[0] for p in paired]
                cv_list = [p[1] for p in paired]
                try:
                    corr = correlation(fv_list, cv_list)
                    correlations[comp_id] = round(corr, 4)
                except Exception:
                    correlations[comp_id] = None
            else:
                correlations[comp_id] = None

        return correlations

    def _compute_decay(self, factor_values, start_date, end_date):
        """IC decay curve over horizons 1, 3, 5, 10, 20, 60."""
        decay_horizons = [1, 3, 5, 10, 20, 60]
        dataset = self._build_dataset(
            factor_values, start_date, end_date, decay_horizons
        )
        ic_results = self._compute_ic(dataset, decay_horizons)
        return {
            str(h): ic_results.get(str(h), {}).get("ic_mean") for h in decay_horizons
        }

    # ------------------------------------------------------------------
    # P&L attribution & dominant-component analysis (15.7–15.8)
    # ------------------------------------------------------------------

    def evaluate_component_contribution(
        self, stock_code, start_date, end_date, horizon=20
    ):
        """Compute average per-component scores for entry and exit predictions.

        Entry predictions are those with recommendation == "BUY"; exit
        predictions are those with recommendation == "AVOID".

        Returns a dict with keys "entry_contributions", "exit_contributions",
        "dominant_entry_component", and "dominant_exit_component".
        """
        predictions = list(
            StockScorePrediction.objects(
                stock_code=stock_code,
                date__gte=start_date.replace(hour=0, minute=0, second=0, microsecond=0),
                date__lte=end_date.replace(hour=0, minute=0, second=0, microsecond=0),
                horizon=horizon,
                status="VERIFIED",
            )
        )

        entry_scores = defaultdict(list)
        exit_scores = defaultdict(list)

        for pred in predictions:
            exp = pred.explanation or {}
            components = exp.get("components", [])
            if not components:
                continue

            target = entry_scores if pred.recommendation == "BUY" else exit_scores
            for comp in components:
                comp_id = comp.get("id")
                if comp_id:
                    norm = comp.get("normalized_value", 0) or 0
                    target[comp_id].append(float(norm))

        entry_contributions = {}
        exit_contributions = {}
        dominant_entry = None
        dominant_exit = None
        best_entry_score = -float("inf")
        best_exit_score = -float("inf")

        for comp_id, scores in entry_scores.items():
            avg = round(mean(scores), 4) if scores else 0.0
            entry_contributions[comp_id] = avg
            if avg > best_entry_score:
                best_entry_score = avg
                dominant_entry = comp_id

        for comp_id, scores in exit_scores.items():
            avg = round(mean(scores), 4) if scores else 0.0
            exit_contributions[comp_id] = avg
            if avg > best_exit_score:
                best_exit_score = avg
                dominant_exit = comp_id

        return {
            "entry_contributions": entry_contributions,
            "exit_contributions": exit_contributions,
            "dominant_entry_component": dominant_entry,
            "dominant_exit_component": dominant_exit,
        }

    def win_rate_by_component(self, stock_code, start_date, end_date, horizon=20):
        """Compute win rate grouped by the dominant (highest-scoring) component.

        For each VERIFIED prediction, the component with the highest
        normalized_value is the "dominant" component.  Predictions are
        grouped by their dominant component and the win rate
        (hit_target_close == True) is computed per group.

        Returns a dict: {component_id: {"trades": N, "win_rate": 0.XX}, ...}
        """
        predictions = list(
            StockScorePrediction.objects(
                stock_code=stock_code,
                date__gte=start_date.replace(hour=0, minute=0, second=0, microsecond=0),
                date__lte=end_date.replace(hour=0, minute=0, second=0, microsecond=0),
                horizon=horizon,
                status="VERIFIED",
            )
        )

        component_trades = defaultdict(list)

        for pred in predictions:
            exp = pred.explanation or {}
            components = exp.get("components", [])
            if not components:
                continue

            # Find dominant component (highest normalized_value)
            dominant_comp = None
            best_score = -float("inf")
            for comp in components:
                comp_id = comp.get("id")
                if comp_id:
                    norm = comp.get("normalized_value", 0) or 0
                    if float(norm) > best_score:
                        best_score = float(norm)
                        dominant_comp = comp_id

            if not dominant_comp:
                continue

            verification = pred.verification or {}
            hit = verification.get("hit_target_close", False)
            component_trades[dominant_comp].append(hit)

        results = {}
        for comp_id, outcomes in component_trades.items():
            wins = sum(1 for o in outcomes if o)
            total = len(outcomes)
            results[comp_id] = {
                "trades": total,
                "win_rate": round(wins / total, 4) if total > 0 else None,
            }

        return results


def _spearman_rank(x, y):
    """Compute Spearman rank correlation."""
    if len(x) < 3:
        return None

    # Rank x
    x_ranked = sorted(range(len(x)), key=lambda i: x[i])
    x_ranks = [0] * len(x)
    for rank, idx in enumerate(x_ranked):
        x_ranks[idx] = rank + 1

    # Rank y
    y_ranked = sorted(range(len(y)), key=lambda i: y[i])
    y_ranks = [0] * len(y)
    for rank, idx in enumerate(y_ranked):
        y_ranks[idx] = rank + 1

    try:
        return correlation(x_ranks, y_ranks)
    except Exception:
        return None
