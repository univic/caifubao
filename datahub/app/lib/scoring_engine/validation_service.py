# -*- coding: utf-8 -*-
"""Walk-forward validation service with regime analysis, stability checks,
and best-config recommendation.

Used by:
  - compute-worker rolling-validation handler (regime split, 17.3)
  - autoresearch / experiment grid search (stability, recommendation, 17.4 / 17.7)
"""

import datetime
import logging

from app.lib.market_regime import MarketRegimeService

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Composite-score constants (mirrors backend/app/services/backtest_service.py)
# ---------------------------------------------------------------------------
COMPOSITE_EXCESS_WEIGHT = 1.0
COMPOSITE_DRAWDOWN_PENALTY = 0.5
COMPOSITE_IR_WEIGHT = 2.0
COMPOSITE_TURNOVER_PENALTY = 0.1
COMPOSITE_CONCENTRATION_PENALTY = 1.0
MIN_TRADES_FOR_RANKING = 5
MIN_TRADING_DAYS = 120
CONCENTRATION_THRESHOLD = 0.40


def _composite_score_from_metrics(metrics: dict) -> dict:
    """Compute a composite ranking score from experiment-level metrics.

    Uses the same formula as ``backend/app/services/backtest_service.py``.

    Expected keys in *metrics*:
      - excess_return_pct (float)
      - max_drawdown (float, positive percentage)
      - information_ratio (float)
      - total_trades (int)
      - daily_values (list, optional)
      - trades (list of dict, optional — used for concentration check)
    """
    trades = metrics.get("trades") or []
    total_trades = metrics.get("total_trades", 0)
    if total_trades < MIN_TRADES_FOR_RANKING:
        return {"score": -999.0, "flags": ["low_sample"], "rankable": False}

    excess = metrics.get("excess_return_pct") or 0.0
    dd = abs(metrics.get("max_drawdown") or 0.0)
    ir = metrics.get("information_ratio") or 0.0

    base = excess * COMPOSITE_EXCESS_WEIGHT
    ir_contrib = ir * COMPOSITE_IR_WEIGHT

    # Non-linear drawdown penalty above 20 %
    dd_penalty = dd * COMPOSITE_DRAWDOWN_PENALTY
    if dd > 20:
        dd_penalty = (
            20 * COMPOSITE_DRAWDOWN_PENALTY + (dd - 20) * COMPOSITE_DRAWDOWN_PENALTY * 2
        )

    # Turnover penalty
    daily_values = metrics.get("daily_values") or []
    days = max(len(daily_values), 1)
    trade_rate = total_trades / days
    turnover_penalty = trade_rate * COMPOSITE_TURNOVER_PENALTY

    # Concentration penalty
    concentration_penalty = 0.0
    single_best_contrib = 0.0
    flags: list = []
    if trades:
        total_pnl = sum((t.get("pnl") or 0) for t in trades if t.get("side") == "SELL")
        if total_pnl > 0:
            best_pnl = max(
                (t.get("pnl") or 0) for t in trades if t.get("side") == "SELL"
            )
            single_best_contrib = best_pnl / total_pnl
            if single_best_contrib > CONCENTRATION_THRESHOLD:
                concentration_penalty = (
                    single_best_contrib * COMPOSITE_CONCENTRATION_PENALTY
                )
                flags.append(f"concentrated_returns:{single_best_contrib:.0%}")

    if dd > 30:
        flags.append("high_drawdown")
    if days < MIN_TRADING_DAYS:
        flags.append("insufficient_period")

    score = base + ir_contrib - dd_penalty - turnover_penalty - concentration_penalty

    return {
        "score": round(score, 4),
        "rankable": True,
        "flags": flags,
        "breakdown": {
            "excess_contrib": round(base, 4),
            "ir_contrib": round(ir_contrib, 4),
            "dd_penalty": round(dd_penalty, 4),
            "turnover_penalty": round(turnover_penalty, 4),
            "concentration_penalty": round(concentration_penalty, 4),
            "single_best_pnl_pct": round(single_best_contrib, 4),
        },
    }


# ---------------------------------------------------------------------------
# ValidationService
# ---------------------------------------------------------------------------


class ValidationService:
    """Walk-forward validation with regime analysis, stability checks,
    and best-config recommendation.
    """

    def __init__(self, regime_service: MarketRegimeService | None = None):
        """*regime_service* allows injecting a pre-configured instance."""
        self._regime_service = regime_service

    @property
    def regime_service(self) -> MarketRegimeService:
        if self._regime_service is None:
            self._regime_service = MarketRegimeService()
        return self._regime_service

    # ------------------------------------------------------------------
    # 17.3  Regime-split report
    # ------------------------------------------------------------------

    def regime_split_report(
        self,
        rolling_results: list[dict],
        start_date: datetime.datetime | None = None,
        end_date: datetime.datetime | None = None,
    ) -> dict:
        """Split rolling validation results by market regime.

        **rolling_results** is the list of window dicts produced by
        ``_handle_rolling_validation``.  Each window has ``train_year``,
        ``test_year``, and a ``horizons`` dict keyed by horizon string with
        ``train_hit_rate``, ``test_hit_rate``, ``decay``, and ``overfit``.

        Each window's test period is classified by its *dominant* regime
        (the regime that occupies the most days).  Metrics are then
        aggregated per regime.

        Returns::

            {
              "regimes": {
                "bull":    {"window_count": N, "avg_train_hit": ..., "avg_test_hit": ...},
                "bear":    { ... },
                "sideways":{ ... },
              },
              "windows": [
                {..., "dominant_regime": "bull", ...},
                ...
              ]
            }
        """
        from collections import Counter

        if not rolling_results:
            return {"regimes": {}, "windows": [], "error": "no rolling results"}

        # Determine the overall date range for regime classification.
        if start_date is None:
            start_date = datetime.datetime(
                min(w["train_year"] for w in rolling_results), 1, 1
            )
        if end_date is None:
            end_date = datetime.datetime(
                max(w["test_year"] for w in rolling_results), 12, 31
            )

        # Pre-load regime data for the full range.
        regime_map = self.regime_service.classify_range(start_date, end_date)

        # Aggregate per-regime accumulators (keyed by horizon).
        regime_accum: dict[str, dict[str, list]] = {
            r: {"train_hit": [], "test_hit": [], "overfit_count": 0}
            for r in ("bull", "bear", "sideways", "unknown")
        }

        windows_out: list[dict] = []
        for window in rolling_results:
            test_year = window["test_year"]

            # --- Determine dominant regime in the test year -------------------
            test_start = datetime.datetime(test_year, 1, 1)
            test_end = datetime.datetime(test_year, 12, 31)

            regime_counts: Counter = Counter()
            current = test_start
            while current <= test_end:
                day_str = current.isoformat()  # matches classify_range() key format
                regime = regime_map.get(day_str, "unknown")
                regime_counts[regime] += 1
                current += datetime.timedelta(days=1)

            dominant_regime = (
                regime_counts.most_common(1)[0][0] if regime_counts else "unknown"
            )

            # --- Collect horizon-level metrics --------------------------------
            window_out = dict(window)
            window_out["dominant_regime"] = dominant_regime
            window_out["regime_distribution"] = dict(regime_counts)

            horizons = window.get("horizons", {})
            for h_str, h_metrics in horizons.items():
                regime_accum[dominant_regime]["train_hit"].append(
                    h_metrics.get("train_hit_rate", 0)
                )
                regime_accum[dominant_regime]["test_hit"].append(
                    h_metrics.get("test_hit_rate", 0)
                )
                if h_metrics.get("overfit"):
                    regime_accum[dominant_regime]["overfit_count"] += 1

            windows_out.append(window_out)

        # --- Build per-regime summary ----------------------------------------
        regimes: dict = {}
        for regime, acc in regime_accum.items():
            n = len(acc["train_hit"])
            if n == 0:
                if regime != "unknown":
                    regimes[regime] = {
                        "window_count": 0,
                        "avg_train_hit": None,
                        "avg_test_hit": None,
                        "overfit_count": 0,
                    }
                continue
            regimes[regime] = {
                "window_count": n,
                "avg_train_hit": round(sum(acc["train_hit"]) / n, 6),
                "avg_test_hit": round(sum(acc["test_hit"]) / n, 6),
                "overfit_count": acc["overfit_count"],
            }

        return {"regimes": regimes, "windows": windows_out}

    # ------------------------------------------------------------------
    # 17.4  Stability check
    # ------------------------------------------------------------------

    def stability_check(
        self,
        config: dict,
        perturbation_pct: float = 5.0,
    ) -> dict:
        """Evaluate how sensitive a scoring config is to small weight changes.

        For every weight component, perturbs it up *and* down by
        *perturbation_pct* percentage points while scaling the other
        components proportionally so that the total remains 100.

        Returns::

            {
              "stable": bool,
              "perturbations": [
                {"component": "momentum", "direction": "up",
                 "original_weight": 25, "perturbed_weight": 30,
                 "score_delta_pct": 8.3},
                ...
              ],
              "max_delta_pct": 12.5,
            }
        """
        from copy import deepcopy

        horizon_key = None
        weights_orig: dict = {}

        # Extract the first horizon's weights – we check one representative horizon.
        for key, value in config.items():
            if isinstance(value, dict) and "weights" in value:
                horizon_key = key
                weights_orig = dict(value["weights"])
                break

        if not weights_orig:
            return {
                "stable": True,
                "perturbations": [],
                "max_delta_pct": 0.0,
                "error": "no weights found in config",
            }

        baseline_score = self._compute_config_score(config)
        if baseline_score is None:
            return {
                "stable": True,
                "perturbations": [],
                "max_delta_pct": 0.0,
                "error": "could not compute baseline composite score",
            }

        total_orig = sum(weights_orig.values())
        if abs(total_orig - 100) > 0.5:
            logger.warning(
                "stability_check: baseline weights sum to %.1f, not 100", total_orig
            )

        perturbations: list[dict] = []
        max_delta = 0.0
        unstable = False

        for comp, w_orig in weights_orig.items():
            if w_orig == 0:
                continue

            for direction in ("up", "down"):
                delta = perturbation_pct
                if direction == "down":
                    delta = -perturbation_pct
                w_new = w_orig + delta
                if w_new < 0:
                    w_new = 0

                # Build perturbed weights preserving sum ~100.
                perturbed = dict(weights_orig)
                perturbed[comp] = w_new

                # Scale all other components so total remains target.
                other_total = total_orig - w_orig
                if other_total > 0:
                    remaining = 100.0 - w_new
                    scale = remaining / other_total if other_total > 0 else 1.0
                    for k, v in perturbed.items():
                        if k != comp:
                            perturbed[k] = round(v * scale, 1)
                else:
                    # Only one component — just set it.
                    perturbed[comp] = 100.0

                # Rebuild config for this single horizon.
                test_config = deepcopy(config)
                if horizon_key is not None:
                    test_config[horizon_key]["weights"] = perturbed

                test_score = self._compute_config_score(test_config)
                if test_score is None:
                    continue

                delta_pct = (
                    (test_score["score"] - baseline_score["score"])
                    / max(abs(baseline_score["score"]), 0.001)
                    * 100
                )

                perturbations.append(
                    {
                        "component": comp,
                        "direction": direction,
                        "original_weight": w_orig,
                        "perturbed_weight": round(w_new, 1),
                        "score_delta_pct": round(delta_pct, 2),
                    }
                )

                abs_delta = abs(delta_pct)
                if abs_delta > max_delta:
                    max_delta = abs_delta
                if abs_delta > 20:
                    unstable = True

        return {
            "stable": not unstable,
            "perturbations": perturbations,
            "max_delta_pct": round(max_delta, 2),
        }

    # ------------------------------------------------------------------
    # 17.7  Best-config recommendation
    # ------------------------------------------------------------------

    def best_config_recommendation(
        self,
        horizon: int,
        experiments: list[dict],
    ) -> dict:
        """Rank experiments by composite score and return the best
        recommendation for *horizon*.

        *experiments* is a list of experiment dicts.  Each dict should
        provide at least::

          {"config": ..., "composite_score": ..., "walk_forward_decay": ...,
           "metrics": {"excess_return_pct": ...}}

        Walk-forward decay > 20 % causes the experiment to be flagged as
        overfit and excluded from top-ranking positions.

        Returns::

          {
            "horizon": 20,
            "recommendation": {
              "model_version": "...", "config": {...}, "composite_score": ...,
              "confidence_interval": {"lower": ..., "upper": ..., "confidence": 0.95},
              "regime_robustness": {"bull": ..., "bear": ..., "sideways": ...}
            },
            "alternatives": [...],
            "overfit_count": N,
          }
        """

        # --- Filter & rank ---------------------------------------------------
        ranked: list[dict] = []
        overfit_count = 0

        for exp in experiments:
            decay = exp.get("walk_forward_decay", exp.get("decay", 0))
            if isinstance(decay, (int, float)) and abs(decay) > 0.20:
                overfit_count += 1
                continue

            cs = exp.get("composite_score")
            if cs is None:
                metrics = exp.get("metrics") or {}
                cs = _composite_score_from_metrics(metrics)
            # Normalize: cs may be a number or a dict with "score" key
            if isinstance(cs, (int, float)):
                cs = {
                    "score": float(cs),
                    "flags": [],
                    "breakdown": {},
                    "rankable": True,
                }
            ranked.append({"experiment": exp, "composite": cs})

        # Sort descending by composite score.
        ranked.sort(
            key=lambda x: (
                x["composite"].get("score", -999)
                if isinstance(x["composite"], dict)
                else float(x["composite"] or -999)
            ),
            reverse=True,
        )

        # --- Top-3 -----------------------------------------------------------
        recommendation = None
        alternatives: list[dict] = []

        for idx, entry in enumerate(ranked[:3]):
            exp = entry["experiment"]
            cs_raw = entry["composite"]
            cs = cs_raw if isinstance(cs_raw, dict) else {"score": float(cs_raw or 0)}

            item = {
                "model_version": exp.get("model_version", ""),
                "config": exp.get("config", {}),
                "composite_score": cs.get("score"),
                "composite_breakdown": cs.get("breakdown"),
                "composite_flags": cs.get("flags", []),
                "confidence_interval": self._bootstrap_ci(exp),
                "regime_robustness": self._regime_robustness(exp),
            }
            if idx == 0:
                recommendation = item
            else:
                alternatives.append(item)

        return {
            "horizon": horizon,
            "recommendation": recommendation,
            "alternatives": alternatives,
            "overfit_count": overfit_count,
            "total_experiments": len(experiments),
        }

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _compute_config_score(self, config: dict) -> dict | None:
        """Compute a synthetic composite score for *config*.

        Since we cannot run a full backtest here, we derive a rough
        signal-quality score from the weight distribution and implied
        calibration metrics.  Returns None when insufficient data.

        This is a *cost function proxy*: higher = likely better config.
        """
        weights: dict = {}
        for h_cfg in config.values():
            if isinstance(h_cfg, dict) and "weights" in h_cfg:
                weights = h_cfg["weights"]
                break

        if not weights:
            return None

        # Proxy: reward well-distributed weights with reasonable coverage.
        active = sum(1 for v in weights.values() if v > 0)
        total_nodes = len(weights)
        diversity = active / max(total_nodes, 1)

        # Penalise extreme concentration on a single component.
        max_weight = max(weights.values()) if weights else 0
        concentration = 100 * max_weight / max(sum(weights.values()), 1)

        # Simple heuristic score — higher is favourable.
        score = (
            diversity * 50.0 - (concentration - 40) * 0.5
            if concentration > 40
            else diversity * 50.0
        )

        return {
            "score": round(score, 4),
            "rankable": True,
            "flags": [],
            "breakdown": {
                "diversity": round(diversity, 4),
                "concentration_pct": round(concentration, 1),
            },
        }

    def _bootstrap_ci(self, exp: dict) -> dict | None:
        """Return a bootstrap-style CI if the experiment provides
        per-period return data; otherwise None."""
        returns = exp.get("period_returns") or exp.get("returns")
        if not returns or len(returns) < 5:
            return None
        import random

        random.seed(42)
        n = len(returns)
        iterations = min(1000, max(100, n * 10))
        means: list[float] = []
        for _ in range(iterations):
            sample = [random.choice(returns) for _ in range(n)]
            means.append(sum(sample) / n)
        means.sort()
        # Percentile indices, clamped to available range
        lo_idx = max(0, min(len(means) - 1, int(len(means) * 0.025)))
        hi_idx = max(0, min(len(means) - 1, int(len(means) * 0.975)))
        return {
            "lower": round(means[lo_idx], 6),
            "upper": round(means[hi_idx], 6),
            "confidence": 0.95,
        }

    def _regime_robustness(self, exp: dict) -> dict | None:
        """Extract regime-level robustness from experiment data, if present."""
        rr = exp.get("regime_results") or exp.get("regime_robustness")
        if isinstance(rr, dict):
            return rr
        return None
