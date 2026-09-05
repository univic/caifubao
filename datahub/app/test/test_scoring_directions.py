"""Tests for component-direction versioning in the scoring engine.

Construction-layer direction flips (research candidates like flip_wide) are
expressed as a per-component direction override in the scoring config:
- get_effective_horizon_config resolves "directions" into a per-component map
  with defaults (components +1, risk_penalty -1).
- score_all_stocks_ranked applies the resolved direction to each component's
  rank contribution, so a flipped direction makes a HIGH raw component value
  LOWER the final score (mean-reversion semantics) instead of raising it.
"""

from __future__ import annotations

import pytest

from app.lib.scoring_engine.config import (
    SCORING_CONFIG,
    get_effective_horizon_config,
)


def _direction_defaults(horizon: int) -> dict[str, int]:
    weights = SCORING_CONFIG[horizon]["weights"]
    return {key: (-1 if key == "risk_penalty" else 1) for key in weights}


class TestDirectionOverrideResolution:
    def test_no_override_keeps_default_directions(self):
        config = get_effective_horizon_config(20)
        assert "directions" not in config

    def test_flip_override_resolves_all_components(self):
        # flip_wide-style: all 7 positive components flipped to -1; risk keeps
        # its default -1 (already negative in the default polarity).
        flip = {"signal_strength": -1, "momentum": -1, "trend_alignment": -1}
        config = get_effective_horizon_config(20, {20: {"directions": flip}})
        resolved = config["directions"]
        defaults = _direction_defaults(20)
        assert resolved["signal_strength"] == -1
        assert resolved["trend_alignment"] == -1
        # untouched components keep defaults
        assert resolved["breakout_or_position"] == defaults["breakout_or_position"]
        assert resolved["risk_penalty"] == -1
        assert set(resolved) == set(defaults)

    def test_unknown_direction_key_raises(self):
        with pytest.raises(ValueError, match="direction override keys"):
            get_effective_horizon_config(20, {20: {"directions": {"bogus": -1}}})

    def test_invalid_direction_value_raises(self):
        with pytest.raises(ValueError, match="must be -1, 0, or 1"):
            get_effective_horizon_config(20, {20: {"directions": {"momentum": 5}}})

    def test_weights_and_directions_coexist(self):
        config = get_effective_horizon_config(
            20,
            {20: {"weights": {"momentum": 40.0}, "directions": {"momentum": -1}}},
        )
        assert config["weights"]["momentum"] == 40.0
        assert config["directions"]["momentum"] == -1

    def test_override_keyed_by_string_horizon(self):
        config = get_effective_horizon_config(
            5, {"5": {"directions": {"signal_strength": -1}}}
        )
        assert config["directions"]["signal_strength"] == -1

    def test_float_direction_value_rejected_not_truncated(self):
        with pytest.raises(ValueError, match="must be -1, 0, or 1"):
            get_effective_horizon_config(20, {20: {"directions": {"momentum": 0.5}}})

    def test_bool_direction_value_rejected(self):
        with pytest.raises(ValueError, match="must be -1, 0, or 1"):
            get_effective_horizon_config(20, {20: {"directions": {"momentum": True}}})

    def test_non_dict_directions_rejected(self):
        with pytest.raises(ValueError, match="must be a dict"):
            get_effective_horizon_config(20, {20: {"directions": ["momentum"]}})


class TestRankedDirectionFlip:
    """Construction-layer flip inverts which stock scores higher.

    With a positive direction, the stock whose momentum component is higher
    scores higher. With momentum flipped to -1 (flip_wide semantics), the same
    raw components must invert the ranking: high momentum now LOWERS the score.
    A FULL flip (every component -1) must keep scores signed and sortable -
    never collapse to an all-zero tie.
    """

    def _ranked_scores(
        self,
        monkeypatch,
        directions: dict[str, int] | None = None,
        *,
        risky_penalty: bool = False,
    ) -> dict[str, float]:
        import datetime
        from unittest.mock import MagicMock, patch

        from app.lib.scoring_engine.scoring_service import StockScoringService
        from app.test.test_scoring_service import (
            FakeFactor,
            FakePrediction,
            FakeQuote,
            FakeSignal,
            FakeStock,
        )

        for model in (FakeStock, FakeQuote, FakeFactor, FakeSignal, FakePrediction):
            model.records = []

        calendar = [datetime.datetime(2026, 4, 10, tzinfo=datetime.UTC)]
        for code in ("sh600000", "sh600001"):
            FakeStock.records.append(
                FakeStock(code=code, name=f"Stock {code}", active_status=0)
            )
            FakeQuote.records.append(
                FakeQuote(
                    code=code,
                    date=calendar[0],
                    close=10.0,
                    high=10.1,
                    low=9.9,
                    trade_status=1,
                    isST=0,
                )
            )

        # Deterministic raw components: A has higher momentum than B.
        def fake_components(stock, date, horizon):
            if stock.code == "sh600000":
                momentum, trend = 0.9, 0.8
            else:
                momentum, trend = 0.2, 0.3

            def comp(component_id, raw_value, weight):
                return {"id": component_id, "raw_value": raw_value, "weight": weight}

            penalties = []
            if risky_penalty:
                # A owns no penalty, B carries a high risk penalty whose raw
                # value is 0.9 (top of the cohort) - under DEFAULT directions
                # B's score would be negative before the floor clamp.
                penalties = [
                    {
                        "id": "risk_penalty",
                        "raw_value": 0.9 if stock.code == "sh600001" else 0.1,
                        "weight": 15.0,
                    }
                ]
            return {
                "stock_code": stock.code,
                "stock_name": stock.name,
                "base_price": 10.0,
                "target_date": date,
                "components": [
                    comp("momentum", momentum, 15.0),
                    comp("trend_alignment", trend, 30.0),
                    comp("signal_strength", 0.5, 15.0),
                    comp("breakout_or_position", 0.5, 5.0),
                    comp("relative_strength", momentum, 15.0),
                    comp("real_relative_strength", momentum, 10.0),
                ],
                "penalties": penalties,
            }

        with (
            patch(
                "app.lib.scoring_engine.scoring_service.FinanceMarket.objects"
            ) as mock_market_objs,
            patch(
                "app.lib.scoring_engine.scoring_service.industry_momentum_component",
                return_value={
                    "id": "industry_momentum",
                    "group": "industry",
                    "label": "Industry momentum",
                    "raw_value": None,
                    "normalized_value": 0.5,
                    "weight": 0.0,
                    "contribution": 0.0,
                    "direction": "positive",
                    "evidence": {},
                },
            ),
            patch(
                "app.lib.scoring_engine.scoring_service.aggregate_industry_metrics",
                return_value=[],
            ),
        ):
            mock_market = MagicMock()
            mock_market.trade_calendar = calendar
            mock_market_objs.return_value.first.return_value = mock_market
            scoring_config = {}
            if directions:
                scoring_config = {20: {"directions": dict(directions)}}
            service = StockScoringService(
                stock_model=FakeStock,
                quote_model=FakeQuote,
                factor_model=FakeFactor,
                signal_model=FakeSignal,
                prediction_model=FakePrediction,
                model_version="direction_test",
                scoring_config=scoring_config,
            )
            service.calendar = calendar
            service._compute_raw_components = fake_components
            service.score_all_stocks_ranked(
                date=datetime.datetime(2026, 4, 10, tzinfo=datetime.UTC),
                horizon=20,
            )
        scores = {
            prediction.stock_code: prediction.score
            for prediction in FakePrediction.records
        }
        assert len(scores) == 2, f"expected 2 scored predictions, got {scores}"
        return scores

    def test_default_direction_high_momentum_scores_higher(self, monkeypatch):
        scores = self._ranked_scores(monkeypatch)
        assert scores["sh600000"] > scores["sh600001"], (
            f"A(momentum 0.9) should outrank B(0.2): {scores}"
        )

    def test_flipped_direction_inverts_ranking(self, monkeypatch):
        scores = self._ranked_scores(
            monkeypatch,
            directions={
                "momentum": -1,
                "relative_strength": -1,
                "real_relative_strength": -1,
            },
        )
        assert scores["sh600000"] < scores["sh600001"], (
            f"flip should invert ranking: A high momentum scores lower: {scores}"
        )

    def test_full_flip_keeps_scores_sortable_and_inverts_ranking(self, monkeypatch):
        """flip_wide full flip (every component -1) must NOT collapse scores to
        a 0.0 tie: scores stay signed (negative) and strictly ordered, and the
        ranking is the exact inverse of the default-direction ranking.
        """
        flip_all = {
            "signal_strength": -1,
            "momentum": -1,
            "trend_alignment": -1,
            "breakout_or_position": -1,
            "relative_strength": -1,
            "real_relative_strength": -1,
            "risk_penalty": -1,
        }
        scores = self._ranked_scores(monkeypatch, directions=flip_all)
        assert scores["sh600000"] != scores["sh600001"], (
            f"full flip must not collapse to a tie (rank needs strict order): {scores}"
        )
        assert scores["sh600000"] < scores["sh600001"], (
            f"full flip should invert ranking: {scores}"
        )
        assert scores["sh600000"] < 0, (
            f"flipped score should stay signed (negative), got {scores}"
        )

    def test_default_direction_with_penalty_keeps_floor_zero(self, monkeypatch):
        """Default (no flip) model with a heavy risk penalty must keep the
        develop floor clamp: B's pre-clamp score is negative (component ranks
        below its penalty rank), but its stored score must be 0.0, not a
        negative value - default models are bit-identical to develop.
        """
        scores = self._ranked_scores(monkeypatch, risky_penalty=True)
        assert scores["sh600001"] == 0.0, (
            f"default model must floor risky stock to 0.0, got {scores}"
        )
        assert scores["sh600000"] >= 0.0

    def test_flip_with_penalty_keeps_signed_scores(self, monkeypatch):
        """A real component flip must open the floor even when a penalty is
        present: scores stay signed (possibly negative for penalized names)
        but strictly sortable, never clamped to a 0.0 tie.
        """
        flip_all = {
            "signal_strength": -1,
            "momentum": -1,
            "trend_alignment": -1,
            "breakout_or_position": -1,
            "relative_strength": -1,
            "real_relative_strength": -1,
            "risk_penalty": -1,
        }
        scores = self._ranked_scores(
            monkeypatch, directions=flip_all, risky_penalty=True
        )
        assert scores["sh600000"] != scores["sh600001"], (
            f"flip must keep strict order with penalties present: {scores}"
        )
        assert scores["sh600001"] <= 0.0, (
            f"penalized flipped name should stay non-positive: {scores}"
        )
