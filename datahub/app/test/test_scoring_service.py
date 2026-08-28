# -*- coding: utf-8 -*-

import datetime
from unittest.mock import MagicMock, patch

import pytest

from app.lib.scoring_engine.calibration_report import ScoreCalibrationReport
from app.lib.scoring_engine.replay_service import ScoreReplayService
from app.lib.scoring_engine.scoring_service import StockScoringService
from app.lib.scoring_engine.verification_service import ScoreVerificationService
from app.model.scoring import StockScorePrediction


class FakeQuerySet:
    def __init__(self, items):
        self.items = list(items)

    def first(self):
        return self.items[0] if self.items else None

    def order_by(self, *fields):
        items = self.items
        for field in reversed(fields):
            reverse = field.startswith("-")
            key = field[1:] if reverse else field
            items = sorted(items, key=lambda item: getattr(item, key), reverse=reverse)
        return FakeQuerySet(items)

    def limit(self, count):
        return FakeQuerySet(self.items[:count])

    def count(self):
        return len(self.items)

    def __iter__(self):
        return iter(self.items)


class FakeModel:
    records = []

    def __init_subclass__(cls):
        cls.records = []

    def __init__(self, **kwargs):
        for key, value in kwargs.items():
            setattr(self, key, value)

    @classmethod
    def objects(cls, **query):
        return FakeQuerySet(
            [record for record in cls.records if matches_query(record, query)]
        )

    def save(self):
        if self not in self.__class__.records:
            self.__class__.records.append(self)
        return self


class FakeStock(FakeModel):
    pass


class FakeQuote(FakeModel):
    pass


class FakeFactor(FakeModel):
    pass


class FakeSignal(FakeModel):
    pass


class FakePrediction(FakeModel):
    pass


def matches_query(record, query):
    for key, expected in query.items():
        if key.endswith("__lt"):
            if not getattr(record, key[:-4]) < expected:
                return False
        elif key.endswith("__lte"):
            if not getattr(record, key[:-5]) <= expected:
                return False
        elif key.endswith("__gt"):
            if not getattr(record, key[:-4]) > expected:
                return False
        elif key.endswith("__gte"):
            if not getattr(record, key[:-5]) >= expected:
                return False
        elif key.endswith("__ne"):
            if getattr(record, key[:-4]) == expected:
                return False
        elif key.endswith("__in"):
            if getattr(record, key[:-4]) not in expected:
                return False
        elif getattr(record, key, None) != expected:
            return False
    return True


@pytest.fixture
def calendar():
    return [
        datetime.datetime(2026, 4, 10, tzinfo=datetime.UTC),
        datetime.datetime(2026, 4, 13, tzinfo=datetime.UTC),
        datetime.datetime(2026, 4, 14, tzinfo=datetime.UTC),
        datetime.datetime(2026, 4, 15, tzinfo=datetime.UTC),
        datetime.datetime(2026, 4, 16, tzinfo=datetime.UTC),
        datetime.datetime(2026, 4, 17, tzinfo=datetime.UTC),
        datetime.datetime(2026, 4, 20, tzinfo=datetime.UTC),
        datetime.datetime(2026, 4, 21, tzinfo=datetime.UTC),
        datetime.datetime(2026, 4, 22, tzinfo=datetime.UTC),
        datetime.datetime(2026, 4, 23, tzinfo=datetime.UTC),
        datetime.datetime(2026, 4, 24, tzinfo=datetime.UTC),
    ]


@pytest.fixture
def scoring_service(calendar):
    for model in (FakeStock, FakeQuote, FakeFactor, FakeSignal, FakePrediction):
        model.records = []

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
        service = StockScoringService(
            stock_model=FakeStock,
            quote_model=FakeQuote,
            factor_model=FakeFactor,
            signal_model=FakeSignal,
            prediction_model=FakePrediction,
        )
        service.calendar = calendar
        yield service


def seed_stock():
    stock = FakeStock(code="sh600000", name="浦发银行", active_status=0)
    FakeStock.records.append(stock)
    return stock


def seed_quotes(stock_code="sh600000"):
    start = datetime.datetime(2026, 3, 1, tzinfo=datetime.UTC)
    for idx in range(40):
        FakeQuote.records.append(
            FakeQuote(
                code=stock_code,
                date=start + datetime.timedelta(days=idx),
                close=8.0 + idx * 0.05,
                high=8.1 + idx * 0.05,
                low=7.9 + idx * 0.05,
                trade_status=1,
                isST=0,
            )
        )

    evaluation_date = datetime.datetime(2026, 4, 10, tzinfo=datetime.UTC)
    FakeQuote.records.append(
        FakeQuote(
            code=stock_code,
            date=evaluation_date,
            close=10.0,
            high=10.2,
            low=9.8,
            trade_status=1,
            isST=0,
        )
    )
    return evaluation_date


def seed_factors_and_signal(date, stock_code="sh600000"):
    FakeFactor.records.append(
        FakeFactor(
            stock_code=stock_code,
            date=date,
            ma_20=9.5,
            ma_60=9.0,
            ma_120=8.5,
        )
    )
    FakeSignal.records.append(
        FakeSignal(
            stock_code=stock_code,
            date=date,
            signal_name="MA10_CROSS_MA20",
            direction="BULLISH",
            strength=1.0,
            reason="MA10 crosses MA20",
        )
    )


def test_stock_score_prediction_model_shape():
    assert StockScorePrediction._get_collection_name() == "stock_score_predictions"
    assert StockScorePrediction._fields["stock_code"].required is True
    assert StockScorePrediction._fields["date"].required is True
    assert StockScorePrediction._fields["horizon"].required is True
    assert StockScorePrediction._fields["model_version"].required is True
    assert StockScorePrediction._fields["model_version"].default == "score_v2_202604"
    assert type(StockScorePrediction._fields["explanation"]).__name__ == "DictField"
    assert type(StockScorePrediction._fields["verification"]).__name__ == "DictField"
    assert type(StockScorePrediction._fields["input_snapshot"]).__name__ == "DictField"


def test_get_t_plus_n_day(scoring_service):
    start = datetime.datetime(2026, 4, 10, tzinfo=datetime.UTC)
    assert scoring_service.get_t_plus_n_day(start, 5) == datetime.datetime(
        2026, 4, 17, tzinfo=datetime.UTC
    )


def test_score_single_stock_creates_horizon_prediction(scoring_service):
    stock = seed_stock()
    date = seed_quotes()
    seed_factors_and_signal(date)

    prediction = scoring_service.score_single_stock(stock, date, 5)

    assert prediction.horizon == 5
    assert prediction.target_date == datetime.datetime(2026, 4, 17, tzinfo=datetime.UTC)
    assert prediction.score > 50
    assert prediction.recommendation in {"WATCH", "BUY"}
    assert prediction.status == "PENDING"
    assert prediction.input_snapshot["quote"]["date"] == date.isoformat()
    assert prediction.explanation["components"]
    assert (
        round(
            sum(item["contribution"] for item in prediction.explanation["components"])
            + sum(item["contribution"] for item in prediction.explanation["penalties"]),
            2,
        )
        == prediction.score
    )


def test_score_all_stocks_generates_all_horizons_and_ranks(scoring_service):
    seed_stock()
    date = seed_quotes()
    seed_factors_and_signal(date)

    result = scoring_service.score_all_stocks(date=date)

    assert result["horizons"] == [5, 20, 60]
    assert result["scored_count"] == 3
    assert {item.horizon for item in FakePrediction.records} == {5, 20, 60}
    assert all(item.rank == 1 for item in FakePrediction.records)


def test_missing_quote_creates_blocked_prediction(scoring_service):
    stock = seed_stock()
    date = datetime.datetime(2026, 4, 10, tzinfo=datetime.UTC)

    prediction = scoring_service.score_single_stock(stock, date, 5)

    assert prediction.status == "BLOCKED"
    assert prediction.input_snapshot["blocked_reason"] == "missing_quote"
    assert prediction.score == 0.0


def test_scoring_does_not_read_future_quotes(scoring_service):
    stock = seed_stock()
    date = seed_quotes()
    seed_factors_and_signal(date)
    FakeQuote.records.append(
        FakeQuote(
            code=stock.code,
            date=date + datetime.timedelta(days=1),
            close=100.0,
            high=100.0,
            low=100.0,
            trade_status=1,
            isST=0,
        )
    )

    prediction = scoring_service.score_single_stock(stock, date, 5)

    evidence = prediction.explanation["components"][2]["evidence"]
    assert evidence["close"] == 10.0
    assert evidence["old_close"] < 10.0


def test_verification_transitions_to_verified(scoring_service):
    stock = seed_stock()
    date = seed_quotes()
    seed_factors_and_signal(date)
    prediction = scoring_service.score_single_stock(stock, date, 5)
    prediction.date = prediction.date.replace(tzinfo=None)
    prediction.target_date = prediction.target_date.replace(tzinfo=None)
    for quote in FakeQuote.records:
        quote.date = quote.date.replace(tzinfo=None)
    future_dates = [
        datetime.datetime(2026, 4, 13),
        datetime.datetime(2026, 4, 14),
        datetime.datetime(2026, 4, 15),
        datetime.datetime(2026, 4, 16),
        datetime.datetime(2026, 4, 17),
    ]
    for idx, quote_date in enumerate(future_dates):
        FakeQuote.records.append(
            FakeQuote(
                code=stock.code,
                date=quote_date,
                close=10.0 + idx * 0.2,
                high=10.1 + idx * 0.2,
                low=9.9,
            )
        )

    service = ScoreVerificationService(
        quote_model=FakeQuote, prediction_model=FakePrediction
    )
    status = service.verify_single_prediction(
        prediction, today=datetime.datetime(2026, 4, 18, tzinfo=datetime.UTC)
    )

    assert status == "VERIFIED"
    assert prediction.status == "VERIFIED"
    assert prediction.verification["verified_quote_count"] == 5
    assert prediction.verification["max_return"] > 0
    assert prediction.verification["hit_target_close"] is True
    assert prediction.verification["hit_target_intra"] is True


def test_replay_backfills_trading_dates(scoring_service):
    seed_stock()
    date = seed_quotes()
    seed_factors_and_signal(date)
    replay = ScoreReplayService(scoring_service=scoring_service)

    result = replay.backfill_predictions(
        start_date=date,
        end_date=date,
        horizon=5,
        dry_run=False,
        replace=True,
    )

    assert result["date_count"] == 1
    assert result["scored_count"] == 1


def test_calibration_report_summarizes_verified_predictions():
    FakePrediction.records = [
        FakePrediction(
            stock_code="sh600000",
            stock_name="浦发银行",
            date=datetime.datetime(2026, 4, 10, tzinfo=datetime.UTC),
            horizon=5,
            model_version="score_v2_202604",
            status="VERIFIED",
            score=82.0,
            rank=1,
            recommendation="BUY",
            verification={
                "return_at_target": 0.03,
                "max_return": 0.08,
                "min_return": -0.01,
                "max_drawdown": -0.01,
                "hit_target_close": True,
                "hit_target_intra": True,
                "hit_stop_loss": False,
            },
            explanation={
                "components": [
                    {"id": "signal_strength", "contribution": 30.0},
                ]
            },
        ),
        FakePrediction(
            stock_code="sh600001",
            stock_name="测试股票",
            date=datetime.datetime(2026, 4, 10, tzinfo=datetime.UTC),
            horizon=5,
            model_version="score_v2_202604",
            status="VERIFIED",
            score=35.0,
            rank=2,
            recommendation="AVOID",
            verification={
                "return_at_target": -0.01,
                "max_return": 0.09,
                "min_return": -0.03,
                "max_drawdown": -0.03,
                "hit_target_close": False,
                "hit_target_intra": True,
                "hit_stop_loss": False,
            },
            explanation={"components": []},
        ),
    ]

    report = ScoreCalibrationReport(
        prediction_model=FakePrediction,
        model_version="score_v2_202604",
    ).generate(
        start_date=datetime.datetime(2026, 4, 1, tzinfo=datetime.UTC),
        end_date=datetime.datetime(2026, 4, 30, tzinfo=datetime.UTC),
        horizon=5,
    )

    assert report["prediction_count"] == 2
    assert report["top_n"]["top_10"]["count"] == 2
    assert report["component_summary"]["signal_strength"]["count"] == 1
    assert report["false_negatives"][0]["stock_code"] == "sh600001"


# ---------------------------------------------------------------------------
# Hybrid _recommendation() tests (Task 12d.6)
# ---------------------------------------------------------------------------


class TestHybridRecommendation:
    """Unit tests for the hybrid percentile+absolute recommendation logic."""

    @staticmethod
    def _cfg(**overrides):
        """Build a minimal config dict for _recommendation()."""
        return {
            "buy_threshold": 70.0,
            "watch_threshold": 50.0,
            "avoid_threshold": 20.0,
            "buy_percentile": 0.95,
            "watch_percentile": 0.80,
            "avoid_percentile": 0.20,
            **overrides,
        }

    # --- Percentile-driven path (percentile provided) ---
    # Recommendation is driven by cohort percentile alone; absolute score
    # thresholds are NOT required (they drift with weight configuration).

    def test_hybrid_buy(self, scoring_service):
        """pct>=0.95 → BUY regardless of absolute score"""
        cfg = self._cfg()
        assert scoring_service._recommendation(80.0, cfg, 0.97) == "BUY"
        assert scoring_service._recommendation(70.0, cfg, 0.95) == "BUY"
        # low absolute score but top percentile still BUY (rank-driven)
        assert scoring_service._recommendation(30.0, cfg, 0.98) == "BUY"

    def test_hybrid_strong_score_weak_pct(self, scoring_service):
        """pct<0.95 but >=0.80 → WATCH; pct mid-band → NONE (rank-driven)"""
        cfg = self._cfg()
        assert scoring_service._recommendation(75.0, cfg, 0.80) == "WATCH"
        assert scoring_service._recommendation(85.0, cfg, 0.50) == "NONE"

    def test_hybrid_weak_score_strong_pct(self, scoring_service):
        """pct>=0.95 → BUY, pct in 0.80-0.95 → WATCH regardless of score"""
        cfg = self._cfg()
        assert scoring_service._recommendation(55.0, cfg, 0.90) == "WATCH"
        assert scoring_service._recommendation(10.0, cfg, 0.96) == "BUY"

    def test_hybrid_avoid_by_percentile(self, scoring_service):
        """pct<=0.20 → AVOID (bottom 20%) regardless of absolute score"""
        cfg = self._cfg()
        assert scoring_service._recommendation(25.0, cfg, 0.10) == "AVOID"
        assert scoring_service._recommendation(30.0, cfg, 0.05) == "AVOID"
        assert scoring_service._recommendation(80.0, cfg, 0.10) == "AVOID"

    def test_hybrid_avoid_by_absolute(self, scoring_service):
        """score<=20 with no useful percentile → AVOID (fallback absolute)"""
        cfg = self._cfg()
        # percentile=0.0 falls back to absolute thresholds
        assert scoring_service._recommendation(20.0, cfg, 0.0) == "AVOID"
        assert scoring_service._recommendation(10.0, cfg, None) == "AVOID"

    def test_hybrid_none(self, scoring_service):
        """pct between avoid and watch → NONE"""
        cfg = self._cfg()
        assert scoring_service._recommendation(25.0, cfg, 0.50) == "NONE"
        assert scoring_service._recommendation(40.0, cfg, 0.60) == "NONE"
        assert scoring_service._recommendation(95.0, cfg, 0.50) == "NONE"

    def test_hybrid_boundaries(self, scoring_service):
        """Exact boundary percentiles: 0.95/0.80/0.20"""
        cfg = self._cfg()
        # At BUY boundary
        assert scoring_service._recommendation(70.0, cfg, 0.95) == "BUY"
        assert scoring_service._recommendation(70.0, cfg, 0.94) == "WATCH"
        # At WATCH boundary
        assert scoring_service._recommendation(50.0, cfg, 0.80) == "WATCH"
        assert scoring_service._recommendation(50.0, cfg, 0.79) == "NONE"
        # At AVOID boundary
        assert scoring_service._recommendation(20.0, cfg, 0.20) == "AVOID"
        assert scoring_service._recommendation(20.0, cfg, 0.21) == "NONE"

    # --- Fallback path (percentile=None or 0.0) ---

    def test_fallback_high_score_returns_watch(self, scoring_service):
        """score>=70 with no percentile → WATCH (safe placeholder)"""
        cfg = self._cfg()
        assert scoring_service._recommendation(80.0, cfg, None) == "WATCH"
        assert scoring_service._recommendation(70.0, cfg, 0.0) == "WATCH"

    def test_fallback_mid_score(self, scoring_service):
        """50<=score<70 with no percentile → WATCH"""
        cfg = self._cfg()
        assert scoring_service._recommendation(60.0, cfg, None) == "WATCH"

    def test_fallback_avoid(self, scoring_service):
        """score<=20 with no percentile → AVOID"""
        cfg = self._cfg()
        assert scoring_service._recommendation(15.0, cfg, None) == "AVOID"
        assert scoring_service._recommendation(0.0, cfg, 0.0) == "AVOID"

    def test_fallback_none(self, scoring_service):
        """20<score<50 with no percentile → NONE"""
        cfg = self._cfg()
        assert scoring_service._recommendation(30.0, cfg, None) == "NONE"

    # --- Old config compatibility (no percentile keys) ---

    def test_old_config_fallback(self, scoring_service):
        """Config without percentile keys uses defaults (0.95/0.80/0.20)"""
        cfg = {"buy_threshold": 70.0, "watch_threshold": 50.0, "avoid_threshold": 20.0}
        assert scoring_service._recommendation(80.0, cfg, 0.97) == "BUY"
        assert scoring_service._recommendation(25.0, cfg, 0.10) == "AVOID"


# ---------------------------------------------------------------------------
# Rank-normalized component scoring tests (scoring-percentile-rank change)
# ---------------------------------------------------------------------------


class TestRankNormalize:
    """Unit tests for cross-sectional component rank normalization."""

    def test_rank_normalize_basic(self):
        from app.lib.scoring_engine.scoring_service import StockScoringService

        values = {"a": 10.0, "b": 20.0, "c": 30.0}
        result = StockScoringService._rank_normalize(values)
        assert result["a"] == 0.0
        assert result["b"] == 0.5
        assert result["c"] == 1.0

    def test_rank_normalize_ties(self):
        from app.lib.scoring_engine.scoring_service import StockScoringService

        values = {"a": 5.0, "b": 5.0, "c": 10.0}
        result = StockScoringService._rank_normalize(values)
        # a and b share ranks 1,2 -> avg 1.5 -> (1.5-1)/2 = 0.25
        assert result["a"] == result["b"] == 0.25
        assert result["c"] == 1.0

    def test_rank_normalize_none_ranks_lowest(self):
        from app.lib.scoring_engine.scoring_service import StockScoringService

        values = {"a": None, "b": 10.0, "c": 20.0}
        result = StockScoringService._rank_normalize(values)
        assert result["a"] == 0.0
        assert result["b"] == 0.5
        assert result["c"] == 1.0

    def test_rank_normalize_single(self):
        from app.lib.scoring_engine.scoring_service import StockScoringService

        result = StockScoringService._rank_normalize({"a": 42.0})
        assert result["a"] == 1.0

    def test_rank_normalize_all_none(self):
        from app.lib.scoring_engine.scoring_service import StockScoringService

        result = StockScoringService._rank_normalize({"a": None, "b": None})
        assert result["a"] == 0.0
        assert result["b"] == 0.0

    def test_score_all_stocks_ranked_dispatch(self, monkeypatch, scoring_service):
        """DATAHUB_SCORING_MODE=ranked dispatches to ranked path."""

        monkeypatch.setenv("DATAHUB_SCORING_MODE", "ranked")
        called = {}

        def fake_ranked(self_obj, **kwargs):
            called["hit"] = True
            return {"date": kwargs.get("date"), "scored_count": 1}

        monkeypatch.setattr(
            type(scoring_service), "score_all_stocks_ranked", fake_ranked
        )
        scoring_service.score_all_stocks()
        assert called.get("hit") is True
        monkeypatch.delenv("DATAHUB_SCORING_MODE", raising=False)

    def test_score_all_stocks_default_raw(self, monkeypatch, scoring_service):
        """Without env, score_all_stocks does NOT dispatch to ranked."""
        import datetime

        monkeypatch.delenv("DATAHUB_SCORING_MODE", raising=False)
        called = {}

        def fake_ranked(self_obj, **kwargs):
            called["hit"] = True
            return {"scored_count": 1}

        monkeypatch.setattr(
            type(scoring_service), "score_all_stocks_ranked", fake_ranked
        )

        # empty stock model -> raw path scores nothing, ranked not called
        class EmptyStockModel:
            records = []

            @staticmethod
            def objects(**kwargs):
                return []

        monkeypatch.setattr(scoring_service, "stock_model", EmptyStockModel)
        result = scoring_service.score_all_stocks(date=datetime.datetime(2026, 6, 1))
        assert called.get("hit") is None
        assert result["scored_count"] == 0


class TestScoreAllStocksRankedEndToEnd:
    """End-to-end tests for score_all_stocks_ranked using fake models."""

    def _seed_cohort(self, scoring_service):
        """Seed 3 stocks with quotes; 1 stock without (blocked)."""
        from app.test.test_scoring_service import (
            FakeStock,
            seed_quotes,
        )

        codes = ["sh600000", "sh600001", "sh600002"]
        for code in codes:
            s = FakeStock(code=code, name=f"Stock {code}", active_status=0)
            FakeStock.records.append(s)
            seed_quotes(stock_code=code)
        # blocked stock: no quotes
        blocked = FakeStock(code="sh600099", name="Blocked", active_status=0)
        FakeStock.records.append(blocked)
        return codes + ["sh600099"]

    def test_ranked_scores_all_and_persists_blocked(self, scoring_service, monkeypatch):
        import datetime

        self._seed_cohort(scoring_service)
        monkeypatch.delenv("DATAHUB_SCORING_MODE", raising=False)

        result = scoring_service.score_all_stocks_ranked(
            date=datetime.datetime(2026, 4, 10, tzinfo=datetime.UTC),
            horizon=5,
        )

        # all 4 stocks get a prediction row (3 scored + 1 blocked)
        assert result["scored_count"] >= 3
        preds = list(scoring_service.prediction_model.records)
        assert len(preds) == 4
        blocked = [p for p in preds if p.stock_code == "sh600099"]
        assert len(blocked) == 1
        assert blocked[0].status == "BLOCKED"

    def test_ranked_explanation_has_components(self, scoring_service, monkeypatch):
        import datetime

        self._seed_cohort(scoring_service)
        monkeypatch.delenv("DATAHUB_SCORING_MODE", raising=False)

        scoring_service.score_all_stocks_ranked(
            date=datetime.datetime(2026, 4, 10, tzinfo=datetime.UTC),
            horizon=5,
        )
        scored = [
            p
            for p in scoring_service.prediction_model.records
            if p.stock_code != "sh600099"
        ]
        assert scored, "expected at least one scored prediction"
        comps = scored[0].explanation.get("components", [])
        assert comps, "explanation must carry real components"
        assert all("id" in c and "raw_value" in c for c in comps)

    def test_ranked_respects_replace_false(self, scoring_service, monkeypatch):
        import datetime

        self._seed_cohort(scoring_service)
        monkeypatch.delenv("DATAHUB_SCORING_MODE", raising=False)
        d = datetime.datetime(2026, 4, 10, tzinfo=datetime.UTC)

        scoring_service.score_all_stocks_ranked(date=d, horizon=5, replace=True)
        first = len(scoring_service.prediction_model.records)

        # second run with replace=False must NOT overwrite (same count)
        scoring_service.score_all_stocks_ranked(date=d, horizon=5, replace=False)
        assert len(scoring_service.prediction_model.records) == first


class TestRankedPenaltyDirection:
    """Ranked-mode penalty must SUBTRACT: risky stocks score lower."""

    def test_high_volatility_does_not_outrank_low_volatility(self, monkeypatch):
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

        calendar = [
            datetime.datetime(2026, 4, 10, tzinfo=datetime.UTC),
            datetime.datetime(2026, 4, 13, tzinfo=datetime.UTC),
            datetime.datetime(2026, 4, 14, tzinfo=datetime.UTC),
            datetime.datetime(2026, 4, 15, tzinfo=datetime.UTC),
            datetime.datetime(2026, 4, 16, tzinfo=datetime.UTC),
        ]

        # two stocks, identical quotes except volatility proxy in risk_penalty
        for code, close in (("sh600000", 10.0), ("sh600001", 10.0)):
            FakeStock.records.append(
                FakeStock(code=code, name=f"Stock {code}", active_status=0)
            )
            for idx in range(60):
                FakeQuote.records.append(
                    FakeQuote(
                        code=code,
                        date=datetime.datetime(2026, 3, 1, tzinfo=datetime.UTC)
                        + datetime.timedelta(days=idx),
                        close=close + idx * 0.01,
                        high=close + idx * 0.01 + 0.1,
                        low=close + idx * 0.01 - 0.1,
                        trade_status=1,
                        isST=0,
                    )
                )
        # stock A is ST on the SCORING-DATE quote (2026-04-10, idx 40 from
        # 2026-03-01); risk_penalty reads isST from the scoring-date quote,
        # so this is what makes A riskier than B
        for rec in FakeQuote.records:
            if rec.code == "sh600000":
                setattr(rec, "isST", 1)

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
        ):
            mock_market = MagicMock()
            mock_market.trade_calendar = calendar
            mock_market_objs.return_value.first.return_value = mock_market
            service = StockScoringService(
                stock_model=FakeStock,
                quote_model=FakeQuote,
                factor_model=FakeFactor,
                signal_model=FakeSignal,
                prediction_model=FakePrediction,
            )
            service.calendar = calendar
            monkeypatch.delenv("DATAHUB_SCORING_MODE", raising=False)
            service.score_all_stocks_ranked(
                date=datetime.datetime(2026, 4, 10, tzinfo=datetime.UTC),
                horizon=5,
            )
        a = [p for p in FakePrediction.records if p.stock_code == "sh600000"][0]
        b = [p for p in FakePrediction.records if p.stock_code == "sh600001"][0]
        # ST stock (risky) must score STRICTLY lower than the clean peer
        assert a.score < b.score, f"ST stock {a.score} outranked clean stock {b.score}"
