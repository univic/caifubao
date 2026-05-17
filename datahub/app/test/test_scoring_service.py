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

    with patch(
        "app.lib.scoring_engine.scoring_service.FinanceMarket.objects"
    ) as mock_market_objs:
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
        return service


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
    future_dates = [
        datetime.datetime(2026, 4, 13, tzinfo=datetime.UTC),
        datetime.datetime(2026, 4, 14, tzinfo=datetime.UTC),
        datetime.datetime(2026, 4, 15, tzinfo=datetime.UTC),
        datetime.datetime(2026, 4, 16, tzinfo=datetime.UTC),
        datetime.datetime(2026, 4, 17, tzinfo=datetime.UTC),
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

    report = ScoreCalibrationReport(prediction_model=FakePrediction).generate(
        start_date=datetime.datetime(2026, 4, 1, tzinfo=datetime.UTC),
        end_date=datetime.datetime(2026, 4, 30, tzinfo=datetime.UTC),
        horizon=5,
    )

    assert report["prediction_count"] == 2
    assert report["top_n"]["top_10"]["count"] == 2
    assert report["component_summary"]["signal_strength"]["count"] == 1
    assert report["false_negatives"][0]["stock_code"] == "sh600001"
