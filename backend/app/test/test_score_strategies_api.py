# -*- coding: utf-8 -*-
import datetime
import json
from types import SimpleNamespace


class FakeQuerySet:
    def __init__(self, rows):
        self._rows = list(rows)

    def filter(self, **kwargs):
        return FakeQuerySet([row for row in self._rows if _matches(row, kwargs)])

    def order_by(self, *fields):
        rows = self._rows
        for field in reversed(fields):
            reverse = field.startswith("-")
            name = field.removeprefix("-")
            rows = sorted(rows, key=lambda row: getattr(row, name, None) or 0,
                          reverse=reverse)
        return FakeQuerySet(rows)

    def first(self):
        return self._rows[0] if self._rows else None

    def only(self, *_fields):
        return self

    def __iter__(self):
        return iter(self._rows)

    def __len__(self):
        return len(self._rows)

    def __bool__(self):
        return len(self._rows) > 0


def _matches(row, kwargs):
    for key, value in kwargs.items():
        if key.endswith("__gte"):
            field = key.removesuffix("__gte")
            if getattr(row, field, None) is None or getattr(row, field) < value:
                return False
        elif key.endswith("__lte"):
            field = key.removesuffix("__lte")
            if getattr(row, field, None) is None or getattr(row, field) > value:
                return False
        elif key.endswith("__in"):
            field = key.removesuffix("__in")
            if getattr(row, field, None) not in value:
                return False
        else:
            if getattr(row, key, None) != value:
                return False
    return True


def _mk_pred(stock_code="sh600519", stock_name="test", date=None, horizon=5,
             score=75.0, status="VERIFIED",
             verification=None, model_version="score_v2_202605"):
    if date is None:
        date = datetime.datetime(2025, 6, 1)
    if verification is None:
        verification = {"return_at_target": 0.035, "max_return": 0.08,
                        "min_return": -0.02, "max_drawdown": -0.02,
                        "hit_target": True, "hit_stop_loss": False}
    return SimpleNamespace(
        stock_code=stock_code, stock_name=stock_name,
        date=date, horizon=horizon, score=score, rank=5,
        percentile=0.25, recommendation="BUY", base_price=100.0,
        target_date=date + datetime.timedelta(days=horizon),
        status=status, verification=verification,
        model_version=model_version,
    )


# --- Backtest tests ---

def test_backtest_missing_dates_400(client):
    resp = client.post("/api/score-strategies/backtest",
                       data=json.dumps({"horizon": 5, "top_n": 10}),
                       content_type="application/json")
    assert resp.status_code == 400


def test_backtest_date_validation_400(client):
    resp = client.post("/api/score-strategies/backtest",
                       data=json.dumps({"horizon": 5, "top_n": 10,
                                        "start_date": "2025-12-31",
                                        "end_date": "2025-01-01"}),
                       content_type="application/json")
    assert resp.status_code == 400


def test_backtest_no_data_404(client, monkeypatch):
    import app.api.v1.score_strategies as score_strategies
    monkeypatch.setattr(
        score_strategies, "StockScorePrediction",
        SimpleNamespace(objects=lambda **kwargs: FakeQuerySet([])),
    )
    resp = client.post("/api/score-strategies/backtest",
                       data=json.dumps({"horizon": 5, "top_n": 10,
                                        "start_date": "2020-01-01",
                                        "end_date": "2020-01-10"}),
                       content_type="application/json")
    assert resp.status_code == 404


def test_backtest_computes_metrics(client, monkeypatch):
    p1 = _mk_pred("sh600519", "茅台", datetime.datetime(2025, 6, 1),
                  score=85.0,
                  verification={"return_at_target": 0.05, "max_return": 0.08,
                                "hit_target": True})
    p2 = _mk_pred("sz000001", "平安银行", datetime.datetime(2025, 6, 2),
                  score=90.0,
                  verification={"return_at_target": -0.02, "max_return": 0.03,
                                "hit_target": False})

    import app.api.v1.score_strategies as score_strategies
    monkeypatch.setattr(
        score_strategies, "StockScorePrediction",
        SimpleNamespace(objects=lambda **kwargs: FakeQuerySet([p1, p2])),
    )
    resp = client.post("/api/score-strategies/backtest",
                       data=json.dumps({"horizon": 5, "top_n": 1,
                                        "start_date": "2025-06-01",
                                        "end_date": "2025-06-05"}),
                       content_type="application/json")
    assert resp.status_code == 200
    body = resp.get_json()
    assert body["success"] is True
    assert body["strategy"]["horizon"] == 5
    assert body["summary"]["total_trading_days"] == 2
    assert body["summary"]["overall_hit_rate"] == 0.5
    assert len(body["equity_curve"]) == 3
    assert len(body["daily_results"]) == 2


def test_backtest_unsupported_horizon_400(client):
    resp = client.post("/api/score-strategies/backtest",
                       data=json.dumps({"horizon": 10, "top_n": 10,
                                        "start_date": "2025-01-01",
                                        "end_date": "2025-01-10"}),
                       content_type="application/json")
    assert resp.status_code == 400


# --- Calibration tests ---

def test_calibration_bucket_breakdown(client, monkeypatch):
    predictions = [
        _mk_pred("sh600519", "茅台", datetime.datetime(2025, 6, 1),
                 score=85.0, verification={"return_at_target": 0.05,
                                           "max_return": 0.08,
                                           "hit_target": True,
                                           "max_drawdown": -0.03,
                                           "hit_stop_loss": False}),
        _mk_pred("sz000001", "平安银行", datetime.datetime(2025, 6, 1),
                 score=90.0, verification={"return_at_target": -0.02,
                                           "max_return": 0.03,
                                           "hit_target": False,
                                           "max_drawdown": -0.06,
                                           "hit_stop_loss": True}),
        _mk_pred("sh600036", "招商银行", datetime.datetime(2025, 6, 1),
                 score=70.0, verification={"return_at_target": 0.04,
                                           "max_return": 0.07,
                                           "hit_target": True,
                                           "max_drawdown": -0.02,
                                           "hit_stop_loss": False}),
    ]
    import app.api.v1.score_strategies as score_strategies
    monkeypatch.setattr(
        score_strategies, "StockScorePrediction",
        SimpleNamespace(objects=lambda **kwargs: FakeQuerySet(predictions)),
    )
    resp = client.get("/api/score-strategies/calibration?horizon=5&days=365")
    assert resp.status_code == 200
    body = resp.get_json()
    assert body["success"] is True
    assert body["horizon"] == 5
    assert body["prediction_count"] == 3
    assert len(body["buckets"]) == 5
    bucket_80 = [b for b in body["buckets"] if b["bucket"] == "80-100"]
    assert bucket_80[0]["count"] == 2
    assert bucket_80[0]["hit_rate"] == 0.5


def test_calibration_unsupported_horizon_400(client):
    resp = client.get("/api/score-strategies/calibration?horizon=10")
    assert resp.status_code == 400


def test_calibration_empty_data(client, monkeypatch):
    import app.api.v1.score_strategies as score_strategies
    monkeypatch.setattr(
        score_strategies, "StockScorePrediction",
        SimpleNamespace(objects=lambda **kwargs: FakeQuerySet([])),
    )
    resp = client.get("/api/score-strategies/calibration?horizon=5&days=90")
    assert resp.status_code == 200
    body = resp.get_json()
    assert body["prediction_count"] == 0
    assert body["buckets"] == []


# --- Confidence tests ---

def test_confidence_returns_data(client, monkeypatch):
    predicted = _mk_pred("sh600519", "茅台", datetime.datetime(2025, 6, 1),
                         score=85.0, status="PENDING")
    verified = [
        _mk_pred("sh600519", "茅台", datetime.datetime(2025, 5, 1),
                 score=85.0,
                 verification={"return_at_target": 0.06, "max_return": 0.08,
                               "hit_target": True, "max_drawdown": -0.02}),
        _mk_pred("sz000001", "平安行", datetime.datetime(2025, 5, 2),
                 score=88.0,
                 verification={"return_at_target": -0.01, "max_return": 0.04,
                               "hit_target": False, "max_drawdown": -0.05}),
        _mk_pred("sh600036", "招商", datetime.datetime(2025, 5, 3),
                 score=90.0,
                 verification={"return_at_target": 0.04, "max_return": 0.07,
                               "hit_target": True, "max_drawdown": -0.03}),
        _mk_pred("sz000002", "万科", datetime.datetime(2025, 5, 4),
                 score=82.0,
                 verification={"return_at_target": 0.03, "max_return": 0.06,
                               "hit_target": True, "max_drawdown": -0.02}),
        _mk_pred("sh601318", "平安", datetime.datetime(2025, 5, 5),
                 score=87.0,
                 verification={"return_at_target": 0.02, "max_return": 0.05,
                               "hit_target": True, "max_drawdown": -0.01}),
    ]
    all_rows = [predicted] + verified

    class FakeDynamicQuery:
        def __init__(self, rows):
            self._rows = rows
        def filter(self, **kwargs):
            return FakeDynamicQuery([r for r in self._rows if _matches(r, kwargs)])
        def first(self):
            return self._rows[0] if self._rows else None
        def __iter__(self):
            return iter(self._rows)

    import app.api.v1.score_strategies as score_strategies
    monkeypatch.setattr(
        score_strategies, "StockScorePrediction",
        SimpleNamespace(objects=lambda **kw: FakeQuerySet(all_rows).filter(**kw)),
    )
    resp = client.get("/api/score-strategies/confidence"
                      "?stock_code=sh600519&date=2025-06-01&horizon=5")
    assert resp.status_code == 200
    body = resp.get_json()
    assert body["success"] is True
    assert body["score_bucket"] == "80-100"
    assert body["confidence"] == "high"
    assert body["bucket_hit_rate"] == 0.8
    assert body["trade_suggestions"] is not None
    assert body["trade_suggestions"]["stop_loss"] is not None


def test_confidence_missing_params_400(client):
    resp = client.get("/api/score-strategies/confidence?horizon=5")
    assert resp.status_code == 400


def test_confidence_unsupported_horizon_400(client):
    resp = client.get("/api/score-strategies/confidence"
                      "?stock_code=sh600519&date=2025-06-01&horizon=10")
    assert resp.status_code == 400


def test_confidence_not_found_404(client, monkeypatch):
    class FakeEmpty:
        def filter(self, **kwargs):
            return self
        def first(self):
            return None

    import app.api.v1.score_strategies as score_strategies
    monkeypatch.setattr(
        score_strategies, "StockScorePrediction",
        SimpleNamespace(objects=lambda **kw: FakeEmpty()),
    )
    resp = client.get("/api/score-strategies/confidence"
                      "?stock_code=sh000001&date=2020-01-01&horizon=5")
    assert resp.status_code == 404
