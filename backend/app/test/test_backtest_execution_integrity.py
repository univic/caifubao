"""Regression tests for score-driven backtest execution integrity."""

import datetime
from types import SimpleNamespace

import pytest


def _day(value: str) -> datetime.datetime:
    return datetime.datetime.strptime(value, "%Y-%m-%d")


def _quote(close: float, open_hfq: float, trade_status: int = 1):
    return SimpleNamespace(
        close=close,
        close_hfq=close,
        open_hfq=open_hfq,
        trade_status=trade_status,
        change_rate=0.0,
    )


class _Query:
    def __init__(self, rows):
        self.rows = list(rows)

    def __call__(self, **filters):
        return self.filter(**filters)

    def filter(self, **filters):
        rows = self.rows
        for key, value in filters.items():
            if key.endswith("__gte"):
                field = key.removesuffix("__gte")
                rows = [row for row in rows if getattr(row, field) >= value]
            elif key.endswith("__lte"):
                field = key.removesuffix("__lte")
                rows = [row for row in rows if getattr(row, field) <= value]
            elif key.endswith("__nin"):
                field = key.removesuffix("__nin")
                rows = [row for row in rows if getattr(row, field) not in value]
            else:
                rows = [row for row in rows if getattr(row, key) == value]
        return type(self)(rows)

    def order_by(self, field):
        reverse = field.startswith("-")
        name = field.removeprefix("-")
        return type(self)(
            sorted(self.rows, key=lambda row: getattr(row, name), reverse=reverse)
        )

    def __iter__(self):
        return iter(self.rows)


class TestScoreExecutionTiming:
    def test_threshold_uses_next_trading_day_adjusted_open(self):
        from app.services.backtest_service import _simulate

        days = [_day("2026-08-26"), _day("2026-08-27"), _day("2026-08-28")]
        result = _simulate(
            strategy="SCORE_THRESHOLD",
            trading_days=days,
            quote_map={
                days[0]: _quote(100.0, 90.0),
                days[1]: _quote(130.0, 120.0),
                days[2]: _quote(140.0, 135.0),
            },
            factor_map={},
            initial_cash=100000.0,
            score_map={days[0]: SimpleNamespace(score=80.0)},
            horizon=5,
        )

        buy = next(t for t in result["trades"] if t["side"] == "BUY")
        assert buy["date"] == days[1].isoformat()
        assert buy["price"] == 120.0
        assert buy["exec_price"] == pytest.approx(120.12)

    def test_momentum_uses_next_trading_day_adjusted_open(self):
        from app.services.backtest_service import _simulate

        days = [
            _day("2026-08-25"),
            _day("2026-08-26"),
            _day("2026-08-27"),
            _day("2026-08-28"),
        ]
        result = _simulate(
            strategy="SCORE_MOMENTUM",
            trading_days=days,
            quote_map={
                days[0]: _quote(100.0, 90.0),
                days[1]: _quote(105.0, 95.0),
                days[2]: _quote(130.0, 120.0),
                days[3]: _quote(140.0, 135.0),
            },
            factor_map={},
            initial_cash=100000.0,
            score_map={
                days[0]: SimpleNamespace(score=50.0),
                days[1]: SimpleNamespace(score=70.0),
            },
            horizon=5,
            score_delta=10.0,
        )

        buy = next(t for t in result["trades"] if t["side"] == "BUY")
        assert buy["date"] == days[2].isoformat()
        assert buy["price"] == 120.0

    def test_consensus_uses_next_trading_day_adjusted_open(self):
        from app.services.backtest_service import _simulate

        days = [_day("2026-08-26"), _day("2026-08-27"), _day("2026-08-28")]
        scores = {
            5: {days[0]: SimpleNamespace(score=70.0)},
            20: {days[0]: SimpleNamespace(score=65.0)},
            60: {days[0]: SimpleNamespace(score=60.0)},
        }
        result = _simulate(
            strategy="MULTI_HORIZON_CONSENSUS",
            trading_days=days,
            quote_map={
                days[0]: _quote(100.0, 90.0),
                days[1]: _quote(130.0, 120.0),
                days[2]: _quote(140.0, 135.0),
            },
            factor_map={},
            initial_cash=100000.0,
            score_maps=scores,
        )

        buy = next(t for t in result["trades"] if t["side"] == "BUY")
        assert buy["date"] == days[1].isoformat()
        assert buy["price"] == 120.0

    def test_final_day_score_remains_unexecuted(self):
        from app.services.backtest_service import _simulate

        days = [_day("2026-08-27"), _day("2026-08-28")]
        result = _simulate(
            strategy="SCORE_THRESHOLD",
            trading_days=days,
            quote_map={
                days[0]: _quote(100.0, 90.0),
                days[1]: _quote(110.0, 105.0),
            },
            factor_map={},
            initial_cash=100000.0,
            score_map={days[1]: SimpleNamespace(score=90.0)},
            horizon=5,
        )

        assert result["trades"] == []

    def test_missing_adjusted_open_does_not_fall_back_to_close(self):
        from app.services.backtest_service import _simulate

        days = [_day("2026-08-26"), _day("2026-08-27")]
        result = _simulate(
            strategy="SCORE_THRESHOLD",
            trading_days=days,
            quote_map={
                days[0]: _quote(100.0, 90.0),
                days[1]: _quote(130.0, None),
            },
            factor_map={},
            initial_cash=100000.0,
            score_map={days[0]: SimpleNamespace(score=80.0)},
            horizon=5,
        )

        assert result["trades"] == []

    def test_blocked_threshold_order_retries_on_later_open(self):
        from app.services.backtest_service import _simulate

        days = [
            _day("2026-08-26"),
            _day("2026-08-27"),
            _day("2026-08-28"),
            _day("2026-08-31"),
        ]
        result = _simulate(
            strategy="SCORE_THRESHOLD",
            trading_days=days,
            quote_map={
                days[0]: _quote(100.0, 90.0),
                days[1]: _quote(110.0, 105.0, trade_status=0),
                days[2]: _quote(130.0, 120.0),
                days[3]: _quote(140.0, 135.0),
            },
            factor_map={},
            initial_cash=100000.0,
            score_map={days[0]: SimpleNamespace(score=80.0)},
            horizon=5,
        )

        buy = next(t for t in result["trades"] if t["side"] == "BUY")
        assert buy["date"] == days[2].isoformat()
        assert buy["price"] == 120.0

    @pytest.mark.parametrize(
        ("strategy", "score_map", "score_maps", "expected_sell_index"),
        [
            (
                "SCORE_THRESHOLD",
                {0: 80.0, 1: 40.0},
                None,
                2,
            ),
            (
                "SCORE_MOMENTUM",
                {0: 50.0, 1: 70.0, 2: 40.0},
                None,
                3,
            ),
            (
                "MULTI_HORIZON_CONSENSUS",
                None,
                {
                    5: {0: 70.0, 1: 20.0},
                    20: {0: 65.0, 1: 65.0},
                    60: {0: 60.0, 1: 60.0},
                },
                2,
            ),
        ],
    )
    def test_score_driven_exit_executes_next_open(
        self, strategy, score_map, score_maps, expected_sell_index
    ):
        from app.services.backtest_service import _simulate

        days = [_day(f"2026-08-{25 + i:02d}") for i in range(5)]
        mapped_scores = (
            {days[i]: SimpleNamespace(score=value) for i, value in score_map.items()}
            if score_map
            else None
        )
        mapped_horizons = (
            {
                horizon: {
                    days[i]: SimpleNamespace(score=value) for i, value in values.items()
                }
                for horizon, values in score_maps.items()
            }
            if score_maps
            else None
        )
        result = _simulate(
            strategy=strategy,
            trading_days=days,
            quote_map={day: _quote(100.0 + i, 90.0 + i) for i, day in enumerate(days)},
            factor_map={},
            initial_cash=100000.0,
            score_map=mapped_scores,
            score_maps=mapped_horizons,
            horizon=5,
            score_delta=10.0,
        )

        sell = next(
            t
            for t in result["trades"]
            if t["side"] == "SELL" and "Liquidation" not in t["reason"]
        )
        assert sell["date"] == days[expected_sell_index].isoformat()
        assert sell["price"] == 90.0 + expected_sell_index

    def test_top_n_ranking_executes_on_next_open(self):
        from app.services.backtest_service import _simulate_multi

        days = [_day("2026-08-26"), _day("2026-08-27"), _day("2026-08-28")]
        quote_maps = {
            "sh600000": {
                days[0]: _quote(10.0, 9.0),
                days[1]: _quote(13.0, 12.0),
                days[2]: _quote(14.0, 13.5),
            },
            "sz000001": {
                days[0]: _quote(20.0, 19.0),
                days[1]: _quote(22.0, 21.0),
                days[2]: _quote(23.0, 22.0),
            },
        }
        result = _simulate_multi(
            strategy="TOP_N_ROTATION",
            trading_days=days,
            quote_maps=quote_maps,
            factor_maps={},
            score_maps={
                "sh600000": {days[0]: SimpleNamespace(score=90.0)},
                "sz000001": {days[0]: SimpleNamespace(score=80.0)},
            },
            stock_names={},
            initial_cash=100000.0,
            top_n=1,
            rebalance_interval=5,
        )

        buy = next(t for t in result["trades"] if t["side"] == "BUY")
        assert buy["date"] == days[1].isoformat()
        assert buy["price"] == 12.0

    def test_top_n_rebalance_sell_and_buy_use_next_open(self):
        from app.services.backtest_service import _simulate_multi

        days = [_day(f"2026-08-{26 + i:02d}") for i in range(4)]
        quote_maps = {
            "sh600000": {day: _quote(10.0 + i, 9.0 + i) for i, day in enumerate(days)},
            "sz000001": {day: _quote(20.0 + i, 19.0 + i) for i, day in enumerate(days)},
        }
        result = _simulate_multi(
            strategy="TOP_N_ROTATION",
            trading_days=days,
            quote_maps=quote_maps,
            factor_maps={},
            score_maps={
                "sh600000": {
                    days[0]: SimpleNamespace(score=90.0),
                    days[1]: SimpleNamespace(score=70.0),
                },
                "sz000001": {
                    days[0]: SimpleNamespace(score=80.0),
                    days[1]: SimpleNamespace(score=95.0),
                },
            },
            stock_names={},
            initial_cash=100000.0,
            top_n=1,
            rebalance_interval=1,
        )

        rebalance_sell = next(
            t
            for t in result["trades"]
            if t["side"] == "SELL" and "rebalance" in t["reason"]
        )
        second_buy = [t for t in result["trades"] if t["side"] == "BUY"][1]
        assert rebalance_sell["date"] == days[2].isoformat()
        assert rebalance_sell["price"] == 11.0
        assert second_buy["date"] == days[2].isoformat()
        assert second_buy["price"] == 21.0

    def test_top_n_blocked_order_retries_on_later_open(self):
        from app.services.backtest_service import _simulate_multi

        days = [_day(f"2026-08-{26 + i:02d}") for i in range(4)]
        quote_maps = {
            "sh600000": {
                days[0]: _quote(10.0, 9.0),
                days[1]: _quote(11.0, 10.0, trade_status=0),
                days[2]: _quote(12.0, 11.0),
                days[3]: _quote(13.0, 12.0),
            }
        }
        result = _simulate_multi(
            strategy="TOP_N_ROTATION",
            trading_days=days,
            quote_maps=quote_maps,
            factor_maps={},
            score_maps={
                "sh600000": {days[0]: SimpleNamespace(score=90.0)},
            },
            stock_names={},
            initial_cash=100000.0,
            top_n=1,
            rebalance_interval=5,
        )

        buy = next(t for t in result["trades"] if t["side"] == "BUY")
        assert buy["date"] == days[2].isoformat()
        assert buy["price"] == 11.0


class TestScoreLoadingIntegrity:
    def test_service_filters_version_and_unusable_statuses_and_records_timing(
        self, monkeypatch
    ):
        from app.services import backtest_service

        days = [_day("2026-08-26"), _day("2026-08-27"), _day("2026-08-28")]
        quotes = [
            SimpleNamespace(
                code="sh600000",
                date=day,
                close=100.0 + i,
                close_hfq=100.0 + i,
                open_hfq=90.0 + i,
                trade_status=1,
                change_rate=0.0,
            )
            for i, day in enumerate(days)
        ]
        predictions = [
            SimpleNamespace(
                stock_code="sh600000",
                date=days[0],
                horizon=5,
                model_version="selected-v1",
                status="PENDING",
                score=80.0,
            ),
            SimpleNamespace(
                stock_code="sh600000",
                date=days[0],
                horizon=5,
                model_version="selected-v1",
                status="BLOCKED",
                score=99.0,
            ),
            SimpleNamespace(
                stock_code="sh600000",
                date=days[0],
                horizon=5,
                model_version="other-v2",
                status="PENDING",
                score=100.0,
            ),
        ]
        captured = {}

        monkeypatch.setattr(
            backtest_service,
            "StockDailyQuote",
            SimpleNamespace(objects=_Query(quotes)),
        )

        def _score_objects(**filters):
            captured.update(filters)
            return _Query(predictions).filter(**filters)

        monkeypatch.setattr(
            backtest_service,
            "StockScorePrediction",
            SimpleNamespace(objects=_score_objects),
        )
        monkeypatch.setattr(backtest_service, "_resolve_stock_name", lambda code: code)
        monkeypatch.setattr(backtest_service, "_compute_benchmark", lambda *a: {})

        result = backtest_service.run_backtest(
            stock_code="sh600000",
            strategy="SCORE_THRESHOLD",
            start_date=days[0],
            end_date=days[-1],
            save_result=False,
            horizon=5,
            model_version="selected-v1",
        )

        assert "error" not in result
        assert captured["model_version"] == "selected-v1"
        assert captured["status__nin"] == ("BLOCKED", "FAILED")
        assert result["score_config"] == {
            "horizon": 5,
            "entry_threshold": 70.0,
            "exit_threshold": 50.0,
            "stop_loss_pct": -5.0,
            "score_delta": 10.0,
            "model_version": "selected-v1",
            "execution_timing": "next_trading_day_open",
        }

        monkeypatch.setattr(
            backtest_service,
            "StockScorePrediction",
            SimpleNamespace(objects=lambda **filters: _Query([])),
        )
        empty_result = backtest_service.run_backtest(
            stock_code="sh600000",
            strategy="SCORE_THRESHOLD",
            start_date=days[0],
            end_date=days[-1],
            save_result=False,
            horizon=5,
            model_version="selected-v1",
        )
        assert empty_result["error"] == "No usable score data"

    def test_direct_service_call_without_model_version_fails_closed(self):
        from app.services.backtest_service import run_backtest

        result = run_backtest(
            stock_code="sh600000",
            strategy="SCORE_THRESHOLD",
            start_date=_day("2026-08-26"),
            end_date=_day("2026-08-28"),
            save_result=False,
            horizon=5,
        )

        assert result["error"] == (
            "model_version is required for score-driven strategies"
        )


class TestScoreModelVersionAPI:
    @pytest.mark.parametrize(
        ("path", "payload"),
        [
            (
                "/api/backtest/run",
                {
                    "stock_code": "sh600000",
                    "strategy": "SCORE_THRESHOLD",
                    "start_date": "2026-08-26",
                    "end_date": "2026-08-28",
                    "horizon": 5,
                },
            ),
            (
                "/api/backtest/run-multi",
                {
                    "stock_codes": ["sh600000", "sz000001"],
                    "strategy": "TOP_N_ROTATION",
                    "start_date": "2026-08-26",
                    "end_date": "2026-08-28",
                    "horizon": 5,
                },
            ),
            (
                "/api/backtest/optimize",
                {
                    "stock_code": "sh600000",
                    "strategy": "SCORE_THRESHOLD",
                    "start_date": "2026-01-01",
                    "end_date": "2026-08-28",
                    "horizon": 5,
                    "param_grid": {"entry_threshold": [70]},
                },
            ),
            (
                "/api/backtest/scan",
                {
                    "strategy": "SCORE_THRESHOLD",
                    "start_date": "2026-01-01",
                    "end_date": "2026-08-28",
                    "horizon": 5,
                },
            ),
            (
                "/api/backtest/walk-forward",
                {
                    "stock_code": "sh600000",
                    "strategy": "SCORE_THRESHOLD",
                    "start_date": "2026-01-01",
                    "end_date": "2026-08-28",
                    "horizon": 5,
                },
            ),
            (
                "/api/backtest/decay-analysis",
                {
                    "stock_code": "sh600000",
                    "strategy": "SCORE_THRESHOLD",
                    "start_date": "2026-01-01",
                    "end_date": "2026-08-28",
                    "horizon": 5,
                },
            ),
            (
                "/api/backtest/landscape",
                {
                    "stock_code": "sh600000",
                    "strategy": "SCORE_THRESHOLD",
                    "start_date": "2026-01-01",
                    "end_date": "2026-08-28",
                    "horizon": 5,
                    "param_x": "entry_threshold",
                    "x_values": [70],
                    "param_y": "stop_loss_pct",
                    "y_values": [-5],
                },
            ),
            (
                "/api/backtest/recommendation",
                {
                    "stock_code": "sh600000",
                    "start_date": "2026-01-01",
                    "end_date": "2026-08-28",
                    "horizons": [5],
                    "strategies": ["SCORE_THRESHOLD"],
                },
            ),
            (
                "/api/backtest/export/scan",
                {
                    "strategy": "SCORE_THRESHOLD",
                    "start_date": "2026-01-01",
                    "end_date": "2026-08-28",
                    "horizon": 5,
                },
            ),
            (
                "/api/backtest/export/walk-forward",
                {
                    "stock_code": "sh600000",
                    "strategy": "SCORE_THRESHOLD",
                    "start_date": "2026-01-01",
                    "end_date": "2026-08-28",
                    "horizon": 5,
                },
            ),
        ],
    )
    def test_score_driven_entry_requires_model_version(self, client, path, payload):
        response = client.post(path, json=payload)

        assert response.status_code == 400
        assert response.get_json()["message"] == (
            "model_version is required for score-driven strategies"
        )

    def test_async_backtest_task_requires_model_version(self, client):
        response = client.post(
            "/api/tasks",
            json={
                "task_type": "BACKTEST_SINGLE",
                "params": {
                    "stock_code": "sh600000",
                    "strategy": "SCORE_THRESHOLD",
                    "start_date": "2026-08-26",
                    "end_date": "2026-08-28",
                    "horizon": 5,
                },
            },
        )

        assert response.status_code == 400
        assert response.get_json()["message"] == (
            "model_version is required for score-driven strategies"
        )

    def test_run_propagates_explicit_model_version(self, client, monkeypatch):
        captured = {}

        def _run(**kwargs):
            captured.update(kwargs)
            return {"id": None, "trades": [], "daily_values": []}

        monkeypatch.setattr("app.api.v1.backtest.run_backtest", _run)
        response = client.post(
            "/api/backtest/run",
            json={
                "stock_code": "sh600000",
                "strategy": "SCORE_THRESHOLD",
                "start_date": "2026-08-26",
                "end_date": "2026-08-28",
                "horizon": 5,
                "model_version": "selected-v1",
            },
        )

        assert response.status_code == 200
        assert captured["model_version"] == "selected-v1"
