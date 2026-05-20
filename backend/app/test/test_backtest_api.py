# -*- coding: utf-8 -*-
"""Tests for the MVP backtest API endpoints."""

import datetime
from types import SimpleNamespace


# ---------------------------------------------------------------------------
# Fake mongoengine query helpers
# ---------------------------------------------------------------------------
class FakeQuery:
    """Minimal mongoengine-style query chain for backtest result lookups."""

    def __init__(self, rows):
        self.rows = list(rows)

    def __call__(self, **kwargs):
        """Support mongoengine-style MyDocument.objects(**filters) syntax."""
        if not kwargs:
            return type(self)(self.rows)
        return self.filter(**kwargs)

    def filter(self, **kwargs):
        rows = self.rows
        for key, value in kwargs.items():
            rows = [r for r in rows if _getattr(r, key) == value]
        return type(self)(rows)

    def order_by(self, *fields):
        rows = self.rows
        for field in reversed(fields):
            reverse = field.startswith("-")
            name = field.removeprefix("-")
            rows = sorted(rows, key=lambda r: _getattr(r, name) or "", reverse=reverse)
        return type(self)(rows)

    def skip(self, n):
        return type(self)(self.rows[n:])

    def limit(self, n):
        return type(self)(self.rows[:n])

    def count(self):
        return len(self.rows)

    def first(self):
        return self.rows[0] if self.rows else None

    def only(self, *fields):
        return self

    def __iter__(self):
        return iter(self.rows)


def _getattr(obj, name):
    val = getattr(obj, name, None)
    if isinstance(val, datetime.datetime):
        return val.replace(tzinfo=None)
    return val


# ---------------------------------------------------------------------------
# Fixture: a sample completed backtest row
# ---------------------------------------------------------------------------
def _make_row(idx: int) -> SimpleNamespace:
    return SimpleNamespace(
        id=f"bt_result_{idx}",
        name=f"sh600519-MA_CROSS-2024-01-01-2024-06-30-{idx}",
        stock_code="sh600519",
        stock_name="贵州茅台",
        strategy="MA_CROSS",
        start_date=datetime.datetime(2024, 1, 1),
        end_date=datetime.datetime(2024, 6, 30),
        initial_cash=100000.0,
        final_value=105234.5,
        total_return=5234.5,
        total_return_pct=5.23,
        annualized_return=10.46,
        max_drawdown=-8.5,
        max_drawdown_duration=15,
        sharpe_ratio=0.85,
        win_rate=60.0,
        total_trades=5,
        profit_trades=3,
        loss_trades=2,
        best_trade=3200.0,
        worst_trade=-1200.0,
        status="COMPLETED",
        error_message=None,
        trades=[
            {
                "date": "2024-01-15T00:00:00",
                "side": "BUY",
                "price": 1650.0,
                "quantity": 60,
                "amount": 99000.0,
                "reason": "Golden cross",
            },
            {
                "date": "2024-02-20T00:00:00",
                "side": "SELL",
                "price": 1703.33,
                "quantity": 60,
                "amount": 102200.0,
                "pnl": 3200.0,
                "reason": "Dead cross",
            },
        ],
        daily_values=[
            {
                "date": "2024-01-15T00:00:00",
                "close": 1650.0,
                "cash": 1000.0,
                "shares": 60,
                "equity": 100000.0,
            },
            {
                "date": "2024-01-16T00:00:00",
                "close": 1660.0,
                "cash": 1000.0,
                "shares": 60,
                "equity": 100600.0,
            },
        ],
        created_at=datetime.datetime(2024, 7, 1, 12, 0, 0),
        completed_at=datetime.datetime(2024, 7, 1, 12, 0, 1),
        total_commission=0.0,
        total_stamp_duty=0.0,
        total_slippage=0.0,
        gross_return=5234.5,
        gross_return_pct=5.23,
        benchmark_code="sh000300",
        benchmark_return=None,
        benchmark_return_pct=None,
        benchmark_annualized_return=None,
        excess_return=None,
        excess_return_pct=None,
        information_ratio=None,
        score_config=None,
        horizon=None,
        top_n=None,
        rebalance_interval=None,
        allocation=None,
        per_stock_contributions=None,
    )


# ---------------------------------------------------------------------------
# Helper: build fake StockDailyQuote list
# ---------------------------------------------------------------------------
def _fake_quotes_map():
    """Build a dict of {date: SimpleNamespace} mimicking StockDailyQuote."""
    return {
        datetime.datetime(2024, 1, 15): SimpleNamespace(
            code="sh600519",
            date=datetime.datetime(2024, 1, 15),
            close=1645.0,
            close_hfq=1650.0,
        ),
        datetime.datetime(2024, 1, 16): SimpleNamespace(
            code="sh600519",
            date=datetime.datetime(2024, 1, 16),
            close=1655.0,
            close_hfq=1660.0,
        ),
        datetime.datetime(2024, 1, 17): SimpleNamespace(
            code="sh600519",
            date=datetime.datetime(2024, 1, 17),
            close=1665.0,
            close_hfq=1670.0,
        ),
    }


def _fake_factors_map():
    """Build a dict of {date: SimpleNamespace} mimicking StockFactorDaily."""
    ma_base = 100.0
    return {
        datetime.datetime(2024, 1, 15): SimpleNamespace(
            stock_code="sh600519",
            date=datetime.datetime(2024, 1, 15),
            ma_10=ma_base + 5,
            ma_20=ma_base - 1,  # MA10 > MA20 already
        ),
        datetime.datetime(2024, 1, 16): SimpleNamespace(
            stock_code="sh600519",
            date=datetime.datetime(2024, 1, 16),
            ma_10=ma_base + 10,
            ma_20=ma_base + 3,
        ),
    }


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------
class TestBacktestListAPI:
    """GET /api/backtest – list completed backtests."""

    def test_list_returns_empty(self, client, monkeypatch):
        from app.model import backtest as bt_mod

        monkeypatch.setattr(bt_mod.BacktestResult, "objects", FakeQuery([]))

        resp = client.get("/api/backtest")
        assert resp.status_code == 200
        body = resp.get_json()
        assert body["success"] is True
        assert body["data"]["total"] == 0
        assert body["data"]["items"] == []

    def test_list_returns_items(self, client, monkeypatch):
        from app.model import backtest as bt_mod

        rows = [_make_row(i) for i in range(3)]
        monkeypatch.setattr(bt_mod.BacktestResult, "objects", FakeQuery(rows))

        resp = client.get("/api/backtest?limit=10")
        assert resp.status_code == 200
        body = resp.get_json()
        assert body["data"]["total"] == 3
        assert len(body["data"]["items"]) == 3
        assert body["data"]["items"][0]["stock_code"] == "sh600519"

    def test_list_pagination(self, client, monkeypatch):
        from app.model import backtest as bt_mod

        rows = [_make_row(i) for i in range(5)]
        monkeypatch.setattr(bt_mod.BacktestResult, "objects", FakeQuery(rows))

        resp = client.get("/api/backtest?limit=2&offset=2")
        assert resp.status_code == 200
        body = resp.get_json()
        assert body["data"]["total"] == 5
        assert len(body["data"]["items"]) == 2


class TestBacktestGetAPI:
    """GET /api/backtest/<id> – single backtest detail."""

    def test_get_not_found(self, client, monkeypatch):
        from app.model import backtest as bt_mod

        monkeypatch.setattr(bt_mod.BacktestResult, "objects", FakeQuery([]))

        resp = client.get("/api/backtest/nonexistent")
        assert resp.status_code == 404
        body = resp.get_json()
        assert body["success"] is False

    def test_get_returns_detail(self, client, monkeypatch):
        from app.model import backtest as bt_mod

        row = _make_row(1)
        monkeypatch.setattr(bt_mod.BacktestResult, "objects", FakeQuery([row]))

        resp = client.get("/api/backtest/bt_result_1")
        assert resp.status_code == 200
        body = resp.get_json()
        assert body["success"] is True
        data = body["data"]
        assert data["id"] == "bt_result_1"
        assert data["stock_code"] == "sh600519"
        assert data["strategy"] == "MA_CROSS"
        # Detail includes trades and daily_values
        assert data.get("trades") is not None
        assert data.get("daily_values") is not None
        assert len(data["trades"]) == 2
        assert len(data["daily_values"]) == 2


class TestBacktestDeleteAPI:
    """DELETE /api/backtest/<id> – delete a backtest result."""

    def test_delete_not_found(self, client, monkeypatch):
        from app.model import backtest as bt_mod

        monkeypatch.setattr(bt_mod.BacktestResult, "objects", FakeQuery([]))

        resp = client.delete("/api/backtest/nonexistent")
        assert resp.status_code == 404
        body = resp.get_json()
        assert body["success"] is False

    def test_delete_success(self, client, monkeypatch):
        from app.model import backtest as bt_mod

        row = _make_row(1)
        deleted = {}

        class FakeQueryWithDelete(FakeQuery):
            def first(self):
                return self.rows[0] if self.rows else None

        # Patch row.delete() before creating the query so the patched attribute
        # is on the same object instance that first() will return.
        row.delete = lambda: deleted.update({"deleted": True})
        monkeypatch.setattr(
            bt_mod.BacktestResult, "objects", FakeQueryWithDelete([row])
        )

        resp = client.delete("/api/backtest/bt_result_1")
        assert resp.status_code == 200
        body = resp.get_json()
        assert body["success"] is True
        assert deleted.get("deleted") is True


class TestBacktestRunAPI:
    """POST /api/backtest/run – run a new backtest."""

    def test_missing_stock_code(self, client):
        resp = client.post(
            "/api/backtest/run",
            json={
                "strategy": "MA_CROSS",
                "start_date": "2024-01-01",
                "end_date": "2024-06-30",
            },
        )
        assert resp.status_code == 400
        body = resp.get_json()
        assert body["success"] is False
        assert "stock_code" in body["message"]

    def test_missing_strategy(self, client):
        resp = client.post(
            "/api/backtest/run",
            json={
                "stock_code": "sh600519",
                "start_date": "2024-01-01",
                "end_date": "2024-06-30",
            },
        )
        assert resp.status_code == 400
        body = resp.get_json()
        assert body["success"] is False
        assert "strategy" in body["message"]

    def test_invalid_date(self, client):
        resp = client.post(
            "/api/backtest/run",
            json={
                "stock_code": "sh600519",
                "strategy": "MA_CROSS",
                "start_date": "not-a-date",
                "end_date": "2024-06-30",
            },
        )
        assert resp.status_code == 400
        body = resp.get_json()
        assert body["success"] is False

    def test_date_range_order(self, client):
        resp = client.post(
            "/api/backtest/run",
            json={
                "stock_code": "sh600519",
                "strategy": "MA_CROSS",
                "start_date": "2024-06-30",
                "end_date": "2024-01-01",
            },
        )
        assert resp.status_code == 400
        body = resp.get_json()
        assert body["success"] is False
        assert "start_date" in body["message"]

    def test_unsupported_strategy(self, client, monkeypatch):
        """An unsupported strategy should be caught by the service layer."""

        def _fake_bt(*args, **kwargs):
            return {"error": "Unsupported strategy", "detail": "strategy=UNKNOWN"}

        monkeypatch.setattr("app.api.v1.backtest.run_backtest", _fake_bt)

        resp = client.post(
            "/api/backtest/run",
            json={
                "stock_code": "sh600519",
                "strategy": "UNKNOWN",
                "start_date": "2024-01-01",
                "end_date": "2024-06-30",
            },
        )
        assert resp.status_code == 400
        body = resp.get_json()
        assert body["success"] is False

    def test_successful_backtest(self, client, monkeypatch):
        """End-to-end: the service returns a valid result dict."""

        success_result = {
            "id": "bt_abc123",
            "name": "sh600519-MA_CROSS-2024-01-01-2024-06-30-ts",
            "final_value": 105000.0,
            "total_return": 5000.0,
            "total_return_pct": 5.0,
            "annualized_return": 10.0,
            "max_drawdown": -3.5,
            "max_drawdown_duration": 7,
            "sharpe_ratio": 0.9,
            "win_rate": 50.0,
            "total_trades": 2,
            "profit_trades": 1,
            "loss_trades": 1,
            "best_trade": 4000.0,
            "worst_trade": -2000.0,
            "trades": [
                {
                    "date": "2024-01-15T00:00:00",
                    "side": "BUY",
                    "price": 1650.0,
                    "quantity": 60,
                    "amount": 99000.0,
                    "reason": "Golden cross",
                },
                {
                    "date": "2024-02-20T00:00:00",
                    "side": "SELL",
                    "price": 1716.67,
                    "quantity": 60,
                    "amount": 103000.0,
                    "pnl": 4000.0,
                    "reason": "Dead cross",
                },
            ],
            "daily_values": [
                {
                    "date": "2024-01-15T00:00:00",
                    "close": 1650.0,
                    "cash": 1000.0,
                    "shares": 60,
                    "equity": 100000.0,
                },
            ],
        }
        monkeypatch.setattr(
            "app.api.v1.backtest.run_backtest", lambda **kw: success_result
        )

        resp = client.post(
            "/api/backtest/run",
            json={
                "stock_code": "sh600519",
                "strategy": "MA_CROSS",
                "start_date": "2024-01-01",
                "end_date": "2024-06-30",
                "initial_cash": 100000,
            },
        )
        assert resp.status_code == 200
        body = resp.get_json()
        assert body["success"] is True
        data = body["data"]
        assert data["final_value"] == 105000.0
        assert data["total_return_pct"] == 5.0

    def test_initial_cash_default(self, client, monkeypatch):
        """initial_cash should default to 100000 when not provided."""

        captured_kwargs = {}

        def _fake_bt(**kwargs):
            captured_kwargs.update(kwargs)
            return {
                "id": "bt_test",
                "final_value": 100000.0,
                "total_return": 0.0,
                "total_return_pct": 0.0,
                "annualized_return": 0.0,
                "max_drawdown": 0.0,
                "max_drawdown_duration": 0,
                "sharpe_ratio": 0.0,
                "win_rate": 0.0,
                "total_trades": 0,
                "profit_trades": 0,
                "loss_trades": 0,
                "best_trade": 0.0,
                "worst_trade": 0.0,
                "trades": [],
                "daily_values": [],
            }

        monkeypatch.setattr("app.api.v1.backtest.run_backtest", _fake_bt)

        client.post(
            "/api/backtest/run",
            json={
                "stock_code": "sh600519",
                "strategy": "BUY_HOLD",
                "start_date": "2024-01-01",
                "end_date": "2024-06-30",
                # initial_cash omitted
            },
        )
        assert captured_kwargs.get("initial_cash") == 100000.0

    def test_negative_initial_cash(self, client):
        resp = client.post(
            "/api/backtest/run",
            json={
                "stock_code": "sh600519",
                "strategy": "MA_CROSS",
                "start_date": "2024-01-01",
                "end_date": "2024-06-30",
                "initial_cash": -100,
            },
        )
        assert resp.status_code == 400
        body = resp.get_json()
        assert body["success"] is False


class TestBacktestResponseEnvelope:
    """Verify the response envelope shape on all endpoints."""

    def test_run_envelope(self, client, monkeypatch):

        success_result = {
            "id": "bt_env",
            "name": "env-test",
            "final_value": 100000.0,
            "total_return": 0.0,
            "total_return_pct": 0.0,
            "annualized_return": 0.0,
            "max_drawdown": 0.0,
            "max_drawdown_duration": 0,
            "sharpe_ratio": 0.0,
            "win_rate": 0.0,
            "total_trades": 0,
            "profit_trades": 0,
            "loss_trades": 0,
            "best_trade": 0.0,
            "worst_trade": 0.0,
            "trades": [],
            "daily_values": [],
        }
        monkeypatch.setattr(
            "app.api.v1.backtest.run_backtest", lambda **kw: success_result
        )

        resp = client.post(
            "/api/backtest/run",
            json={
                "stock_code": "sh600519",
                "strategy": "BUY_HOLD",
                "start_date": "2024-01-01",
                "end_date": "2024-06-30",
            },
        )
        body = resp.get_json()
        assert "success" in body
        assert "message" in body
        assert "request_id" in body
        assert "generated_at" in body
        assert "data" in body
        assert body["data"]["id"] is not None

    def test_list_envelope(self, client, monkeypatch):
        from app.model import backtest as bt_mod

        monkeypatch.setattr(bt_mod.BacktestResult, "objects", FakeQuery([]))

        resp = client.get("/api/backtest")
        body = resp.get_json()
        assert "success" in body
        assert body["success"] is True
        assert "data" in body
        assert body["data"]["total"] == 0
        assert body["data"]["items"] == []

    def test_get_envelope(self, client, monkeypatch):
        from app.model import backtest as bt_mod

        row = _make_row(1)
        monkeypatch.setattr(bt_mod.BacktestResult, "objects", FakeQuery([row]))

        resp = client.get("/api/backtest/bt_result_1")
        body = resp.get_json()
        assert body["success"] is True
        assert "data" in body
        assert body["data"]["id"] == "bt_result_1"

    def test_error_envelope(self, client):
        resp = client.post(
            "/api/backtest/run",
            json={"stock_code": "sh600519"},
        )
        body = resp.get_json()
        assert body["success"] is False
        assert "message" in body
        assert "request_id" in body
        assert "generated_at" in body
        assert body["data"] is None


# ============================================================================
# Optimize endpoint tests
# ============================================================================
class TestBacktestOptimizeAPI:
    """POST /api/backtest/optimize"""

    @staticmethod
    def _make_run_backtest(*, sharpe=1.0, return_pct=10.0, trades=10):
        """Factory for mock run_backtest results."""
        return {
            "sharpe_ratio": sharpe,
            "total_return_pct": return_pct,
            "max_drawdown": -15.0,
            "total_trades": trades,
            "excess_return_pct": 5.0,
        }

    def test_optimize_runs_and_returns_best(self, client, monkeypatch):
        """Happy path: optimize with param_grid, returns ranked results."""

        # Mock run_backtest to return deterministic results based on entry_threshold
        def _mock_run(**kw):
            entry = kw.get("entry_threshold", 70)
            sharpe = 2.0 if entry == 60 else 1.0 if entry == 70 else 0.5
            return self._make_run_backtest(sharpe=sharpe)

        monkeypatch.setattr("app.api.v1.backtest.run_backtest", _mock_run)

        resp = client.post(
            "/api/backtest/optimize",
            json={
                "stock_code": "sz000977",
                "strategy": "SCORE_THRESHOLD",
                "start_date": "2025-01-01",
                "end_date": "2025-12-31",
                "horizon": 5,
                "use_split": False,
                "param_grid": {
                    "entry_threshold": [50, 60, 70],
                },
            },
        )

        assert resp.status_code == 200
        body = resp.get_json()
        assert body["success"] is True
        data = body["data"]
        assert data["strategy"] == "SCORE_THRESHOLD"
        assert data["horizon"] == 5
        assert data["total_combinations"] == 3
        assert data["completed"] == 3
        # Best should be entry=60 (Sharpe 2.0)
        assert data["best"]["params"]["entry_threshold"] == 60
        assert data["best"]["val_sharpe_ratio"] == 2.0
        assert len(data["results"]) == 3
        # Results sorted by val Sharpe descending
        assert (
            data["results"][0]["val_sharpe_ratio"]
            >= data["results"][-1]["val_sharpe_ratio"]
        )

    def test_optimize_requires_strategy(self, client):
        """Rejects unknown strategies."""
        resp = client.post(
            "/api/backtest/optimize",
            json={
                "stock_code": "sz000977",
                "strategy": "MA_CROSS",
                "start_date": "2025-01-01",
                "end_date": "2025-12-31",
                "horizon": 5,
                "param_grid": {"entry_threshold": [50]},
            },
        )
        assert resp.status_code != 200
        body = resp.get_json()
        assert body["success"] is False

    def test_optimize_rejects_unknown_param_key(self, client):
        """Rejects param keys not in the per-strategy whitelist."""
        resp = client.post(
            "/api/backtest/optimize",
            json={
                "stock_code": "sz000977",
                "strategy": "SCORE_THRESHOLD",
                "start_date": "2025-01-01",
                "end_date": "2025-12-31",
                "horizon": 5,
                "param_grid": {
                    "entry_threshold": [50],
                    "invalid_param": [10, 20],
                },
            },
        )
        body = resp.get_json()
        assert body["success"] is False
        assert (
            "invalid_param" in body["message"].lower() or "Unknown" in body["message"]
        )

    def test_optimize_rejects_non_list_param_value(self, client):
        """Rejects param_grid values that are not lists."""
        resp = client.post(
            "/api/backtest/optimize",
            json={
                "stock_code": "sz000977",
                "strategy": "SCORE_THRESHOLD",
                "start_date": "2025-01-01",
                "end_date": "2025-12-31",
                "horizon": 5,
                "param_grid": {"entry_threshold": 50},
            },
        )
        body = resp.get_json()
        assert body["success"] is False
        assert "non-empty list" in body["message"]

    def test_optimize_empty_param_grid(self, client):
        """Rejects empty param_grid."""
        resp = client.post(
            "/api/backtest/optimize",
            json={
                "stock_code": "sz000977",
                "strategy": "SCORE_THRESHOLD",
                "start_date": "2025-01-01",
                "end_date": "2025-12-31",
                "horizon": 5,
                "param_grid": {},
            },
        )
        body = resp.get_json()
        assert body["success"] is False


# ============================================================================
# Consensus threshold validation tests
# ============================================================================
class TestConsensusThresholdValidation:
    """POST /api/backtest/run with MULTI_HORIZON_CONSENSUS thresholds."""

    def test_rejects_non_integer_threshold_keys(self, client, monkeypatch):
        """Consensus threshold dict keys must be integers 5/20/60."""
        # Mock run_backtest to avoid actual DB
        monkeypatch.setattr(
            "app.api.v1.backtest.run_backtest",
            lambda **kw: {"error": None, "id": "test", "total_return": 0},
        )

        resp = client.post(
            "/api/backtest/run",
            json={
                "stock_code": "sz000977",
                "strategy": "MULTI_HORIZON_CONSENSUS",
                "start_date": "2025-01-01",
                "end_date": "2025-06-30",
                "consensus_entry_thresholds": {"wrong_key": 50},
            },
        )
        body = resp.get_json()
        assert body["success"] is False
        assert "key" in body["message"].lower()

    def test_accepts_string_keys_that_are_valid(self, client, monkeypatch):
        """String keys '5', '20', '60' are normalized to int."""
        captured = {}

        def _capture(**kw):
            captured.update(kw)
            return {
                "id": "test",
                "total_return": 10000.0,
                "total_return_pct": 10.0,
                "total_trades": 0,
                "total_commission": 0.0,
                "total_stamp_duty": 0.0,
                "total_slippage": 0.0,
                "sharpe_ratio": 1.0,
                "max_drawdown": 0.0,
                "win_rate": 100.0,
                "profit_trades": 0,
                "loss_trades": 0,
                "best_trade": 0.0,
                "worst_trade": 0.0,
                "gross_return": 10000.0,
                "gross_return_pct": 10.0,
                "annualized_return": 10.0,
                "max_drawdown_duration": 0,
                "trades": [],
                "daily_values": [],
            }

        monkeypatch.setattr("app.api.v1.backtest.run_backtest", _capture)

        resp = client.post(
            "/api/backtest/run",
            json={
                "stock_code": "sz000977",
                "strategy": "MULTI_HORIZON_CONSENSUS",
                "start_date": "2025-01-01",
                "end_date": "2025-06-30",
                "consensus_entry_thresholds": {"5": 45},
                "consensus_exit_thresholds": {"5": 20},
            },
        )
        assert resp.status_code == 200, resp.get_json()
        # Keys should be normalized to int
        assert captured.get("consensus_entry_thresholds") == {5: 45.0}
        assert captured.get("consensus_exit_thresholds") == {5: 20.0}


# ============================================================================
# Component contribution tests
# ============================================================================
class TestComponentContributionAPI:
    """GET /api/backtest/<id>/component-contribution"""

    def test_component_contribution_not_found(self, client, monkeypatch):
        """Returns 404 for missing backtest."""
        from app.model import backtest as bt_mod

        monkeypatch.setattr(bt_mod.BacktestResult, "objects", FakeQuery([]))
        resp = client.get("/api/backtest/nonexistent/component-contribution")
        assert resp.status_code == 404

    def test_component_contribution_rejects_non_score_strategy(
        self, client, monkeypatch
    ):
        """Returns 400 for non-score-driven strategies."""
        from app.model import backtest as bt_mod

        row = SimpleNamespace(
            id="bt_comp_2",
            stock_code="sz000977",
            strategy="MA_CROSS",
            horizon=None,
            trades=[{"date": "2025-06-15T00:00:00", "side": "BUY", "pnl": None}],
        )
        monkeypatch.setattr(bt_mod.BacktestResult, "objects", FakeQuery([row]))
        resp = client.get("/api/backtest/bt_comp_2/component-contribution")
        assert resp.status_code == 400, resp.get_json()


# ============================================================================
# Factor evaluation tests
# ============================================================================
class TestFactorEvaluateAPI:
    """POST /api/backtest/evaluate-factor"""

    def test_evaluate_factor_requires_component_id(self, client):
        """Rejects request without component_id."""
        resp = client.post(
            "/api/backtest/evaluate-factor",
            json={
                "start_date": "2025-01-01",
                "end_date": "2025-06-30",
            },
        )
        assert resp.status_code != 200
        body = resp.get_json()
        assert body["success"] is False

    def test_evaluate_factor_rejects_invalid_horizons(self, client):
        """Rejects non-list horizons."""
        resp = client.post(
            "/api/backtest/evaluate-factor",
            json={
                "component_id": "momentum",
                "start_date": "2025-01-01",
                "end_date": "2025-06-30",
                "horizons": "not_a_list",
            },
        )
        assert resp.status_code != 200
        body = resp.get_json()
        assert body["success"] is False

    def test_evaluate_factor_missing_dates(self, client):
        """Rejects missing dates."""
        resp = client.post(
            "/api/backtest/evaluate-factor",
            json={"component_id": "momentum"},
        )
        assert resp.status_code != 200
        body = resp.get_json()
        assert body["success"] is False


# ============================================================================
# Significance test endpoint
# ============================================================================
class TestSignificanceAPI:
    """GET /api/backtest/<id>/significance"""

    def test_significance_returns_permutation_and_bootstrap(self, client, monkeypatch):
        """Returns both permutation and bootstrap results."""
        from app.model import backtest as bt_mod

        daily_ret = [0.005] * 200 + [-0.15] * 10 + [0.005] * 42
        equity = 100000.0
        dv = []
        for r in daily_ret:
            equity *= 1 + r
            dv.append({"equity": equity, "close": 10.0})
        row = SimpleNamespace(
            id="sig_test_1",
            stock_code="sz000977",
            strategy="MA_CROSS",
            daily_values=dv,
            initial_cash=100000.0,
        )
        monkeypatch.setattr(bt_mod.BacktestResult, "objects", FakeQuery([row]))

        resp = client.get("/api/backtest/sig_test_1/significance")
        assert resp.status_code == 200
        body = resp.get_json()
        data = body["data"]
        assert "permutation" in data
        assert "bootstrap" in data

    def test_significance_not_found(self, client, monkeypatch):
        """404 for missing backtest."""
        from app.model import backtest as bt_mod

        monkeypatch.setattr(bt_mod.BacktestResult, "objects", FakeQuery([]))
        resp = client.get("/api/backtest/nonexistent/significance")
        assert resp.status_code == 404
