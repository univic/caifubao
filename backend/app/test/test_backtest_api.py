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

    def filter(self, **kwargs):
        rows = self.rows
        for key, value in kwargs.items():
            rows = [r for r in rows if _getattr(r, key) == value]
        return FakeQuery(rows)

    def order_by(self, *fields):
        rows = self.rows
        for field in reversed(fields):
            reverse = field.startswith("-")
            name = field.removeprefix("-")
            rows = sorted(
                rows, key=lambda r: _getattr(r, name) or "", reverse=reverse
            )
        return FakeQuery(rows)

    def skip(self, n):
        return FakeQuery(self.rows[n:])

    def limit(self, n):
        return FakeQuery(self.rows[:n])

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
            {"date": "2024-01-15T00:00:00", "close": 1650.0, "cash": 1000.0, "shares": 60, "equity": 100000.0},
            {"date": "2024-01-16T00:00:00", "close": 1660.0, "cash": 1000.0, "shares": 60, "equity": 100600.0},
        ],
        created_at=datetime.datetime(2024, 7, 1, 12, 0, 0),
        completed_at=datetime.datetime(2024, 7, 1, 12, 0, 1),
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

        monkeypatch.setattr(bt_mod.BacktestResult, "objects", FakeQueryWithDelete([row]))
        # Patch row.delete() to record the call
        monkeypatch.setattr(row, "delete", lambda: deleted.update({"deleted": True}))

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
            json={"strategy": "MA_CROSS", "start_date": "2024-01-01", "end_date": "2024-06-30"},
        )
        assert resp.status_code == 400
        body = resp.get_json()
        assert body["success"] is False
        assert "stock_code" in body["message"]

    def test_missing_strategy(self, client):
        resp = client.post(
            "/api/backtest/run",
            json={"stock_code": "sh600519", "start_date": "2024-01-01", "end_date": "2024-06-30"},
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
        from app.services import backtest_service

        def _fake_bt(*args, **kwargs):
            return {"error": "Unsupported strategy", "detail": "strategy=UNKNOWN"}

        monkeypatch.setattr(backtest_service, "run_backtest", _fake_bt)

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
        from app.services import backtest_service

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
                {"date": "2024-01-15T00:00:00", "side": "BUY", "price": 1650.0, "quantity": 60, "amount": 99000.0, "reason": "Golden cross"},
                {"date": "2024-02-20T00:00:00", "side": "SELL", "price": 1716.67, "quantity": 60, "amount": 103000.0, "pnl": 4000.0, "reason": "Dead cross"},
            ],
            "daily_values": [
                {"date": "2024-01-15T00:00:00", "close": 1650.0, "cash": 1000.0, "shares": 60, "equity": 100000.0},
            ],
        }
        monkeypatch.setattr(backtest_service, "run_backtest", lambda **kw: success_result)

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
        from app.services import backtest_service

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

        monkeypatch.setattr(backtest_service, "run_backtest", _fake_bt)

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
        from app.services import backtest_service

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
        monkeypatch.setattr(backtest_service, "run_backtest", lambda **kw: success_result)

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
