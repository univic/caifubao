import io
from types import SimpleNamespace

import pytest


class FakeQuery:
    def __init__(self, rows):
        self.rows = rows

    def first(self):
        return self.rows[0] if self.rows else None

    def order_by(self, *_fields):
        return self

    def limit(self, _count):
        return self

    def __iter__(self):
        return iter(self.rows)


def test_normalize_order_intent_payload_rejects_invalid_quantity():
    from app.api.v1 import portfolios

    with pytest.raises(ValueError, match="target_quantity"):
        portfolios._normalize_order_intent_payload(
            {"stock_code": "sh600000", "side": "BUY", "target_quantity": 0}
        )


def test_refresh_intent_status_tracks_partial_and_filled(monkeypatch):
    from app.api.v1 import portfolios

    fills = [SimpleNamespace(quantity=40.0)]
    monkeypatch.setattr(
        portfolios,
        "ExecutionFill",
        SimpleNamespace(objects=lambda **_kwargs: FakeQuery(fills)),
    )

    intent = SimpleNamespace(
        target_quantity=100.0,
        filled_quantity=0.0,
        status="OPEN",
        save=lambda: None,
    )
    portfolios._refresh_intent_status(intent)
    assert intent.filled_quantity == 40.0
    assert intent.status == "PARTIAL"

    fills.append(SimpleNamespace(quantity=60.0))
    portfolios._refresh_intent_status(intent)
    assert intent.filled_quantity == 100.0
    assert intent.status == "FILLED"


def test_duplicate_fill_is_not_applied_twice(monkeypatch):
    from app.api.v1 import portfolios

    store = {}

    class FakeFill:
        def __init__(self, **kwargs):
            self.id = "fill-1"
            self.created_at = None
            self.portfolio_transaction_id = None
            self.apply_status = "PENDING"
            self.apply_error = None
            for key, value in kwargs.items():
                setattr(self, key, value)

        @classmethod
        def objects(cls, **kwargs):
            key = (id(kwargs.get("portfolio")), kwargs.get("external_fill_id"))
            row = store.get(key)
            return FakeQuery([row] if row else [])

        def save(self, force_insert=False):
            key = (id(self.portfolio), self.external_fill_id)
            if force_insert and key in store:
                raise AssertionError("unique reservation should be checked first")
            store[key] = self
            return self

        def delete(self):
            store.pop((id(self.portfolio), self.external_fill_id), None)

    apply_calls = []

    def fake_apply_transaction(_portfolio, payload):
        apply_calls.append(payload)
        return SimpleNamespace(id="txn-1")

    monkeypatch.setattr(portfolios, "ExecutionFill", FakeFill)
    monkeypatch.setattr(portfolios, "_apply_transaction", fake_apply_transaction)
    monkeypatch.setattr(
        portfolios,
        "_stock_name",
        lambda stock_code, fallback=None: fallback or stock_code,
    )

    portfolio = SimpleNamespace(id="portfolio-1", account_mode="MANUAL_LIVE")
    payload = {
        "external_fill_id": "broker-fill-001",
        "stock_code": "sh600000",
        "side": "BUY",
        "quantity": 100,
        "price": 10,
        "fee": 5,
        "trade_time": "2026-10-06T09:31:00+08:00",
    }

    first, duplicate = portfolios._ingest_execution_fill(portfolio, payload)
    assert duplicate is False
    assert first.portfolio_transaction_id == "txn-1"
    assert len(apply_calls) == 1

    second, duplicate = portfolios._ingest_execution_fill(portfolio, payload)
    assert duplicate is True
    assert second is first
    assert second.apply_status == "APPLIED"
    assert len(apply_calls) == 1


def test_failed_apply_keeps_pending_reservation(monkeypatch):
    from app.api.v1 import portfolios

    store = {}

    class FakeFill:
        def __init__(self, **kwargs):
            self.apply_status = "PENDING"
            self.apply_error = None
            self.portfolio_transaction_id = None
            for key, value in kwargs.items():
                setattr(self, key, value)

        @classmethod
        def objects(cls, **kwargs):
            row = store.get(
                (id(kwargs.get("portfolio")), kwargs.get("external_fill_id"))
            )
            return FakeQuery([row] if row else [])

        def save(self, force_insert=False):
            store[(id(self.portfolio), self.external_fill_id)] = self
            return self

    monkeypatch.setattr(portfolios, "ExecutionFill", FakeFill)
    monkeypatch.setattr(
        portfolios,
        "_apply_transaction",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            ValueError("ledger write failed")
        ),
    )
    monkeypatch.setattr(
        portfolios, "_stock_name", lambda stock_code, fallback=None: fallback or stock_code
    )

    portfolio = SimpleNamespace(id="portfolio-1", account_mode="MANUAL_LIVE")
    payload = {
        "external_fill_id": "broker-fill-failed",
        "stock_code": "sh600000",
        "side": "BUY",
        "quantity": 100,
        "price": 10,
        "fee": 5,
        "trade_time": "2026-10-06T09:31:00+08:00",
    }

    with pytest.raises(ValueError, match="ledger write failed"):
        portfolios._ingest_execution_fill(portfolio, payload)

    saved = store[(id(portfolio), "broker-fill-failed")]
    assert saved.apply_status == "PENDING"
    assert saved.apply_error == "ledger write failed"


def test_pending_fill_fails_loud_without_reapplying(monkeypatch):
    from app.api.v1 import portfolios

    pending = SimpleNamespace(apply_status="PENDING")
    apply_calls = []
    monkeypatch.setattr(
        portfolios,
        "ExecutionFill",
        SimpleNamespace(objects=lambda **_kwargs: FakeQuery([pending])),
    )
    monkeypatch.setattr(
        portfolios,
        "_apply_transaction",
        lambda *_args, **_kwargs: apply_calls.append(True),
    )

    portfolio = SimpleNamespace(id="portfolio-1", account_mode="MANUAL_LIVE")
    payload = {
        "external_fill_id": "broker-fill-pending",
        "stock_code": "sh600000",
        "side": "BUY",
        "quantity": 100,
        "price": 10,
        "fee": 5,
        "trade_time": "2026-10-06T09:31:00+08:00",
    }

    with pytest.raises(ValueError, match="PENDING"):
        portfolios._ingest_execution_fill(portfolio, payload)

    assert apply_calls == []


def test_csv_import_reports_applied_duplicate_and_error(client, monkeypatch):
    from app.api.v1 import portfolios

    portfolio = SimpleNamespace(id="portfolio-1", account_mode="MANUAL_LIVE")
    monkeypatch.setattr(
        portfolios, "_portfolio_or_404", lambda _portfolio_id: (portfolio, None)
    )

    def fake_ingest(_portfolio, payload, import_source="JSON"):
        external_id = payload["external_fill_id"]
        if external_id == "bad":
            raise ValueError("invalid row")
        fill = SimpleNamespace(
            id=f"fill-{external_id}",
            portfolio=portfolio,
            intent=None,
            external_fill_id=external_id,
            stock_code=payload["stock_code"],
            stock_name=payload["stock_code"],
            side=payload["side"],
            quantity=float(payload["quantity"]),
            price=float(payload["price"]),
            fee=float(payload["fee"]),
            trade_time=payload["trade_time"],
            import_source=import_source,
            apply_status="APPLIED",
            apply_error=None,
            portfolio_transaction_id=f"txn-{external_id}",
            created_at=None,
        )
        return fill, external_id == "dup"

    monkeypatch.setattr(portfolios, "_ingest_execution_fill", fake_ingest)

    csv_text = (
        "external_fill_id,stock_code,side,quantity,price,fee,trade_time\n"
        "new,sh600000,BUY,100,10,5,2026-10-06T09:31:00+08:00\n"
        "dup,sh600000,BUY,100,10,5,2026-10-06T09:32:00+08:00\n"
        "bad,sh600000,BUY,100,10,5,2026-10-06T09:33:00+08:00\n"
    )
    response = client.post(
        "/api/portfolios/portfolio-1/execution/fills/import-csv",
        data={"file": (io.BytesIO(csv_text.encode("utf-8")), "fills.csv")},
        content_type="multipart/form-data",
    )

    assert response.status_code == 200
    body = response.get_json()
    assert body["applied"] == 1
    assert body["duplicates"] == 1
    assert body["errors"] == 1
    assert [row["status"] for row in body["items"]] == [
        "APPLIED",
        "DUPLICATE",
        "ERROR",
    ]


def test_reconciliation_persists_pass_and_break(monkeypatch):
    from app.api.v1 import portfolios

    position_rows = [SimpleNamespace(stock_code="sh600000", quantity=100.0)]
    monkeypatch.setattr(
        portfolios,
        "PortfolioPosition",
        SimpleNamespace(objects=lambda **_kwargs: FakeQuery(position_rows)),
    )

    saved = []

    class FakeReconciliation:
        def __init__(self, **kwargs):
            self.id = f"r-{len(saved) + 1}"
            self.created_at = None
            for key, value in kwargs.items():
                setattr(self, key, value)

        def save(self):
            saved.append(self)
            return self

    monkeypatch.setattr(portfolios, "AccountReconciliation", FakeReconciliation)

    portfolio = SimpleNamespace(
        id="portfolio-1", cash=1000.0, account_mode="MANUAL_LIVE"
    )

    matched = portfolios._build_reconciliation(
        portfolio,
        {
            "cash": 1000.0,
            "positions": [{"stock_code": "sh600000", "quantity": 100}],
            "as_of": "2026-10-06T15:00:00+08:00",
        },
    )
    assert matched.status == "PASS"
    assert matched.breaks == []

    broken = portfolios._build_reconciliation(
        portfolio,
        {
            "cash": 990.0,
            "positions": [
                {"stock_code": "sh600000", "quantity": 90},
                {"stock_code": "sz000001", "quantity": 100},
            ],
            "as_of": "2026-10-06T15:00:00+08:00",
        },
    )
    assert broken.status == "BREAK"
    assert {row["type"] for row in broken.breaks} == {
        "CASH_DRIFT",
        "QUANTITY_DRIFT",
        "UNEXPECTED_POSITION",
    }
    assert len(saved) == 2


def test_execution_routes_reject_research_portfolio(client, monkeypatch):
    from app.api.v1 import portfolios

    portfolio = SimpleNamespace(id="portfolio-1", account_mode="RESEARCH")
    monkeypatch.setattr(
        portfolios, "_portfolio_or_404", lambda _portfolio_id: (portfolio, None)
    )

    response = client.get("/api/portfolios/portfolio-1/execution/fills")

    assert response.status_code == 409
    assert "MANUAL_LIVE" in response.get_json()["message"]
