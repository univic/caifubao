from types import SimpleNamespace


class FakePositionQuery:
    def __init__(self, rows):
        self.rows = rows

    def order_by(self, *_fields):
        return self

    def __iter__(self):
        return iter(self.rows)


def test_build_summary_uses_latest_prices(monkeypatch):
    from app.api.v1 import portfolios

    portfolio = SimpleNamespace(cash=1000.0, initial_cash=2000.0)
    rows = [
        SimpleNamespace(
            id="p1",
            stock_code="sh600000",
            stock_name="浦发银行",
            quantity=100,
            avg_cost=9.0,
            realized_pnl=0.0,
            updated_at=None,
        ),
        SimpleNamespace(
            id="p2",
            stock_code="sz000001",
            stock_name="平安银行",
            quantity=50,
            avg_cost=12.0,
            realized_pnl=0.0,
            updated_at=None,
        ),
    ]

    monkeypatch.setattr(
        portfolios,
        "PortfolioPosition",
        SimpleNamespace(objects=lambda **_kwargs: FakePositionQuery(rows)),
    )
    monkeypatch.setattr(
        portfolios,
        "_latest_price",
        lambda stock_code, fallback=0.0: (
            {"sh600000": 10.0, "sz000001": 11.0}[stock_code],
            None,
        ),
    )

    summary = portfolios._build_summary(portfolio)

    assert summary["cash"] == 1000.0
    assert summary["positions_value"] == 1550.0
    assert summary["total_value"] == 2550.0
    assert summary["total_return"] == 550.0
    assert summary["total_return_pct"] == 0.275
    assert summary["position_count"] == 2
