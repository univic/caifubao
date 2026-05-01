import datetime
from types import SimpleNamespace


class FakeSignalQuery:
    def __init__(self, rows):
        self.rows = list(rows)

    def filter(self, **kwargs):
        rows = self.rows
        for key, value in kwargs.items():
            rows = [row for row in rows if getattr(row, key) == value]
        return FakeSignalQuery(rows)

    def order_by(self, *fields):
        rows = self.rows
        for field in reversed(fields):
            reverse = field.startswith("-")
            name = field.removeprefix("-")
            rows = sorted(
                rows, key=lambda row: getattr(row, name) or 0, reverse=reverse
            )
        return FakeSignalQuery(rows)

    def only(self, *fields):
        return self

    def first(self):
        return self.rows[0] if self.rows else None

    def count(self):
        return len(self.rows)

    def skip(self, offset):
        return FakeSignalQuery(self.rows[offset:])

    def limit(self, limit):
        return FakeSignalQuery(self.rows[:limit])

    def __iter__(self):
        return iter(self.rows)


def test_list_signals_defaults_to_latest_date(client, monkeypatch):
    from app.api.v1 import signals

    rows = [
        SimpleNamespace(
            stock_code="sh600000",
            stock_name="浦发银行",
            category="stock",
            date=datetime.datetime(2026, 4, 13),
            signal_name="MA10_CROSS_MA20",
            signal_version="v1",
            direction="BULLISH",
            signal_type="MA_CROSS",
            strength=1.2,
            reason="MA10 上穿 MA20",
            price_snapshot={},
            factor_snapshot={"ma_10": 10.2, "ma_20": 10.0},
            source_freshness={
                "MA_10": {"latest_data_date": datetime.datetime(2026, 4, 13)}
            },
            generated_at=datetime.datetime(2026, 4, 14, 9, 0, 0),
        ),
        SimpleNamespace(
            stock_code="sz000001",
            stock_name="平安银行",
            category="stock",
            date=datetime.datetime(2026, 4, 12),
            signal_name="MA10_CROSS_MA20",
            signal_version="v1",
            direction="BULLISH",
            signal_type="MA_CROSS",
            strength=0.8,
            reason="old",
            price_snapshot={},
            factor_snapshot={},
            source_freshness={},
            generated_at=datetime.datetime(2026, 4, 13, 9, 0, 0),
        ),
    ]
    monkeypatch.setattr(
        signals,
        "StockSignalDaily",
        SimpleNamespace(objects=lambda: FakeSignalQuery(rows)),
    )

    response = client.get("/api/signals")

    assert response.status_code == 200
    payload = response.get_json()
    assert payload["date"] == "2026-04-13T00:00:00"
    assert payload["total"] == 1
    assert payload["items"][0]["stock_code"] == "sh600000"
    assert (
        payload["items"][0]["source_freshness"]["MA_10"]["latest_data_date"]
        == "2026-04-13T00:00:00"
    )


def test_list_signals_returns_empty_when_no_data(client, monkeypatch):
    from app.api.v1 import signals

    monkeypatch.setattr(
        signals,
        "StockSignalDaily",
        SimpleNamespace(objects=lambda: FakeSignalQuery([])),
    )

    response = client.get("/api/signals")

    assert response.status_code == 200
    payload = response.get_json()
    assert payload == {
        "date": None,
        "requested_date": None,
        "total": 0,
        "limit": 50,
        "offset": 0,
        "items": [],
    }
