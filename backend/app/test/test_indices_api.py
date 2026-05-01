import datetime
from types import SimpleNamespace

import pytest
from flask_jwt_extended import create_access_token


class FakeIndexQuery:
    def __init__(self, items):
        self.items = items

    def first(self):
        return self.items[0] if self.items else None

    def count(self):
        return len(self.items)

    def only(self, *_args, **_kwargs):
        return self.items

    def __iter__(self):
        return iter(self.items)


@pytest.fixture
def jwt_headers(app):
    with app.app_context():
        token = create_access_token(identity="test-user")
    return {"Authorization": f"Bearer {token}"}


def test_get_indices_overview_returns_derived_metrics(client, jwt_headers, monkeypatch):
    from app.api.v1 import indices

    monkeypatch.setattr(indices, "MAIN_INDICES", ["000001"])

    index = SimpleNamespace(code="000001", name="上证指数")
    latest_quote = SimpleNamespace(
        open=3085,
        high=3112,
        low=3070,
        close=3100,
        volume=123456,
        date=datetime.datetime(2026, 4, 3),
    )
    previous_quote = SimpleNamespace(close=3080)

    monkeypatch.setattr(
        indices,
        "StockIndex",
        SimpleNamespace(objects=lambda **kwargs: FakeIndexQuery([index])),
    )
    monkeypatch.setattr(
        indices,
        "get_latest_quote_pair",
        lambda code: (latest_quote, previous_quote),
    )

    response = client.get("/api/v1/indices/overview", headers=jwt_headers)

    assert response.status_code == 200
    payload = response.get_json()
    assert payload == {
        "data": [
            {
                "code": "000001",
                "name": "上证指数",
                "price": 3100,
                "previousClose": 3080,
                "change": 20,
                "changePct": pytest.approx(20 / 3080 * 100),
                "open": 3085,
                "high": 3112,
                "low": 3070,
                "volume": 123456,
                "tradeDate": "2026-04-03T00:00:00",
            }
        ]
    }


def test_get_indices_list_returns_quote_sorted_payload(
    client, jwt_headers, monkeypatch
):
    from app.api.v1 import indices

    all_indices = [
        SimpleNamespace(code="000001", name="上证指数"),
        SimpleNamespace(code="399001", name="深证成指"),
    ]

    snapshots = [
        {
            "code": "399001",
            "open": 100,
            "high": 110,
            "low": 95,
            "close": 108,
            "volume": 2000,
            "date": datetime.datetime(2026, 4, 3),
            "previous_close": 100,
            "change_amount": 8,
            "calculated_change_rate": 8.0,
        },
        {
            "code": "000001",
            "open": 3000,
            "high": 3110,
            "low": 2990,
            "close": 3100,
            "volume": 1000,
            "date": datetime.datetime(2026, 4, 3),
            "previous_close": 3080,
            "change_amount": 20,
            "calculated_change_rate": 20 / 3080 * 100,
        },
    ]

    def fake_stock_index_objects(**kwargs):
        if "code__in" in kwargs:
            code_set = set(kwargs["code__in"])
            return [idx for idx in all_indices if idx.code in code_set]
        return FakeIndexQuery(all_indices)

    monkeypatch.setattr(
        indices,
        "StockIndex",
        SimpleNamespace(objects=fake_stock_index_objects),
    )
    monkeypatch.setattr(
        indices,
        "get_latest_index_quote_snapshots",
        lambda *args, **kwargs: snapshots,
    )

    response = client.get(
        "/api/v1/indices?sort_by=change_rate&order=desc&page=1&page_size=2",
        headers=jwt_headers,
    )

    assert response.status_code == 200
    payload = response.get_json()
    assert payload["total"] == 2
    assert payload["page"] == 1
    assert payload["page_size"] == 2
    assert payload["items"] == [
        {
            "code": "399001",
            "name": "深证成指",
            "close": 108,
            "previousClose": 100,
            "open": 100,
            "high": 110,
            "low": 95,
            "changeRate": 8.0,
            "changeAmount": 8,
            "volume": 2000,
            "tradeDate": "2026-04-03T00:00:00",
        },
        {
            "code": "000001",
            "name": "上证指数",
            "close": 3100,
            "previousClose": 3080,
            "open": 3000,
            "high": 3110,
            "low": 2990,
            "changeRate": pytest.approx(20 / 3080 * 100),
            "changeAmount": 20,
            "volume": 1000,
            "tradeDate": "2026-04-03T00:00:00",
        },
    ]


def test_get_latest_index_quote_snapshots_sorts_by_calculated_change_rate(monkeypatch):
    from app.api.v1 import indices

    captured = {}

    def fake_aggregate(pipeline):
        captured["pipeline"] = pipeline
        return []

    monkeypatch.setattr(
        indices,
        "StockDailyQuote",
        SimpleNamespace(objects=SimpleNamespace(aggregate=fake_aggregate)),
    )

    result = indices.get_latest_index_quote_snapshots(
        index_codes={"000001", "399001"},
        sort_by="change_rate",
        order="desc",
        page=2,
        page_size=50,
    )

    assert result == []
    pipeline = captured["pipeline"]
    assert pipeline[-3] == {"$sort": {"calculated_change_rate": -1, "code": 1}}
    assert pipeline[-2] == {"$skip": 50}
    assert pipeline[-1] == {"$limit": 50}
