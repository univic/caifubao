from types import SimpleNamespace


class FakeQuery:
    def __init__(self, rows):
        self.rows = list(rows)

    def only(self, *_fields):
        return self

    def order_by(self, *_fields):
        return self

    def first(self):
        return self.rows[0] if self.rows else None

    def __iter__(self):
        return iter(self.rows)


def test_market_comprehensive_uses_stock_score_predictions(client, monkeypatch):
    from app.api.v1 import market

    stock = SimpleNamespace(code="sh600000", name="浦发银行")
    quote = SimpleNamespace(
        code="sh600000",
        open=10.0,
        high=10.5,
        low=9.8,
        close=10.2,
        volume=10000,
        change_rate=2.0,
    )
    score5 = SimpleNamespace(
        stock_code="sh600000",
        horizon=5,
        score=76.5,
        rank=3,
        percentile=0.98,
        recommendation="BUY",
        status="TRACKING",
        verification={"verified_quote_count": 2},
        model_version="score_v2_202604",
    )
    score20 = SimpleNamespace(
        stock_code="sh600000",
        horizon=20,
        score=66.0,
        rank=12,
        percentile=0.9,
        recommendation="WATCH",
        status="PENDING",
        verification={},
        model_version="score_v2_202604",
    )

    monkeypatch.setattr(
        market,
        "IndividualStock",
        SimpleNamespace(objects=lambda **_kwargs: FakeQuery([stock])),
    )
    monkeypatch.setattr(
        market,
        "StockDailyQuote",
        SimpleNamespace(objects=lambda **_kwargs: FakeQuery([quote])),
    )
    monkeypatch.setattr(
        market,
        "StockScorePrediction",
        SimpleNamespace(objects=lambda **_kwargs: FakeQuery([score5, score20])),
    )

    response = client.get(
        "/api/market/comprehensive?type=stock&date=2026-04-13&horizon=20"
    )

    assert response.status_code == 200
    payload = response.get_json()
    item = payload["items"][0]
    assert item["evaluation"]["primary_horizon"] == 20
    assert item["evaluation"]["score"] == 66.0
    assert item["evaluation"]["rank"] == 12
    assert item["evaluation"]["display_rank"] == 1
    assert item["evaluation"]["scores"]["5"]["score"] == 76.5
    assert item["evaluation"]["scores"]["60"]["recommendation"] == "NONE"
