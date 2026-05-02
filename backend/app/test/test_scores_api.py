import datetime
from types import SimpleNamespace


class FakeScoreQuery:
    def __init__(self, rows):
        self.rows = list(rows)

    def filter(self, **kwargs):
        return FakeScoreQuery([row for row in self.rows if _matches(row, kwargs)])

    def order_by(self, *fields):
        rows = self.rows
        for field in reversed(fields):
            reverse = field.startswith("-")
            name = field.removeprefix("-")
            rows = sorted(
                rows,
                key=lambda row: getattr(row, name, None) or 0,
                reverse=reverse,
            )
        return FakeScoreQuery(rows)

    def only(self, *_fields):
        return self

    def first(self):
        return self.rows[0] if self.rows else None

    def count(self):
        return len(self.rows)

    def skip(self, offset):
        return FakeScoreQuery(self.rows[offset:])

    def limit(self, limit):
        return FakeScoreQuery(self.rows[:limit])

    def __iter__(self):
        return iter(self.rows)


def _matches(row, kwargs):
    for key, value in kwargs.items():
        if key.endswith("__gte"):
            field = key.removesuffix("__gte")
            if getattr(row, field) < value:
                return False
        elif key.endswith("__lte"):
            field = key.removesuffix("__lte")
            if getattr(row, field) > value:
                return False
        else:
            if getattr(row, key) != value:
                return False
    return True


def _score(**overrides):
    defaults = {
        "stock_code": "sh600000",
        "stock_name": "浦发银行",
        "date": datetime.datetime(2026, 4, 13),
        "horizon": 5,
        "score": 76.5,
        "rank": 1,
        "percentile": 1.0,
        "recommendation": "BUY",
        "base_price": 10.0,
        "target_date": datetime.datetime(2026, 4, 20),
        "status": "TRACKING",
        "verification": {"verified_quote_count": 2},
        "explanation": {
            "summary": "Bullish signal strength",
            "components": [{"id": "signal_strength", "contribution": 30}],
        },
        "input_snapshot": {"status": "READY"},
        "model_version": "score_v2_202604",
        "generated_at": datetime.datetime(2026, 4, 13, 16),
        "updated_at": datetime.datetime(2026, 4, 13, 17),
    }
    defaults.update(overrides)
    return SimpleNamespace(**defaults)


def test_list_scores_defaults_to_latest_horizon_date(client, monkeypatch):
    from app.api.v1 import scores

    rows = [
        _score(stock_code="sh600000", score=76.5, date=datetime.datetime(2026, 4, 13)),
        _score(stock_code="sz000001", score=66.0, date=datetime.datetime(2026, 4, 13)),
        _score(stock_code="sh600519", score=88.0, date=datetime.datetime(2026, 4, 12)),
        _score(stock_code="sh000001", horizon=20, score=99.0),
    ]

    monkeypatch.setattr(
        scores,
        "StockScorePrediction",
        SimpleNamespace(objects=lambda **kwargs: FakeScoreQuery(rows).filter(**kwargs)),
    )

    response = client.get("/api/scores?horizon=5&limit=10")

    assert response.status_code == 200
    payload = response.get_json()
    assert payload["date"] == "2026-04-13T00:00:00"
    assert payload["horizon"] == 5
    assert payload["total"] == 2
    assert [item["stock_code"] for item in payload["items"]] == [
        "sh600000",
        "sz000001",
    ]
    assert payload["items"][0]["verification"] == {"verified_quote_count": 2}


def test_get_score_explanation_returns_details(client, monkeypatch):
    from app.api.v1 import scores

    rows = [_score()]
    monkeypatch.setattr(
        scores,
        "StockScorePrediction",
        SimpleNamespace(objects=lambda **kwargs: FakeScoreQuery(rows).filter(**kwargs)),
    )

    response = client.get("/api/scores/sh600000/2026-04-13/explanation?horizon=5")

    assert response.status_code == 200
    payload = response.get_json()
    assert payload["stock_code"] == "sh600000"
    assert payload["explanation"]["components"][0]["id"] == "signal_strength"
    assert payload["input_snapshot"] == {"status": "READY"}


def test_list_scores_rejects_unsupported_horizon(client):
    response = client.get("/api/scores?horizon=10")

    assert response.status_code == 400
    assert response.get_json()["message"] == "Unsupported horizon"
