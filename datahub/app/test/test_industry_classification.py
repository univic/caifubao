"""Tests for the CSRC industry classification sync handler.

Baostock ``query_stock_industry`` returns rows in the order
``[updateDate, code, code_name, industry, industryClassification]`` where
``industry`` is a CSRC (证监会) string such as ``J66货币金融服务``. These tests
lock the corrected column mapping and CSRC parsing so the earlier
column-shift bug (which wrote ``updateDate`` into ``stock_code``) cannot
regress.

They also lock the canonical stock-code key contract: baostock's ``sh.600036``
must be stored as ``sh600036``, because every scoring lookup uses the canonical
form (industry-classification-code-normalization).
"""

import datetime
from types import SimpleNamespace

import pytest

from app.lib.datahub.data_integrity_keeper.handler import (
    industry_classification as handler,
)


class _FakeResultSet:
    def __init__(self, rows):
        self.rows = rows
        self.error_code = "0"
        self.error_msg = ""
        self._index = 0

    def next(self):
        return self._index < len(self.rows)

    def get_row_data(self):
        row = self.rows[self._index]
        self._index += 1
        return list(row)


def _sample_rows():
    # Real baostock format: [updateDate, code, code_name, industry, class]
    return [
        ["2026-08-31", "sh.600000", "浦发银行", "J66货币金融服务", "证监会行业分类"],
        ["2026-08-31", "sh.600001", "邯郸钢铁", "", "证监会行业分类"],
        ["2026-08-31", "sh.600004", "白云机场", "G56航空运输业", "证监会行业分类"],
    ]


def _patch_baostock(monkeypatch):
    monkeypatch.setattr("baostock.login", lambda: SimpleNamespace(error_code="0"))
    monkeypatch.setattr(
        "baostock.query_stock_industry", lambda: _FakeResultSet(_sample_rows())
    )
    monkeypatch.setattr("baostock.logout", lambda: None)


def test_parse_csrc_industry():
    assert handler._parse_csrc_industry("J66货币金融服务") == ("J66", "货币金融服务")
    assert handler._parse_csrc_industry("C36汽车制造业") == ("C36", "汽车制造业")
    assert handler._parse_csrc_industry("") == (None, None)
    assert handler._parse_csrc_industry("foo") == (None, None)  # no digit code


def test_sync_writes_code_from_baostock_code_column(monkeypatch):
    captured = []

    class FakeIndustry:
        def __init__(self, **kwargs):
            captured.append(kwargs)

        def save(self):
            pass

        @staticmethod
        def objects(**kwargs):
            return SimpleNamespace(first=lambda: None)

    monkeypatch.setattr(handler, "StockIndustryClassification", FakeIndustry)
    _patch_baostock(monkeypatch)

    result = handler.sync_industry_classification()

    assert result["status"] == "GOOD"
    assert result["new_classifications"] == 2  # sh.600001 has empty industry
    assert result["skipped"] == 1

    by_code = {row["stock_code"]: row for row in captured}
    # stock_code must come from baostock column 1, never the updateDate column 0
    assert "2026-08-31" not in by_code
    # Baostock's separated code is stored canonically, like every other table.
    assert by_code["sh600000"]["industry_code_sw_l1"] == "J66"
    assert by_code["sh600000"]["industry_name_sw_l1"] == "货币金融服务"
    assert by_code["sh600004"]["industry_code_sw_l1"] == "G56"
    assert by_code["sh600004"]["industry_name_sw_l1"] == "航空运输业"
    # CSRC has no L2 subdivision
    assert by_code["sh600000"]["industry_code_sw_l2"] is None
    assert by_code["sh600000"]["industry_name_sw_l2"] is None


def test_sync_matches_an_existing_canonical_record(monkeypatch):
    lookups = []

    class Existing:
        last_synced_at = datetime.datetime.now(datetime.UTC)

    class FakeIndustry:
        def __init__(self, **kwargs):
            raise AssertionError("sync must not create a duplicate record")

        @staticmethod
        def objects(**kwargs):
            lookups.append(kwargs.get("stock_code"))
            return SimpleNamespace(first=lambda: Existing())

    monkeypatch.setattr(handler, "StockIndustryClassification", FakeIndustry)
    _patch_baostock(monkeypatch)

    result = handler.sync_industry_classification()

    assert lookups == ["sh600000", "sh600004"]
    assert result["new_classifications"] == 0
    assert result["skipped"] == 3


def test_canonical_stock_code_normalizes_baostock_form():
    assert handler.canonical_stock_code("sh.600036") == "sh600036"
    assert handler.canonical_stock_code("  sz.000001 ") == "sz000001"
    assert handler.canonical_stock_code("bj.430047") == "bj430047"
    assert handler.canonical_stock_code("sh600036") == "sh600036"
    assert handler.canonical_stock_code(None) == ""


class _FakeQuerySet:
    def __init__(self, store, predicate):
        self._store = store
        self._predicate = predicate

    def first(self):
        for doc in self._store:
            if self._predicate(doc):
                return doc
        return None

    def __iter__(self):
        return iter([doc for doc in self._store if self._predicate(doc)])

    def update_one(self, **updates):
        doc = self.first()
        assert doc is not None, "update_one matched no document"
        for key, value in updates.items():
            assert key.startswith("set__"), f"unsupported update operator: {key}"
            setattr(doc, key[len("set__") :], value)
        return 1

    def delete(self):
        removed = [doc for doc in self._store if self._predicate(doc)]
        self._store[:] = [doc for doc in self._store if not self._predicate(doc)]
        return len(removed)


class _FakeIndustryModel:
    """In-memory stand-in for StockIndustryClassification."""

    #: Field defaults, mirroring the mongoengine document so a partially
    #: specified test row still supports plain attribute access.
    fields = {
        "stock_name": None,
        "industry_code_sw_l1": None,
        "industry_name_sw_l1": None,
        "industry_code_sw_l2": None,
        "industry_name_sw_l2": None,
        "assigned_at": None,
        "last_synced_at": None,
        "industry_change_log": [],
    }

    store = []

    def __init__(self, **fields):
        for name, default in self.fields.items():
            setattr(self, name, list(default) if isinstance(default, list) else default)
        self.__dict__.update(fields)
        type(self).store.append(self)

    @classmethod
    def reset(cls, rows=()):
        cls.store = []
        for row in rows:
            cls(**row)

    @classmethod
    def objects(cls, **filters):
        def predicate(doc):
            return all(
                getattr(doc, key, None) == value for key, value in filters.items()
            )

        return _FakeQuerySet(cls.store, predicate)


def _normalize(monkeypatch, rows, dry_run=False):
    _FakeIndustryModel.reset(rows)
    monkeypatch.setattr(handler, "StockIndustryClassification", _FakeIndustryModel)
    result = handler.normalize_stock_codes(dry_run=dry_run)
    return result, _FakeIndustryModel.store


def test_normalize_dry_run_writes_nothing(monkeypatch):
    result, store = _normalize(
        monkeypatch,
        [{"stock_code": "sh.600036", "industry_code_sw_l1": "J66"}],
        dry_run=True,
    )

    assert result["dry_run"] is True
    assert result["renamed"] == 1
    assert [doc.stock_code for doc in store] == ["sh.600036"]


def test_normalize_rename_preserves_point_in_time_fields(monkeypatch):
    assigned = datetime.datetime(2024, 3, 1, tzinfo=datetime.UTC)
    synced = datetime.datetime(2026, 8, 1, tzinfo=datetime.UTC)
    change_log = [
        {
            "timestamp": "2025-01-02T00:00:00+00:00",
            "previous_l1": "C36",
            "new_l1": "J66",
        }
    ]

    result, store = _normalize(
        monkeypatch,
        [
            {
                "stock_code": "sh.600036",
                "industry_code_sw_l1": "J66",
                "industry_name_sw_l1": "货币金融服务",
                "assigned_at": assigned,
                "last_synced_at": synced,
                "industry_change_log": list(change_log),
            }
        ],
    )

    assert result["renamed"] == 1
    assert result["merged"] == 0
    assert result["unrecognized_count"] == 0
    assert [doc.stock_code for doc in store] == ["sh600036"]
    doc = store[0]
    assert doc.assigned_at == assigned
    assert doc.last_synced_at == synced
    assert doc.industry_change_log == change_log


def test_normalize_merge_keeps_canonical_anchor_and_unions_history(monkeypatch):
    canonical_assigned = datetime.datetime(2025, 6, 1, tzinfo=datetime.UTC)
    legacy_assigned = datetime.datetime(2024, 3, 1, tzinfo=datetime.UTC)
    canonical_log = [
        {
            "timestamp": "2025-06-01T00:00:00+00:00",
            "previous_l1": "J66",
            "new_l1": "J66",
        }
    ]
    legacy_log = [
        {
            "timestamp": "2024-03-01T00:00:00+00:00",
            "previous_l1": "C36",
            "new_l1": "J66",
        }
    ]

    result, store = _normalize(
        monkeypatch,
        [
            {
                "stock_code": "sh600036",
                "industry_code_sw_l1": "J66",
                "industry_name_sw_l1": "货币金融服务",
                "assigned_at": canonical_assigned,
                "industry_change_log": list(canonical_log),
            },
            {
                "stock_code": "sh.600036",
                "industry_code_sw_l1": "C36",
                "industry_name_sw_l1": "汽车制造业",
                "assigned_at": legacy_assigned,
                "industry_change_log": list(legacy_log),
            },
        ],
    )

    assert result["merged"] == 1
    assert [doc.stock_code for doc in store] == ["sh600036"]
    doc = store[0]
    # The surviving classification keeps its own anchor: attaching the earlier
    # legacy anchor would claim it existed before it did.
    assert doc.industry_code_sw_l1 == "J66"
    assert doc.assigned_at == canonical_assigned
    # Union of both histories, oldest first, with no fabricated merge entry.
    assert doc.industry_change_log == [legacy_log[0], canonical_log[0]]


def test_normalize_merge_adopts_legacy_classification_when_canonical_has_none(
    monkeypatch,
):
    legacy_assigned = datetime.datetime(2024, 3, 1, tzinfo=datetime.UTC)

    result, store = _normalize(
        monkeypatch,
        [
            {"stock_code": "sh600036", "industry_change_log": []},
            {
                "stock_code": "sh.600036",
                "industry_code_sw_l1": "J66",
                "industry_name_sw_l1": "货币金融服务",
                "assigned_at": legacy_assigned,
                "industry_change_log": [],
            },
        ],
    )

    assert result["merged"] == 1
    assert [doc.stock_code for doc in store] == ["sh600036"]
    assert store[0].industry_code_sw_l1 == "J66"
    assert store[0].assigned_at == legacy_assigned


def test_normalize_is_idempotent(monkeypatch):
    _FakeIndustryModel.reset(
        [{"stock_code": "sh.600036", "industry_code_sw_l1": "J66"}]
    )
    monkeypatch.setattr(handler, "StockIndustryClassification", _FakeIndustryModel)

    first = handler.normalize_stock_codes()
    second = handler.normalize_stock_codes()

    assert first["renamed"] == 1
    assert second["renamed"] == 0
    assert second["merged"] == 0
    assert second["unrecognized_count"] == 0
    assert second["skipped"] == 1
    assert [doc.stock_code for doc in _FakeIndustryModel.store] == ["sh600036"]


def test_normalize_reports_unrecognized_keys_untouched(monkeypatch):
    rows = [
        {"stock_code": "600036", "industry_code_sw_l1": "J66"},
        {"stock_code": "SH600036", "industry_code_sw_l1": "J66"},
        {"stock_code": "sh.60036", "industry_code_sw_l1": "J66"},
    ]

    result, store = _normalize(monkeypatch, rows)

    assert result["renamed"] == 0
    assert result["merged"] == 0
    assert result["unrecognized_count"] == 3
    assert sorted(result["unrecognized"]) == ["600036", "SH600036", "sh.60036"]
    assert sorted(doc.stock_code for doc in store) == [
        "600036",
        "SH600036",
        "sh.60036",
    ]


class _FakeQuery(list):
    """Iterable queryset stub that also supports ``.first()``."""

    def first(self):
        return self[0] if self else None


class _LookupModel:
    def __init__(self, rows):
        self._rows = rows

    def objects(self, **filters):
        def matches(row):
            for key, value in filters.items():
                if key.endswith("__in"):
                    if getattr(row, key[: -len("__in")], None) not in value:
                        return False
                elif getattr(row, key, None) != value:
                    return False
            return True

        return _FakeQuery(row for row in self._rows if matches(row))


class _MetricsModel:
    def __init__(self, row):
        self._row = row

    def objects(self, **filters):
        return SimpleNamespace(
            order_by=lambda *args: SimpleNamespace(first=lambda: self._row)
        )


class _AggregateMetricsModel:
    created = []

    def __init__(self, **fields):
        self.__dict__.update(fields)
        type(self).created.append(self)

    def save(self):
        pass

    @classmethod
    def objects(cls, **filters):
        return SimpleNamespace(first=lambda: None)


def _industry_row(**overrides):
    fields = {
        "stock_code": "sh600036",
        "industry_code_sw_l1": "J66",
        "industry_name_sw_l1": "货币金融服务",
        "assigned_at": datetime.datetime(2020, 1, 2, tzinfo=datetime.UTC),
        "industry_change_log": [],
    }
    fields.update(overrides)
    return SimpleNamespace(**fields)


def _metrics_row():
    return SimpleNamespace(stock_count=10, avg_score=70.0, buy_count=3, watch_count=4)


def test_industry_momentum_uses_canonical_classification(monkeypatch):
    from app.lib.scoring_engine import components

    monkeypatch.setattr(
        components, "StockIndustryClassification", _LookupModel([_industry_row()])
    )
    monkeypatch.setattr(
        components, "IndustryDailyMetrics", _MetricsModel(_metrics_row())
    )

    result = components.industry_momentum_component(
        "sh600036", datetime.datetime(2026, 9, 15), 20, 5.0
    )

    assert result["normalized_value"] == pytest.approx(0.7)
    assert result["evidence"]["industry_code"] == "J66"


def test_industry_momentum_rejects_a_later_classification(monkeypatch):
    from app.lib.scoring_engine import components

    later = _industry_row(
        assigned_at=datetime.datetime(2026, 9, 20, tzinfo=datetime.UTC)
    )
    monkeypatch.setattr(
        components, "StockIndustryClassification", _LookupModel([later])
    )
    monkeypatch.setattr(
        components, "IndustryDailyMetrics", _MetricsModel(_metrics_row())
    )

    result = components.industry_momentum_component(
        "sh600036", datetime.datetime(2026, 9, 15), 20, 5.0
    )

    assert result["normalized_value"] == pytest.approx(0.5)
    assert result["raw_value"] is None


def test_industry_momentum_rejects_a_change_logged_after_the_date(monkeypatch):
    from app.lib.scoring_engine import components

    changed = _industry_row(
        industry_change_log=[
            {
                "timestamp": "2026-10-01T00:00:00+00:00",
                "previous_l1": "J66",
                "new_l1": "C36",
            }
        ]
    )
    monkeypatch.setattr(
        components, "StockIndustryClassification", _LookupModel([changed])
    )
    monkeypatch.setattr(
        components, "IndustryDailyMetrics", _MetricsModel(_metrics_row())
    )

    result = components.industry_momentum_component(
        "sh600036", datetime.datetime(2026, 9, 15), 20, 5.0
    )

    assert result["normalized_value"] == pytest.approx(0.5)


def test_industry_momentum_guards_a_prefetched_lookup(monkeypatch):
    from app.lib.scoring_engine import components

    later = _industry_row(
        assigned_at=datetime.datetime(2026, 9, 20, tzinfo=datetime.UTC)
    )

    result = components.industry_momentum_component(
        "sh600036",
        datetime.datetime(2026, 9, 15),
        20,
        5.0,
        industry_lookup={"sh600036": (later, _metrics_row())},
    )

    assert result["normalized_value"] == pytest.approx(0.5)


def test_aggregate_industry_metrics_uses_in_effect_classification(monkeypatch):
    from app.lib.scoring_engine import components

    _AggregateMetricsModel.created = []
    monkeypatch.setattr(
        components, "StockIndustryClassification", _LookupModel([_industry_row()])
    )
    monkeypatch.setattr(components, "IndustryDailyMetrics", _AggregateMetricsModel)
    predictions = [
        SimpleNamespace(
            stock_code="sh600036",
            horizon=20,
            score=70.0,
            recommendation="BUY",
            percentile=0.9,
            rank=1,
        )
    ]

    result = components.aggregate_industry_metrics(
        datetime.datetime(2026, 9, 15), predictions, "score_v2_202605b"
    )

    assert len(result) == 1
    assert result[0].industry_code == "J66"
    assert result[0].stock_count == 1


def test_aggregate_industry_metrics_skips_a_later_classification(monkeypatch):
    from app.lib.scoring_engine import components

    _AggregateMetricsModel.created = []
    later = _industry_row(
        assigned_at=datetime.datetime(2026, 9, 20, tzinfo=datetime.UTC)
    )
    monkeypatch.setattr(
        components, "StockIndustryClassification", _LookupModel([later])
    )
    monkeypatch.setattr(components, "IndustryDailyMetrics", _AggregateMetricsModel)
    predictions = [
        SimpleNamespace(
            stock_code="sh600036",
            horizon=20,
            score=70.0,
            recommendation="BUY",
            percentile=0.9,
            rank=1,
        )
    ]

    result = components.aggregate_industry_metrics(
        datetime.datetime(2026, 9, 15), predictions, "score_v2_202605b"
    )

    assert result == []
    assert _AggregateMetricsModel.created == []


def test_day_prefetch_drops_classifications_not_yet_in_effect():
    from app.lib.scoring_engine.scoring_service import _DayPrefetch

    class Query:
        def __init__(self, rows):
            self._rows = rows

        def only(self, *fields):
            return self

        def as_pymongo(self):
            return list(self._rows)

    class Model:
        def __init__(self, rows):
            self.rows = rows

        def objects(self, **filters):
            return Query(self.rows)

    class Service:
        model_version = "score_v2_202605b"

        def __init__(self, classifications):
            self.industry_model = Model(classifications)
            self.industry_metrics_model = Model([])

        def _get_horizon_config(self, horizon):
            return {
                "minimum_quote_count": 1,
                "breakout_lookback": 1,
                "risk_lookback": 1,
                "signal_decay_max_days": 1,
            }

        def _history_window_start(self, date, count):
            return date

        def _field_skeleton(self, model):
            return {}

    service = Service(
        [
            {
                "stock_code": "sh600036",
                "industry_code_sw_l1": "J66",
                "industry_name_sw_l1": "货币金融服务",
                "assigned_at": datetime.datetime(2020, 1, 1, tzinfo=datetime.UTC),
                "industry_change_log": [],
            },
            {
                "stock_code": "sh600519",
                "industry_code_sw_l1": "C15",
                "industry_name_sw_l1": "酒、饮料和精制茶制造业",
                "assigned_at": datetime.datetime(2026, 9, 20, tzinfo=datetime.UTC),
                "industry_change_log": [],
            },
        ]
    )

    lookup = _DayPrefetch(
        service,
        datetime.datetime(2026, 9, 15, tzinfo=datetime.UTC),
        ["sh600036", "sh600519"],
        [20],
    ).industry_lookup(20)

    # The per-day production prefetch must drop a classification that was not
    # yet in effect, or a replayed date would inherit it.
    assert set(lookup) == {"sh600036"}
