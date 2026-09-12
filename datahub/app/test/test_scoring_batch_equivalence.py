# -*- coding: utf-8 -*-
"""Perf C1/3.5 equivalence harness: batched per-day scoring vs per-stock scoring.

For one market-wide evaluation day the batched path
(``batch_prefetch=True``, the production default) and the legacy per-stock
path (``False``) must persist byte-identical business fields for every active
stock and horizon: score, rank, percentile, recommendation, base_price,
target_date, status, explanation, verification and input_snapshot.

The seeded market deliberately covers every read the batch path replaces:

* a normal stock (today quote + factor + two bullish signals),
* a stock with no factor and only a decayed bullish signal,
* a thin industry whose newest metrics row must take the neutral branch,
* a stock with no industry classification at all,
* an ST/suspended stock (risk penalty surcharge),
* a sparse-history stock whose day window is too small *and* whose history
  reaches before the window, forcing the exact per-code history fallback and
  its own CSI300 range (the case where a window-only index read would silently
  drop the evaluation date to the self-proxy fallback),
* a stock with a bullish signal outside the h5/h20 decay but inside the h60
  window (the day-prefetched decay window must stay equivalent),
* a stock with no quote on the evaluation day (BLOCKED row),
* CSI300 index quotes (real relative strength reads index data; the harness
  asserts the alpha branch is actually taken),
* pre-existing predictions for the partial-repair runs (raw PENDING and ranked
  BLOCKED rows).

A negative control proves the harness is not vacuous: perturbing the batch
path's score must make the diff non-empty (C1's rollback gate). Prefetch
failures (CSI300, industry) are also exercised: the batch path must fall back
to the component's own reads rather than silently scoring differently.
"""

import datetime

import pytest
from unittest.mock import MagicMock, patch

import app.lib.scoring_engine.components as components_module
from app.lib.scoring_engine.config import DEFAULT_MODEL_VERSION
from app.lib.scoring_engine.scoring_service import (
    StockScoringService,
    _Row,
    _row_from_doc,
    batch_scoring_default,
)
from app.test.test_scoring_service import (
    FakeFactor,
    FakeModel,
    FakePrediction,
    FakeQuerySet,
    FakeQuote,
    FakeSignal,
    FakeStock,
    matches_query,
)


class FakeIndustryClassification(FakeModel):
    pass


class FakeIndustryMetrics(FakeModel):
    pass


def _weekdays(start, end):
    day = start
    while day <= end:
        if day.weekday() < 5:
            yield day
        day += datetime.timedelta(days=1)


EVAL_DATE = datetime.datetime(2026, 4, 10, tzinfo=datetime.UTC)
_HISTORY_START = EVAL_DATE - datetime.timedelta(days=260)
TRADE_DAYS = [day for day in _weekdays(_HISTORY_START, EVAL_DATE)]
CALENDAR = [
    day for day in _weekdays(_HISTORY_START, EVAL_DATE + datetime.timedelta(days=60))
]

SNAPSHOT_FIELDS = (
    "stock_code",
    "stock_name",
    "date",
    "horizon",
    "score",
    "rank",
    "percentile",
    "recommendation",
    "base_price",
    "target_date",
    "status",
    "explanation",
    "verification",
    "input_snapshot",
    "model_version",
)

# code -> (name, industry_code, industry_name, close_hfq, is_st, sparse)
_STOCKS = {
    "sh600000": ("浦发银行", "801010", "银行", 9.5, False, False),
    "sh600001": ("平安银行", "801010", "银行", None, False, False),
    "sh600002": ("ST样本", "801010", "银行", 4.2, True, False),
    "sh600003": ("医药样本", "801020", "医药", None, False, True),
    "sh600004": ("停牌样本", None, None, None, False, False),
    "sh600005": ("医药二号", "801020", "医药", 7.7, False, False),
}

#: Sparse stock shape: a few in-window quotes (fewer than any horizon's
#: history limit, so the per-code fallback always runs) plus older quotes that
#: sit before the prefetched window.
_SPARSE_RECENT_DAYS = 12
_SPARSE_OLD_DAYS = 15


def seed_market():
    """Seed a full synthetic market (quotes/factors/signals/index/industry)."""
    for index, (code, meta) in enumerate(_STOCKS.items()):
        name, industry_code, industry_name, close_hfq, is_st, sparse = meta
        FakeStock.records.append(
            FakeStock(code=code, name=name, active_status=0, id=f"oid-{code}")
        )
        if industry_code:
            FakeIndustryClassification.records.append(
                FakeIndustryClassification(
                    stock_code=code,
                    industry_code_sw_l1=industry_code,
                    industry_name_sw_l1=industry_name,
                )
            )

        if sparse:
            # Sparse/suspended name: too few in-window quotes (forces the exact
            # per-code history fallback), and older quotes *before* the day
            # window so the CSI300 range must follow the fallback, not the
            # window, to keep real_relative_strength equivalent.
            history_days = (
                TRADE_DAYS[20 : 20 + _SPARSE_OLD_DAYS]
                + TRADE_DAYS[-_SPARSE_RECENT_DAYS:]
            )
        else:
            history_days = TRADE_DAYS
        for offset, day in enumerate(history_days):
            if code == "sh600004" and day == EVAL_DATE:
                continue
            quote = FakeQuote(
                code=code,
                date=day,
                close=10.0 + offset * 0.01 + index,
                high=10.2 + offset * 0.01 + index,
                low=9.8 + offset * 0.01 + index,
                # mongoengine documents always carry the hfq fields (None when
                # unavailable), and technical_factors._closing_price reads them
                # directly; set them so the CSI300 alpha path is reachable.
                close_hfq=None if close_hfq is None else close_hfq + offset * 0.01,
                high_hfq=None if close_hfq is None else close_hfq + 0.2 + offset * 0.01,
                low_hfq=None if close_hfq is None else close_hfq - 0.2 + offset * 0.01,
                trade_status=0 if is_st and day == EVAL_DATE else 1,
                isST=1 if is_st else 0,
            )
            FakeQuote.records.append(quote)

        if code not in ("sh600001", "sh600004"):
            FakeFactor.records.append(
                FakeFactor(
                    stock_code=code,
                    date=EVAL_DATE,
                    ma_20=9.5,
                    ma_60=9.0,
                    ma_120=8.5,
                )
            )

    def bullish(code, day, strength):
        FakeSignal.records.append(
            FakeSignal(
                stock_code=code,
                date=day,
                signal_name="MA10_CROSS_MA20",
                direction="BULLISH",
                strength=strength,
                reason="MA10 crosses MA20",
            )
        )

    bullish("sh600000", EVAL_DATE, 1.0)
    bullish("sh600000", EVAL_DATE, 0.6)
    bullish("sh600001", EVAL_DATE - datetime.timedelta(days=2), 0.8)
    # Inside the h60 decay window (20 days), outside the h5/h20 windows.
    bullish("sh600005", EVAL_DATE - datetime.timedelta(days=15), 0.9)
    # Older than every decay window: must stay invisible to both paths.
    bullish("sh600005", EVAL_DATE - datetime.timedelta(days=40), 0.3)

    # CSI300 must cover every seeded stock date, including the sparse name's
    # pre-window history: the per-stock path queried the index over each
    # stock's own [min(quote date), evaluation date] range.
    index_start = min(
        quote.date for quote in FakeQuote.records if quote.code != "sh000300"
    )
    for offset, day in enumerate(_weekdays(index_start, EVAL_DATE)):
        FakeQuote.records.append(
            FakeQuote(
                code="sh000300",
                date=day,
                close=3000.0 + offset * 2.0,
                high=3010.0 + offset * 2.0,
                low=2990.0 + offset * 2.0,
                close_hfq=3000.0 + offset * 2.0,
                high_hfq=3010.0 + offset * 2.0,
                low_hfq=2990.0 + offset * 2.0,
            )
        )

    # Industry metrics: the newest row wins; 801020 stays under the >=3 floor.
    for day, avg_score in (
        (EVAL_DATE - datetime.timedelta(days=1), 40.0),
        (EVAL_DATE - datetime.timedelta(days=3), 62.5),
    ):
        for horizon in (5, 20, 60):
            FakeIndustryMetrics.records.append(
                FakeIndustryMetrics(
                    industry_code="801010",
                    industry_name="银行",
                    date=day,
                    horizon=horizon,
                    model_version=DEFAULT_MODEL_VERSION,
                    stock_count=5,
                    avg_score=avg_score,
                    buy_count=1,
                    watch_count=2,
                )
            )
            FakeIndustryMetrics.records.append(
                FakeIndustryMetrics(
                    industry_code="801020",
                    industry_name="医药",
                    date=day,
                    horizon=horizon,
                    model_version=DEFAULT_MODEL_VERSION,
                    stock_count=2,
                    avg_score=70.0,
                    buy_count=0,
                    watch_count=1,
                )
            )


@pytest.fixture
def calendar():
    return list(CALENDAR)


@pytest.fixture
def batch_harness(calendar):
    for model in (
        FakeStock,
        FakeQuote,
        FakeFactor,
        FakeSignal,
        FakePrediction,
        FakeIndustryClassification,
        FakeIndustryMetrics,
    ):
        model.records = []
    FakePrediction.bulk_calls = []
    FakePrediction.fail_bulk = False
    FakePrediction.fail_rank_bulk = False

    def fake_index_objects(cls, **query):
        # The legacy component imports the real StockDailyQuote; point it at
        # the fake records so both paths read the same CSI300 data.
        return FakeQuerySet(
            [record for record in FakeQuote.records if matches_query(record, query)]
        )

    with (
        patch(
            "app.lib.scoring_engine.scoring_service.FinanceMarket.objects"
        ) as mock_market_objs,
        patch.object(
            components_module,
            "StockIndustryClassification",
            FakeIndustryClassification,
        ),
        patch.object(components_module, "IndustryDailyMetrics", FakeIndustryMetrics),
        patch(
            "app.model.stock.StockDailyQuote.objects",
            classmethod(fake_index_objects),
        ),
        patch(
            "app.lib.scoring_engine.scoring_service.aggregate_industry_metrics",
            return_value=[],
        ),
    ):
        mock_market = MagicMock()
        mock_market.trade_calendar = calendar
        mock_market_objs.return_value.first.return_value = mock_market
        service = StockScoringService(
            stock_model=FakeStock,
            quote_model=FakeQuote,
            factor_model=FakeFactor,
            signal_model=FakeSignal,
            prediction_model=FakePrediction,
            industry_model=FakeIndustryClassification,
            industry_metrics_model=FakeIndustryMetrics,
        )
        service.calendar = calendar
        yield service


def _normalize(value):
    if isinstance(value, datetime.datetime):
        return value.isoformat()
    if isinstance(value, dict):
        return {key: _normalize(item) for key, item in sorted(value.items())}
    if isinstance(value, (list, tuple)):
        return [_normalize(item) for item in value]
    return value


def _snapshot(records):
    snapshot = {}
    for record in records:
        entry = {
            field: _normalize(getattr(record, field, None)) for field in SNAPSHOT_FIELDS
        }
        # The batch upsert must encode the ReferenceField as the stock's _id,
        # exactly as mongoengine's save does; normalise both shapes to the id.
        stock = getattr(record, "stock", None)
        entry["stock"] = _normalize(getattr(stock, "id", stock))
        snapshot[(record.stock_code, record.horizon)] = entry
    return snapshot


def _diff(legacy, batch):
    diffs = []
    for key in sorted(set(legacy) | set(batch)):
        if key not in legacy:
            diffs.append(f"{key}: missing from the per-stock path")
            continue
        if key not in batch:
            diffs.append(f"{key}: missing from the batch path")
            continue
        for field, expected in legacy[key].items():
            actual = batch[key][field]
            if actual != expected:
                diffs.append(f"{key} {field}: per-stock={expected!r} batch={actual!r}")
    return diffs


def _assert_equivalent(legacy, batch):
    assert legacy, "harness produced no per-stock rows"
    assert batch, "harness produced no batch rows"
    diffs = _diff(legacy, batch)
    assert not diffs, "batch path diverged from the per-stock path:\n" + "\n".join(
        diffs[:20]
    )


def _run(service, *, batch, ranked=False, horizon=None, replace=False, existing=None):
    FakePrediction.records = []
    FakePrediction.bulk_calls = []
    if existing is not None:
        FakePrediction.records.extend(existing())
    service.batch_prefetch = batch
    if ranked:
        service.score_all_stocks_ranked(
            date=EVAL_DATE, horizon=horizon, replace=replace
        )
    else:
        service.score_all_stocks(date=EVAL_DATE, horizon=horizon, replace=replace)
    return _snapshot(FakePrediction.records)


def _two_runs(service, **kwargs):
    return _run(service, batch=False, **kwargs), _run(service, batch=True, **kwargs)


def test_raw_all_horizons_batch_matches_per_stock(batch_harness):
    seed_market()

    legacy, batch = _two_runs(batch_harness)

    _assert_equivalent(legacy, batch)
    assert len(batch) == len(_STOCKS) * 3
    # Non-vacuity: real components, real ranks and hybrid recommendations.
    scored = [entry for entry in batch.values() if entry["status"] != "BLOCKED"]
    blocked = [entry for entry in batch.values() if entry["status"] == "BLOCKED"]
    assert len(scored) == (len(_STOCKS) - 1) * 3
    assert all(
        entry["rank"] is not None and entry["percentile"] is not None
        for entry in scored
    )
    assert blocked and all(entry["score"] == 0.0 for entry in blocked)
    assert all(entry["explanation"]["components"] for entry in scored)
    assert any(entry["explanation"]["penalties"] for entry in scored)


def test_raw_single_horizon_batch_matches_per_stock(batch_harness):
    seed_market()

    legacy, batch = _two_runs(batch_harness, horizon=60)

    _assert_equivalent(legacy, batch)
    assert {key[1] for key in batch} == {60}


def test_ranked_batch_matches_per_stock(batch_harness):
    seed_market()

    legacy, batch = _two_runs(batch_harness, ranked=True, horizon=20)

    _assert_equivalent(legacy, batch)
    assert {key[1] for key in batch} == {20}
    assert all(
        entry["input_snapshot"].get("scoring_mode") == "ranked"
        for entry in batch.values()
        if entry["status"] != "BLOCKED"
    )


def test_ranked_flipped_directions_batch_matches_per_stock(batch_harness):
    """A construction-layer flip keeps scores signed/sortable; the batch path
    must reproduce it exactly."""
    seed_market()
    service = StockScoringService(
        stock_model=FakeStock,
        quote_model=FakeQuote,
        factor_model=FakeFactor,
        signal_model=FakeSignal,
        prediction_model=FakePrediction,
        industry_model=FakeIndustryClassification,
        industry_metrics_model=FakeIndustryMetrics,
        scoring_config={"20": {"directions": {"momentum": -1, "trend_alignment": -1}}},
    )
    service.calendar = list(CALENDAR)

    legacy = _run(service, batch=False, ranked=True, horizon=20)
    batch = _run(service, batch=True, ranked=True, horizon=20)

    _assert_equivalent(legacy, batch)
    scores = [
        entry["score"] for entry in batch.values() if entry["status"] != "BLOCKED"
    ]
    assert len(set(scores)) > 1
    assert all(
        entry["explanation"]["components"]
        for entry in batch.values()
        if entry["status"] != "BLOCKED"
    )


def test_partial_replace_false_repair_batch_matches_per_stock(batch_harness):
    seed_market()

    def seed_existing():
        # A stale PENDING row (wrong rank/recommendation) plus a stored BLOCKED
        # row that the replace=False path keeps and repairs.
        return [
            FakePrediction(
                stock_code="sh600000",
                stock_name="浦发银行",
                date=EVAL_DATE,
                horizon=5,
                model_version=DEFAULT_MODEL_VERSION,
                status="PENDING",
                score=11.0,
                rank=99,
                percentile=0.01,
                recommendation="AVOID",
                input_snapshot={"scoring_mode": "raw"},
            ),
            FakePrediction(
                stock_code="sh600001",
                stock_name="平安银行",
                date=EVAL_DATE,
                horizon=5,
                model_version=DEFAULT_MODEL_VERSION,
                status="BLOCKED",
                score=0.0,
                recommendation="NONE",
                input_snapshot={
                    "status": "BLOCKED",
                    "scoring_mode": "raw",
                    "blocked_reason": "missing_quote",
                },
            ),
        ]

    legacy, batch = _two_runs(batch_harness, horizon=5, existing=seed_existing)

    _assert_equivalent(legacy, batch)
    assert {key[0] for key in batch} == set(_STOCKS)
    assert batch[("sh600000", 5)]["score"] == 11.0  # retained, not re-scored
    assert batch[("sh600001", 5)]["status"] == "BLOCKED"


def test_dry_run_reports_match_and_write_nothing(batch_harness):
    seed_market()

    def run(batch):
        FakePrediction.records = []
        batch_harness.batch_prefetch = batch
        return batch_harness.score_all_stocks(date=EVAL_DATE, horizon=5, dry_run=True)

    legacy_report = run(False)
    batch_report = run(True)

    assert legacy_report == batch_report
    assert legacy_report["scored_count"] == len(_STOCKS)
    assert FakePrediction.records == []


def test_harness_detects_a_perturbed_batch_score(batch_harness, monkeypatch):
    """Negative control: the diff gate must fail on a real divergence."""
    seed_market()

    legacy = _run(batch_harness, batch=False, horizon=5)

    original = StockScoringService._calculate_score

    def perturbed(self, components, penalties):
        return round(original(self, components, penalties) + 0.01, 2)

    monkeypatch.setattr(StockScoringService, "_calculate_score", perturbed)
    batch = _run(batch_harness, batch=True, horizon=5)

    assert _diff(legacy, batch), "harness failed to detect a perturbed batch score"


def test_field_skeleton_matches_mongoengine_load_defaults():
    """Batch rows must reproduce mongoengine's load-time field defaults."""
    from app.model.stock import StockDailyQuote

    skeleton = StockScoringService._field_skeleton(StockDailyQuote)
    assert skeleton["close_hfq"] is None
    assert skeleton["trade_status"] is None

    document = StockDailyQuote(code="sh600000", date=EVAL_DATE, close=9.0)
    row = _row_from_doc({"code": "sh600000", "date": EVAL_DATE, "close": 9.0}, skeleton)

    for field in ("close_hfq", "trade_status", "isST"):
        assert getattr(row, field) == getattr(document, field)
    assert row.close == 9.0
    with pytest.raises(AttributeError):
        row.not_a_field  # unknown attributes behave like a Document


def test_batch_prefetch_hands_components_dict_rows(batch_harness):
    """Non-vacuity: the prefetch must hand components raw _Row views (no
    Document hydration), or the equivalence run would not exercise C1."""
    seed_market()
    build = StockScoringService._build_raw_prediction_payload
    seen = {}

    def spy(self, stock, date, horizon, config, prefetch=None):
        quote = prefetch.quote(stock.code)
        if quote is not None:
            seen[stock.code] = type(quote)
        return build(self, stock, date, horizon, config, prefetch)

    batch_harness.batch_prefetch = True
    with patch.object(StockScoringService, "_build_raw_prediction_payload", spy):
        batch_harness.score_all_stocks(date=EVAL_DATE, horizon=5)

    assert seen, "no prefetched quotes observed"
    assert all(issubclass(kind, _Row) for kind in seen.values())


def test_history_window_is_projected_and_date_descending(batch_harness):
    """The window keeps only the whitelisted history fields (memory bound) and
    hands them to components newest-first, exactly like the per-code query."""
    import app.lib.scoring_engine.scoring_service as scoring_module

    seed_market()
    prefetch = scoring_module._DayPrefetch(
        batch_harness, EVAL_DATE, list(_STOCKS), [5, 20, 60]
    )

    history = prefetch.history("sh600000", 20)
    assert len(history) == 20
    assert set(history[0]._data) == set(scoring_module._HISTORY_QUOTE_FIELDS)
    dates = [row.date for row in history]
    assert dates == sorted(dates, reverse=True)
    # The evaluation-day quote keeps its full document (risk penalty needs
    # trade_status/isST), so the projection must not leak into the day read.
    assert prefetch.quote("sh600002").trade_status == 0
    assert prefetch.quote("sh600002").isST == 1


def test_kill_switch_env_controls_batch_path(monkeypatch, batch_harness):
    monkeypatch.setenv("DATAHUB_SCORING_BATCH", "0")
    assert batch_scoring_default() is False
    monkeypatch.setenv("DATAHUB_SCORING_BATCH", "off")
    assert batch_scoring_default() is False
    monkeypatch.setenv("DATAHUB_SCORING_BATCH", "1")
    assert batch_scoring_default() is True
    monkeypatch.delenv("DATAHUB_SCORING_BATCH", raising=False)
    assert batch_scoring_default() is True

    # The constructor must honour the env unless it is overridden explicitly.
    monkeypatch.setenv("DATAHUB_SCORING_BATCH", "0")
    service = StockScoringService(
        stock_model=FakeStock,
        quote_model=FakeQuote,
        factor_model=FakeFactor,
        signal_model=FakeSignal,
        prediction_model=FakePrediction,
        industry_model=FakeIndustryClassification,
        industry_metrics_model=FakeIndustryMetrics,
    )
    assert service.batch_prefetch is False
    monkeypatch.setenv("DATAHUB_SCORING_BATCH", "1")
    service = StockScoringService(
        stock_model=FakeStock,
        quote_model=FakeQuote,
        factor_model=FakeFactor,
        signal_model=FakeSignal,
        prediction_model=FakePrediction,
        industry_model=FakeIndustryClassification,
        industry_metrics_model=FakeIndustryMetrics,
    )
    assert service.batch_prefetch is True


def _seed_normal_stock(code, index):
    """A plain full-history stock: no industry, bullish signal on the day."""
    FakeStock.records.append(
        FakeStock(code=code, name=f"Extra {code}", active_status=0, id=f"oid-{code}")
    )
    for offset, day in enumerate(TRADE_DAYS):
        FakeQuote.records.append(
            FakeQuote(
                code=code,
                date=day,
                close=11.0 + index + offset * 0.01,
                high=11.2 + index + offset * 0.01,
                low=10.8 + index + offset * 0.01,
                trade_status=1,
                isST=0,
            )
        )
    FakeFactor.records.append(
        FakeFactor(stock_code=code, date=EVAL_DATE, ma_20=10.5, ma_60=10.0, ma_120=9.5)
    )
    FakeSignal.records.append(
        FakeSignal(
            stock_code=code,
            date=EVAL_DATE,
            signal_name="MA10_CROSS_MA20",
            direction="BULLISH",
            strength=1.0,
            reason="MA10 crosses MA20",
        )
    )


def _count_model_queries(monkeypatch, counter):
    """Count every model read, including the two component-level caches.

    ``FakeQuote/Factor/Signal/Prediction`` cover the service-level reads. The
    C1/3.4 per-day caches are consumed *inside components* (industry
    classification + L1 metrics through the patched component models, CSI300
    through the real ``StockDailyQuote`` the legacy component imports), so a
    regression that dropped ``industry_lookup=``/``index_quotes=`` wiring would
    leave the other tests green while those reads became O(cohort). Count them
    too.
    """
    for model in (
        FakeQuote,
        FakeFactor,
        FakeSignal,
        FakePrediction,
        FakeIndustryClassification,
        FakeIndustryMetrics,
    ):
        original = model.objects.__func__

        def counting(cls, *args, _original=original, _name=model.__name__, **query):
            counter[_name] += 1
            return _original(cls, *args, **query)

        monkeypatch.setattr(model, "objects", classmethod(counting))

    # The component-level CSI300 fallback goes through the *real*
    # StockDailyQuote, currently patched by the harness to the fake records.
    # Wrap whatever is installed rather than replacing it.
    from app.model.stock import StockDailyQuote as _RealQuoteModel

    installed = _RealQuoteModel.objects

    def counting_index(cls, *args, **query):
        counter["CSI300"] += 1
        return installed(*args, **query)

    monkeypatch.setattr(_RealQuoteModel, "objects", classmethod(counting_index))


def test_batch_reads_are_constant_in_cohort_size(batch_harness, monkeypatch):
    """C1/3.1: one day of reads is a constant number of queries, not O(stocks).

    Adding full-history stocks must not add quote/factor/signal round trips on
    the batch path (the one documented exception is one extra per-code quote
    read per sparse/suspended code, asserted separately), while the per-stock
    path grows linearly with the cohort.
    """
    import collections

    seed_market()
    counter = collections.Counter()
    _count_model_queries(monkeypatch, counter)

    def measure(batch):
        FakePrediction.records = []
        batch_harness.batch_prefetch = batch
        counter.clear()
        batch_harness.score_all_stocks(date=EVAL_DATE)
        return dict(counter)

    small_batch = measure(True)
    small_legacy = measure(False)
    for index in range(12):
        _seed_normal_stock(f"sh6010{index:02d}", index)
    large_batch = measure(True)
    large_legacy = measure(False)

    for model in (
        "FakeQuote",
        "FakeFactor",
        "FakeSignal",
        "FakePrediction",
        "FakeIndustryClassification",
        "FakeIndustryMetrics",
        "CSI300",
    ):
        assert large_batch.get(model, 0) == small_batch.get(model, 0), (
            f"{model} reads must not scale with cohort size "
            f"({small_batch.get(model, 0)} -> {large_batch.get(model, 0)})"
        )
    # The per-day caches are consumed on the batch path. The prefetch itself
    # performs exactly one industry classification + one L1 metrics query for
    # the whole day and no component-level CSI300 read at all, while the
    # per-stock path re-reads industry classification and CSI300 once per stock.
    assert small_batch["FakeIndustryClassification"] == 1
    assert small_batch["FakeIndustryMetrics"] == 1
    assert small_batch.get("CSI300", 0) == 0
    assert (
        large_legacy["FakeIndustryClassification"]
        > small_legacy["FakeIndustryClassification"]
    )
    assert large_legacy["CSI300"] > small_legacy["CSI300"]
    for model in ("FakeQuote", "FakeFactor", "FakeSignal"):
        assert large_legacy[model] > small_legacy[model], (
            f"per-stock {model} reads should scale with cohort size"
        )
    assert small_batch["FakeQuote"] < small_legacy["FakeQuote"]


def test_sparse_history_costs_bounded_extra_reads(batch_harness, monkeypatch):
    """The only cohort-dependent batch reads: a sparse/suspended code adds its
    exact per-code history fallback plus (when that history predates the day
    window) its own CSI300 range read. Both are per code, not per stock-horizon.
    """
    import collections

    seed_market()
    counter = collections.Counter()
    _count_model_queries(monkeypatch, counter)
    batch_harness.batch_prefetch = True
    batch_harness.score_all_stocks(date=EVAL_DATE)
    with_sparse = dict(counter)

    FakeQuote.records = [
        record for record in FakeQuote.records if record.code != "sh600003"
    ]
    FakeStock.records = [
        record for record in FakeStock.records if record.code != "sh600003"
    ]
    FakeIndustryClassification.records = [
        record
        for record in FakeIndustryClassification.records
        if record.stock_code != "sh600003"
    ]
    FakePrediction.records = []
    counter.clear()
    batch_harness.score_all_stocks(date=EVAL_DATE)
    without_sparse = dict(counter)

    # history fallback + its own CSI300 range for the pre-window quote dates
    assert with_sparse["FakeQuote"] == without_sparse["FakeQuote"] + 2
    assert with_sparse["FakeFactor"] == without_sparse["FakeFactor"]
    assert with_sparse["FakeSignal"] == without_sparse["FakeSignal"]


def test_disabled_batch_uses_the_per_stock_path(batch_harness, monkeypatch):
    seed_market()
    batch_harness.batch_prefetch = False
    per_stock_calls = []
    original = batch_harness.score_single_stock

    def recording(stock, *args, **kwargs):
        per_stock_calls.append(stock.code)
        return original(stock, *args, **kwargs)

    monkeypatch.setattr(batch_harness, "score_single_stock", recording)
    batch_harness.score_all_stocks(date=EVAL_DATE, horizon=5)

    assert per_stock_calls == list(_STOCKS)
    assert len(FakePrediction.records) == len(_STOCKS)


def test_batch_upsert_writes_through_one_natural_key_bulk(batch_harness):
    """C1/3.3: the batch path persists with natural-key upserts, not saves."""
    seed_market()
    batch_harness.batch_prefetch = True
    batch_harness.score_all_stocks(date=EVAL_DATE, horizon=5)

    operations = [
        op for operations, _ in FakePrediction.bulk_calls for op in operations
    ]
    upserts = [op for op in operations if getattr(op, "_upsert", False)]
    assert len(upserts) == len(_STOCKS)
    assert all(
        {"stock_code", "date", "horizon", "model_version"} == set(op._filter)
        for op in upserts
    )
    assert all("generated_at" in op._doc.get("$setOnInsert", {}) for op in upserts)
    assert all("updated_at" in op._doc.get("$set", {}) for op in upserts)
    # The ReferenceField must be stored as the referenced _id (a raw bulk write
    # cannot encode a Document), never as a Document object.
    stock_ids = {stock.code: stock.id for stock in FakeStock.records}
    raw_upserts = [op for op in upserts if "stock" in op._doc["$set"]]
    assert raw_upserts
    assert all(
        op._doc["$set"]["stock"] == stock_ids[op._doc["$set"]["stock_code"]]
        for op in raw_upserts
    )
    assert not any(
        isinstance(op._doc["$set"]["stock"], FakeStock) for op in raw_upserts
    )


def test_ranked_replace_false_repair_batch_matches_per_stock(batch_harness):
    """Ranked replace=False repair: a stored BLOCKED row is kept, counted for
    completeness and repaired while the missing code is recomputed."""
    seed_market()

    def seed_existing():
        return [
            FakePrediction(
                stock_code="sh600001",
                stock_name="平安银行",
                date=EVAL_DATE,
                horizon=20,
                model_version=DEFAULT_MODEL_VERSION,
                status="BLOCKED",
                score=0.0,
                rank=7,
                percentile=0.9,
                recommendation="BUY",
                input_snapshot={
                    "status": "BLOCKED",
                    "scoring_mode": "ranked",
                    # A ranked row must carry the frozen cohort fingerprint or
                    # the gate fails closed and demands replace.
                    "cohort_fingerprint": StockScoringService._cohort_fingerprint(
                        list(_STOCKS)
                    ),
                    "blocked_reason": "missing_quote",
                },
            )
        ]

    legacy, batch = _two_runs(
        batch_harness, ranked=True, horizon=20, existing=seed_existing
    )

    _assert_equivalent(legacy, batch)
    assert batch[("sh600001", 20)]["status"] == "BLOCKED"
    assert batch[("sh600001", 20)]["rank"] is None
    assert batch[("sh600001", 20)]["percentile"] is None
    assert batch[("sh600001", 20)]["recommendation"] == "NONE"


def test_index_prefetch_failure_falls_back_to_per_stock_read(
    batch_harness, monkeypatch
):
    """A failed CSI300 prefetch must fall back to the component's own read,
    not to a silent self-proxy change."""
    seed_market()
    legacy = _run(batch_harness, batch=False, horizon=60)

    original = FakeQuote.objects.__func__

    def guarded(cls, **query):
        if query.get("code") == "sh000300":
            raise RuntimeError("index prefetch boom")
        return original(cls, **query)

    monkeypatch.setattr(FakeQuote, "objects", classmethod(guarded))
    batch = _run(batch_harness, batch=True, horizon=60)

    _assert_equivalent(legacy, batch)
    assert any(
        "CSI 300" in component["label"]
        for entry in batch.values()
        for component in entry["explanation"]["components"]
        if component["id"] == "real_relative_strength"
    )


def test_industry_prefetch_failure_falls_back_to_per_stock_read(
    batch_harness, monkeypatch
):
    """A failed industry prefetch must fall back to the component's own reads
    instead of silently scoring every stock as "no industry"."""

    class ExplodingIndustryModel:
        @classmethod
        def objects(cls, **query):
            raise RuntimeError("industry prefetch boom")

    seed_market()
    legacy = _run(batch_harness, batch=False, horizon=20)
    batch_harness.industry_model = ExplodingIndustryModel
    batch = _run(batch_harness, batch=True, horizon=20)

    _assert_equivalent(legacy, batch)
    assert any(
        component["id"] == "industry_momentum"
        and component["evidence"].get("industry_name") == "银行"
        for entry in batch.values()
        for component in entry["explanation"]["components"]
    )


def test_row_from_frame_normalises_pandas_values():
    """pandas NaN/NaT/Timestamp must not leak into component inputs."""
    import pandas as pd

    from app.lib.scoring_engine.scoring_service import _row_from_frame

    frame = pd.DataFrame.from_records(
        [
            {
                "code": "sh600000",
                "date": EVAL_DATE,
                "close": 9.5,
                "close_hfq": None,
                "high": float("nan"),
            },
            {
                "code": "sh600000",
                "date": None,
                "close": None,
                "close_hfq": 8.0,
                "high": 10.0,
            },
        ]
    )
    rows = [_row_from_frame(record) for record in frame.to_dict("records")]

    assert isinstance(rows[0].date, datetime.datetime)
    assert not isinstance(rows[0].date, pd.Timestamp)
    assert rows[0].close == 9.5
    assert rows[0].close_hfq is None
    assert rows[0].high is None  # NaN -> None, not float('nan')
    assert rows[1].date is None  # NaT -> None
    assert rows[1].close is None
    assert rows[1].close_hfq == 8.0


def test_signal_evidence_order_is_canonical_and_path_independent(batch_harness):
    """Signal read order is a persisted field, so both paths must agree.

    ``stock_signal_daily`` is unsorted and the batch path reads it with
    ``stock_code__in`` while the per-stock path reads ``stock_code=X``. A real
    database returned those rows in different orders, which diverged on 27 of
    5,561 rows of ``explanation[].evidence.signals`` in the dev full-market
    check. Seed the signals in reverse name order so an unsorted read is
    detectable, then require both paths to persist the same, name-ordered list.
    """
    from app.lib.scoring_engine.scoring_service import _signal_order_key

    seed_market()
    # Deliberately reverse-alphabetical insertion order for one stock.
    for name in ("PRICE_ABOVE_MA60", "MA20_ABOVE_MA60", "MA10_CROSS_MA20"):
        FakeSignal.records.append(
            FakeSignal(
                stock_code="sh600000",
                date=EVAL_DATE,
                signal_name=name,
                direction="BULLISH",
                strength=1.0,
                reason=name,
            )
        )

    def names_for(batch: bool):
        FakePrediction.records = []
        batch_harness.batch_prefetch = batch
        batch_harness.score_all_stocks(date=EVAL_DATE, horizon=20)
        stored = [
            p
            for p in FakePrediction.records
            if p.stock_code == "sh600000" and p.horizon == 20
        ]
        assert len(stored) == 1
        components = stored[0].explanation["components"]
        signal_component = next(c for c in components if c["id"] == "signal_strength")
        return [s["name"] for s in signal_component["evidence"]["signals"]]

    batch_names = names_for(True)
    legacy_names = names_for(False)

    assert batch_names == legacy_names
    assert batch_names == sorted(batch_names)
    # The three seeded names are present and, because they were inserted in
    # reverse-alphabetical order, a raw (unsorted) read would have put
    # PRICE_ABOVE_MA60 first.
    for name in ("MA10_CROSS_MA20", "MA20_ABOVE_MA60", "PRICE_ABOVE_MA60"):
        assert name in batch_names
    assert batch_names.index("MA10_CROSS_MA20") < batch_names.index("PRICE_ABOVE_MA60")
    assert _signal_order_key(type("S", (), {"signal_name": None})()) == ""
