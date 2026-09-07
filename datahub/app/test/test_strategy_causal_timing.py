"""Regressions for causal paper scheduling and retrospective provenance."""

import datetime as dt
from types import SimpleNamespace

import pytest

from app.lib.strategy_engine.config import validate_strategy_config
from app.lib.strategy_engine.runner import schedule_from_runs, attach_nav_points


def test_signal_executes_after_holiday_and_attaches_to_signal():
    signal = dt.datetime(2026, 9, 4)
    execution = dt.datetime(2026, 9, 8)
    run = SimpleNamespace(
        date=signal,
        execution_date=execution,
        target_holdings=[{"stock_code": "a", "weight": 1.0}],
    )
    assert schedule_from_runs([run])[0]["date"] == "2026-09-08"
    points = attach_nav_points([run], [{"date": "2026-09-08", "nav": 100}])
    assert points["points_by_date"]["2026-09-04"]["nav"] == 100


def test_legacy_schedule_cannot_execute_on_signal_date():
    run = SimpleNamespace(
        date=dt.datetime(2026, 9, 4),
        target_holdings=[{"stock_code": "a", "weight": 1.0}],
    )
    with pytest.raises(ValueError, match="execution_date"):
        schedule_from_runs([run])


def test_calendar_requires_strictly_later_session():
    from app.lib.strategy_engine.runner import next_execution_date

    friday, tuesday = dt.datetime(2026, 9, 4), dt.datetime(2026, 9, 8)
    assert next_execution_date(friday, [tuesday, friday]) == tuesday
    with pytest.raises(ValueError, match="next trading session"):
        next_execution_date(tuesday, [friday, tuesday])
    with pytest.raises(ValueError, match="signal date"):
        next_execution_date(dt.datetime(2026, 9, 5), [friday, tuesday])


def test_config_pins_new_timing_semantics():
    cfg = validate_strategy_config({"score_model_version": "v1"})
    assert cfg["timing_version"] == "paper_causal_v1"
    with pytest.raises(ValueError, match="timing_version"):
        validate_strategy_config(
            {"score_model_version": "v1", "timing_version": "legacy"}
        )


def test_usable_query_is_independent_of_outcome_maturity(monkeypatch):
    import app.jobs.strategy_runner as job
    import app.model.scoring as models

    rows = [
        SimpleNamespace(stock_code=str(i), status=status, score=100 - i, percentile=0.9)
        for i, status in enumerate(
            [
                "PENDING",
                "TRACKING",
                "VERIFIED",
                "INSUFFICIENT_DATA",
                "BLOCKED",
                "FAILED",
            ]
        )
    ]

    class QS(list):
        def order_by(self, *_):
            return self

    class Model:
        @staticmethod
        def objects(**query):
            assert query["model_version"] == "v1"
            return QS([r for r in rows if r.status in query["status__in"]])

    monkeypatch.setattr(models, "StockScorePrediction", Model)
    date = dt.datetime(2026, 9, 4)
    from app.lib.strategy_engine.selection import select_target_holdings

    cfg = {"score_model_version": "v1"}
    before = job._query_usable_predictions("v1", date, 20)
    targets_before = select_target_holdings(before, cfg)
    rows[0].status = "INSUFFICIENT_DATA"
    rows[1].status = "VERIFIED"
    after = job._query_usable_predictions("v1", date, 20)
    assert select_target_holdings(after, cfg) == targets_before
    assert (
        [r.stock_code for r in before]
        == [r.stock_code for r in after]
        == ["0", "1", "2", "3"]
    )


def test_quote_loader_preserves_suspension_and_unknown(monkeypatch):
    import app.jobs.strategy_runner as job
    import app.model.stock as models

    date = dt.datetime(2026, 9, 4)
    rows = [
        SimpleNamespace(code=str(i), date=date, open=10, close=10, trade_status=status)
        for i, status in enumerate([0, None, 1])
    ]
    monkeypatch.setattr(
        models, "StockDailyQuote", SimpleNamespace(objects=lambda **_: rows)
    )
    loaded = job._load_quotes_for_codes(["0", "1", "2"], date, date)
    assert [loaded[str(i)]["2026-09-04"].trade_status for i in range(3)] == [0, None, 1]


@pytest.fixture
def paper_store(monkeypatch):
    import app.jobs.strategy_runner as job
    import app.model.strategy as models
    from app.lib.strategy_engine.config import strategy_config_hash

    records, queries = [], []

    class QS(list):
        def first(self):
            return self[0] if self else None

        def order_by(self, field):
            return QS(
                sorted(
                    self,
                    key=lambda r: getattr(r, field.lstrip("-")),
                    reverse=field.startswith("-"),
                )
            )

    class Run:
        def __init__(self, **values):
            self.__dict__.update(values)

        def save(self):
            if self not in records:
                records.append(self)

        def delete(self):
            records.remove(self)

        @staticmethod
        def objects(**query):
            queries.append(query)

            def matches(row):
                for key, value in query.items():
                    name, _, op = key.partition("__")
                    current = getattr(row, name, None)
                    if op == "lt" and not current < value:
                        return False
                    if op == "gte" and not current >= value:
                        return False
                    if op == "lte" and not current <= value:
                        return False
                    if op == "in" and current not in value:
                        return False
                    if not op and current != value:
                        return False
                return True

            return QS([r for r in records if matches(r)])

    monkeypatch.setattr(models, "StrategyPaperRun", Run)

    class WindowModel:
        @staticmethod
        def objects(**query):
            return QS([])

    monkeypatch.setattr(models, "StrategyForwardWindow", WindowModel)
    monkeypatch.setattr(
        job, "_resolve_model_version", lambda cfg: cfg["score_model_version"]
    )
    monkeypatch.setattr(
        job, "_execution_date", lambda date: date + dt.timedelta(days=3)
    )
    monkeypatch.setattr(
        job,
        "_query_usable_predictions",
        lambda *_: [SimpleNamespace(stock_code="a", score=20, percentile=1)],
    )
    monkeypatch.setattr(
        job, "_query_flags", lambda *_, **kw: {"a": {"trade_status": 1}}
    )
    cfg = {"score_model_version": "v1", "initial_nav": 100000}
    normalized = validate_strategy_config(cfg)

    def add(date, config=cfg, execution=None, evidence="REPLAY"):
        c = validate_strategy_config(config)
        row = Run(
            strategy_name="flip_wide_paper",
            date=date,
            execution_date=execution or date + dt.timedelta(days=3),
            evidence_kind=evidence,
            model_version="v1",
            horizon=20,
            config_hash=strategy_config_hash(c),
            config=c,
            status="COMPLETED",
            target_holdings=[{"stock_code": "b", "weight": 0.5}],
            nav_snapshot={},
        )
        row.save()
        return row

    return SimpleNamespace(
        job=job,
        records=records,
        queries=queries,
        add=add,
        cfg=cfg,
        normalized=normalized,
    )


def test_new_and_replaced_history_stay_replay_and_track_isolated(paper_store):
    p = paper_store
    p.add(dt.datetime(2026, 9, 1), config={**p.cfg, "initial_nav": 200000})
    date = dt.datetime(2026, 9, 4)
    started = dt.datetime.now(dt.UTC)
    result = p.job.run_strategy(date=date, config=p.cfg)
    assert result["evidence_kind"] == "REPLAY"
    assert (
        result["rebalance"]["removed"] == []
    )  # no prior holdings from another capital track
    row = p.records[-1]
    assert row.decision_at.tzinfo == dt.UTC
    assert row.execution_date == dt.datetime(2026, 9, 7)
    assert started <= row.decision_at <= dt.datetime.now(dt.UTC)
    p.job.run_strategy(date=date, config=p.cfg, replace=True)
    assert len(p.records) == 2
    assert p.records[-1].evidence_kind == "REPLAY"
    prior_query = next(q for q in p.queries if "date__lt" in q)
    assert prior_query["config_hash"] == result["config_hash"]


def test_nav_uses_exact_track_and_extends_to_execution_date(paper_store, monkeypatch):
    from app.lib.strategy_engine.nav import QuoteView

    p = paper_store
    signal = dt.datetime(2026, 9, 4)
    chosen = p.add(signal)
    other = p.add(signal, config={**p.cfg, "initial_nav": 200000})
    legacy = p.add(signal, evidence=None)

    def quotes(codes, start, end):
        assert (start, end) == (dt.datetime(2026, 9, 7), dt.datetime(2026, 9, 7))
        return {"b": {"2026-09-07": QuoteView(10, 11)}}

    monkeypatch.setattr(p.job, "_load_quotes_for_codes", quotes)
    monkeypatch.setattr(p.job, "_benchmark_returns_for_dates", lambda *_: {})
    result = p.job.run_nav(from_date=signal, to_date=signal, config=p.cfg)
    assert result["found_runs"] == result["updated_runs"] == 1
    assert result["evidence_kind"] == "REPLAY"
    assert chosen.nav_snapshot["date"] == "2026-09-07"
    assert chosen.nav_snapshot["positions_count"] == 1
    assert other.nav_snapshot == legacy.nav_snapshot == {}


def test_missing_execution_coverage_writes_no_nav(paper_store, monkeypatch):
    from app.lib.strategy_engine.nav import QuoteView

    p = paper_store
    signal = dt.datetime(2026, 9, 4)
    row = p.add(signal)
    monkeypatch.setattr(
        p.job,
        "_load_quotes_for_codes",
        lambda *_: {"b": {"2026-09-04": QuoteView(10, 11)}},
    )
    monkeypatch.setattr(p.job, "_benchmark_returns_for_dates", lambda *_: {})
    with pytest.raises(ValueError, match="missing execution-day quote coverage"):
        p.job.run_nav(from_date=signal, to_date=signal, config=p.cfg)
    assert row.nav_snapshot == {}


def test_failed_calendar_does_not_persist(paper_store, monkeypatch):
    p = paper_store

    def fail(_):
        raise ValueError("calendar has no next trading session")

    monkeypatch.setattr(p.job, "_execution_date", fail)
    with pytest.raises(ValueError, match="next trading session"):
        p.job.run_strategy(date=dt.datetime(2026, 9, 4), config=p.cfg)
    assert p.records == []


def test_duplicate_execution_sessions_are_rejected():
    runs = [
        SimpleNamespace(
            date=dt.datetime(2026, 9, day),
            execution_date=dt.datetime(2026, 9, 8),
            target_holdings=[{"stock_code": "a", "weight": 0.5}],
        )
        for day in (4, 7)
    ]
    with pytest.raises(ValueError, match="duplicate execution_date"):
        schedule_from_runs(runs)
