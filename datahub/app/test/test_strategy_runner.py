# -*- coding: utf-8 -*-
"""Tests for the strategy_runner job wiring (parser + fail-closed registry).

DB-dependent run_strategy internals are exercised with monkeypatched models;
the engine decision logic itself is covered in test_strategy_engine.py.
"""

import datetime


class _FakeRegistered:
    def __init__(self, config):
        self.config = config


class _FakeQS:
    def __init__(self, items):
        self.items = items

    def first(self):
        return self.items[0] if self.items else None

    def limit(self, count):
        return _FakeQS(self.items[:count])

    def order_by(self, *fields):
        return self

    def __iter__(self):
        return iter(self.items)

    def delete(self):
        self.items = []


def test_resolve_model_version_ok(monkeypatch):
    import app.jobs.strategy_runner as strategy_runner
    import app.model.scoring as model_scoring

    cfg = {"score_model_version": "flip_wide_shadow_v1", "horizon": 20}

    class FakeModel:
        @classmethod
        def objects(cls, **query):
            assert query["model_version"] == "flip_wide_shadow_v1"
            assert query["status"] == "ACTIVE"
            return _FakeQS([_FakeRegistered({"20": {"directions": {"momentum": -1}}})])

    monkeypatch.setattr(model_scoring, "ScoreModelVersion", FakeModel)
    assert strategy_runner._resolve_model_version(cfg) == "flip_wide_shadow_v1"


def test_resolve_model_version_fails_closed_when_unregistered(monkeypatch):
    import pytest

    import app.jobs.strategy_runner as strategy_runner
    import app.model.scoring as model_scoring

    cfg = {"score_model_version": "ghost_v1", "horizon": 20}

    class FakeModel:
        @classmethod
        def objects(cls, **query):
            return _FakeQS([])

    monkeypatch.setattr(model_scoring, "ScoreModelVersion", FakeModel)
    with pytest.raises(ValueError, match="not ACTIVE-registered"):
        strategy_runner._resolve_model_version(cfg)


def test_resolve_model_version_fails_closed_when_horizon_missing(monkeypatch):
    import pytest

    import app.jobs.strategy_runner as strategy_runner
    import app.model.scoring as model_scoring

    cfg = {"score_model_version": "flip_wide_shadow_v1", "horizon": 60}

    class FakeModel:
        @classmethod
        def objects(cls, **query):
            return _FakeQS([_FakeRegistered({"20": {"directions": {"momentum": -1}}})])

    monkeypatch.setattr(model_scoring, "ScoreModelVersion", FakeModel)
    with pytest.raises(ValueError, match="does not cover"):
        strategy_runner._resolve_model_version(cfg)


def test_main_run_parser_requires_date(monkeypatch):
    import pytest

    import app.jobs.strategy_runner as strategy_runner

    def fake_main_guard(_):
        raise AssertionError("must not run without date")

    monkeypatch.setattr(strategy_runner, "_init_db", lambda: None)
    monkeypatch.setattr(strategy_runner, "_run_with_tracking", fake_main_guard)
    with pytest.raises(SystemExit) as excinfo:
        strategy_runner.main(["run"])
    assert excinfo.value.code == 2


def test_main_report_parser_requires_date():
    import pytest

    from app.jobs.strategy_runner import build_parser

    with pytest.raises(SystemExit) as excinfo:
        build_parser().parse_args(["report"])
    assert excinfo.value.code == 2


def test_run_strategy_dry_run_skips_when_no_predictions(monkeypatch):
    import app.jobs.strategy_runner as strategy_runner
    import app.model.scoring as model_scoring
    import app.model.strategy as model_strategy

    cfg = {"score_model_version": "flip_wide_shadow_v1", "horizon": 20}

    class FakeModel:
        @classmethod
        def objects(cls, **query):
            return _FakeQS([_FakeRegistered({"20": {"directions": {"momentum": -1}}})])

    class FakeRunQS(_FakeQS):
        pass

    class FakeRunModel:
        @classmethod
        def objects(cls, **query):
            return FakeRunQS([])

    monkeypatch.setattr(model_scoring, "ScoreModelVersion", FakeModel)
    monkeypatch.setattr(model_strategy, "StrategyPaperRun", FakeRunModel)
    monkeypatch.setattr(
        strategy_runner,
        "_query_verified_predictions",
        lambda mv, date, h: [],
    )
    monkeypatch.setattr(
        strategy_runner, "_query_flags", lambda date, h, model_version=None: {}
    )
    result = strategy_runner.run_strategy(
        date=datetime.datetime(2026, 4, 10, tzinfo=datetime.UTC),
        config=cfg,
        dry_run=True,
    )
    assert result["dry_run"] is True
    assert result["plan"]["skipped"] is True
    assert "no VERIFIED predictions" in result["plan"]["reason"]


def test_run_strategy_non_dry_skip_persists_skipped(monkeypatch):
    """P1-1 regression: a real (non-dry) skip returns status SKIPPED (and no
    written data), so the freshness layer records SKIPPED not SUCCESS."""
    import app.jobs.strategy_runner as strategy_runner
    import app.model.scoring as model_scoring
    import app.model.strategy as model_strategy

    saved = []

    class FakeRunModel:
        def __init__(self, **kwargs):
            for k, v in kwargs.items():
                setattr(self, k, v)
            self.status = getattr(self, "status", "RUNNING")
            self.completed_at = None

        def save(self):
            saved.append(self)

        def delete(self):
            pass

        @classmethod
        def objects(cls, **query):
            return _FakeQS([])

    class FakeRegModel:
        @classmethod
        def objects(cls, **query):
            return _FakeQS([_FakeRegistered({"20": {"directions": {"momentum": -1}}})])

    monkeypatch.setattr(model_scoring, "ScoreModelVersion", FakeRegModel)
    monkeypatch.setattr(model_strategy, "StrategyPaperRun", FakeRunModel)
    monkeypatch.setattr(
        strategy_runner, "_query_verified_predictions", lambda mv, d, h: []
    )
    monkeypatch.setattr(
        strategy_runner, "_query_flags", lambda d, h, model_version=None: {}
    )

    result = strategy_runner.run_strategy(
        date=datetime.datetime(2026, 4, 10, tzinfo=datetime.UTC),
        config={"score_model_version": "flip_wide_shadow_v1"},
    )
    assert result["status"] == "SKIPPED"
    assert "no VERIFIED predictions" in result["skip_reason"]
    assert len(saved) == 1
    assert saved[0].status == "SKIPPED"
    assert saved[0].target_holdings == []


def test_run_strategy_rebalance_diffs_against_previous_run(monkeypatch):
    """The persisted rebalance must reflect the previous COMPLETED run's
    holdings (spec: 'rebalance list reflects changes vs previous portfolio')."""
    import app.jobs.strategy_runner as strategy_runner
    import app.model.scoring as model_scoring
    import app.model.strategy as model_strategy

    prior_date = datetime.datetime(2026, 4, 3, tzinfo=datetime.UTC)
    this_date = datetime.datetime(2026, 4, 10, tzinfo=datetime.UTC)

    class PriorRun:
        date = prior_date
        status = "COMPLETED"
        target_holdings = [{"stock_code": "sh600001", "weight": 1.0}]

    class FakePred:
        def __init__(self, stock_code, score, percentile):
            self.stock_code = stock_code
            self.score = score
            self.percentile = percentile

    class FakeRunQS(_FakeQS):
        def order_by(self, *fields):
            return self

        def first(self):
            return self.items[0] if self.items else None

    class FakeRunModel:
        @classmethod
        def objects(cls, **query):
            if query.get("date__lt") is not None:
                return FakeRunQS([PriorRun()])
            return FakeRunQS([])

    class FakeRegModel:
        @classmethod
        def objects(cls, **query):
            return _FakeQS([_FakeRegistered({"20": {"directions": {"momentum": -1}}})])

    monkeypatch.setattr(model_scoring, "ScoreModelVersion", FakeRegModel)
    monkeypatch.setattr(model_strategy, "StrategyPaperRun", FakeRunModel)
    monkeypatch.setattr(
        strategy_runner,
        "_query_verified_predictions",
        lambda mv, d, h: [
            FakePred("sh600001", 90.0, 0.99),
            FakePred("sh600002", 80.0, 0.98),
        ],
    )
    monkeypatch.setattr(
        strategy_runner,
        "_query_flags",
        lambda d, h, model_version=None: {
            "sh600001": {"trade_status": 1},
            "sh600002": {"trade_status": 1},
        },
    )

    result = strategy_runner.run_strategy(
        date=this_date,
        config={
            "score_model_version": "flip_wide_shadow_v1",
            "selection": {"mode": "top_n", "portfolio_size": 10},
        },
        dry_run=True,
    )
    assert result["previous_run_date"] == prior_date.date()
    # sh600001 kept from prior run; sh600002 added
    assert result["plan"]["rebalance"]["added"] == ["sh600002"]
    assert result["plan"]["rebalance"]["removed"] == []


def test_query_flags_reads_stock_daily_quote_excludes_missing(monkeypatch):
    """P1-2 regression: flags come from StockDailyQuote (the populated store);
    codes with no quote row for the date are omitted (fail-closed)."""
    import app.jobs.strategy_runner as strategy_runner
    import app.model.scoring as model_scoring
    import app.model.stock as model_stock

    date = datetime.datetime(2026, 4, 10, tzinfo=datetime.UTC)

    class FakePred:
        stock_code = None

    class FakePredQS(_FakeQS):
        def only(self, *fields):
            return self

        def __call__(self, **extra):
            return self

    class FakePredModel:
        @classmethod
        def objects(cls, **query):
            return FakePredQS(
                [
                    type("P", (), {"stock_code": "sh600000"})(),
                    type("P", (), {"stock_code": "sh600001"})(),
                ]
            )

    class FakeQuote:
        def __init__(self, code, trade_status=1, isST=0):
            self.code = code
            self.trade_status = trade_status
            self.isST = isST

    class FakeQuoteModel:
        @classmethod
        def objects(cls, **query):
            assert query["code__in"] == ["sh600000", "sh600001"]
            return _FakeQS([FakeQuote("sh600000", trade_status=1, isST=1)])

    monkeypatch.setattr(model_scoring, "StockScorePrediction", FakePredModel)
    monkeypatch.setattr(model_stock, "StockDailyQuote", FakeQuoteModel)
    import app.lib.utilities.data_capability_helper as dch

    monkeypatch.setattr(dch, "is_bse_stock_code", lambda code: code.startswith("bj"))

    flags = strategy_runner._query_flags(date, 20, model_version="flip_wide_shadow_v1")
    assert set(flags) == {"sh600000"}  # sh600001 has no quote -> fail-closed
    assert flags["sh600000"]["is_st"] == 1


# ---------------------------------------------------------------------------
# nav helpers (pure) + run_nav wiring
# ---------------------------------------------------------------------------


class _FakeRun:
    def __init__(self, date, target_holdings, status="COMPLETED"):
        self.date = date
        self.target_holdings = target_holdings
        self.status = status

    def save(self):
        pass


def test_schedule_from_runs_sorts_and_skips_empty():
    from app.lib.strategy_engine.runner import schedule_from_runs

    runs = [
        _FakeRun(
            datetime.datetime(2026, 4, 10, tzinfo=datetime.UTC),
            [{"stock_code": "a", "weight": 0.5}, {"stock_code": "b", "weight": 0.5}],
        ),
        _FakeRun(datetime.datetime(2026, 4, 3, tzinfo=datetime.UTC), []),  # skipped
        _FakeRun(
            datetime.datetime(2026, 4, 13, tzinfo=datetime.UTC),
            [{"stock_code": "a", "weight": 1.0}],
        ),
    ]
    schedule = schedule_from_runs(runs)
    # the empty-holdings run (04-03) is skipped; remaining sorted ascending
    # with iso-string dates (the key space simulate_paper_nav + quote/benchmark
    # loaders share — datetime keys would silently open zero positions)
    assert [d["date"] for d in schedule] == ["2026-04-10", "2026-04-13"]
    assert set(schedule[0]["holdings"]) == {"a", "b"}
    assert schedule[1]["holdings"] == {"a": 1.0}


def test_attach_nav_points_matches_by_date():
    from app.lib.strategy_engine.runner import attach_nav_points

    runs = [
        _FakeRun(
            datetime.datetime(2026, 4, 10, tzinfo=datetime.UTC),
            [{"stock_code": "a", "weight": 1.0}],
        ),
        _FakeRun(
            datetime.datetime(2026, 4, 13, tzinfo=datetime.UTC),
            [{"stock_code": "a", "weight": 1.0}],
        ),
    ]
    curve = [
        {
            "date": datetime.datetime(2026, 4, 10, tzinfo=datetime.UTC),
            "nav": 1000.0,
            "daily_return": None,
            "turnover": 1.0,
            "drawdown": 0.0,
            "positions_count": 1,
        },
        {
            "date": datetime.datetime(2026, 4, 13, tzinfo=datetime.UTC),
            "nav": 1010.0,
            "daily_return": 0.01,
            "turnover": 0.0,
            "drawdown": 0.0,
            "positions_count": 1,
        },
    ]
    attached = attach_nav_points(runs, curve)
    assert set(attached["points_by_date"]) == {"2026-04-10", "2026-04-13"}
    assert attached["points_by_date"]["2026-04-13"]["nav"] == 1010.0
    assert attached["unmatched_dates"] == []


def test_run_nav_returns_no_runs_message(monkeypatch):
    import app.jobs.strategy_runner as strategy_runner
    import app.model.scoring as model_scoring
    import app.model.strategy as model_strategy

    class FakeRegModel:
        @classmethod
        def objects(cls, **query):
            return _FakeQS([_FakeRegistered({"20": {"directions": {"momentum": -1}}})])

    class FakeRunModel:
        @classmethod
        def objects(cls, **query):
            assert query["status"] == "COMPLETED"
            return _FakeQS([])

    monkeypatch.setattr(model_scoring, "ScoreModelVersion", FakeRegModel)
    monkeypatch.setattr(model_strategy, "StrategyPaperRun", FakeRunModel)
    result = strategy_runner.run_nav(
        from_date=datetime.datetime(2026, 3, 1, tzinfo=datetime.UTC),
        to_date=datetime.datetime(2026, 6, 30, tzinfo=datetime.UTC),
        config={"score_model_version": "flip_wide_shadow_v1"},
    )
    assert result["found_runs"] == 0
    assert "no COMPLETED runs" in result["reason"]


def test_nav_parser_requires_range():
    import pytest

    from app.jobs.strategy_runner import build_parser

    with pytest.raises(SystemExit) as excinfo:
        build_parser().parse_args(["nav"])
    assert excinfo.value.code == 2


def test_run_nav_happy_path_opens_positions_and_moves_nav(monkeypatch):
    """P1 regression: the composed path (schedule -> quotes -> benchmark ->
    simulate -> writeback) must open positions and move NAV off initial_nav.
    A date-key type mismatch made this degenerate to an all-cash flat curve
    while still reporting updated_runs > 0."""
    import app.jobs.strategy_runner as strategy_runner
    import app.model.scoring as model_scoring
    import app.model.strategy as model_strategy
    from app.lib.strategy_engine.nav import QuoteView

    d1 = datetime.datetime(2026, 4, 10, tzinfo=datetime.UTC)
    d2 = datetime.datetime(2026, 4, 13, tzinfo=datetime.UTC)

    class FakeRegModel:
        @classmethod
        def objects(cls, **query):
            return _FakeQS([_FakeRegistered({"20": {"directions": {"momentum": -1}}})])

    class FakeRunDoc:
        def __init__(self, date, target_holdings, config=None):
            self.date = date
            self.target_holdings = target_holdings
            self.config = config or {"score_model_version": "flip_wide_shadow_v1"}
            self.status = "COMPLETED"
            self.completed_at = date
            self.nav_snapshot = None

        def save(self):
            pass

    class FakeRunModel:
        @classmethod
        def objects(cls, **query):
            runs = [
                FakeRunDoc(
                    d1,
                    [
                        {"stock_code": "a", "weight": 0.5},
                        {"stock_code": "b", "weight": 0.5},
                    ],
                ),
                FakeRunDoc(d2, [{"stock_code": "a", "weight": 1.0}]),
            ]
            return _FakeQS(runs)

    monkeypatch.setattr(model_scoring, "ScoreModelVersion", FakeRegModel)
    monkeypatch.setattr(model_strategy, "StrategyPaperRun", FakeRunModel)
    monkeypatch.setattr(
        strategy_runner,
        "_load_quotes_for_codes",
        lambda codes, f, t: {
            "a": {
                "2026-04-10": QuoteView(10.0, 10.5),
                "2026-04-13": QuoteView(10.5, 11.0),
            },
            "b": {
                "2026-04-10": QuoteView(20.0, 21.0),
                "2026-04-13": QuoteView(21.0, 20.0),
            },
        },
    )
    monkeypatch.setattr(
        strategy_runner,
        "_benchmark_returns_for_dates",
        lambda f, t: {"2026-04-10": 0.005, "2026-04-13": -0.002},
    )

    result = strategy_runner.run_nav(
        from_date=datetime.datetime(2026, 4, 1, tzinfo=datetime.UTC),
        to_date=datetime.datetime(2026, 4, 30, tzinfo=datetime.UTC),
        config={"score_model_version": "flip_wide_shadow_v1"},
    )
    assert result["found_runs"] == 2
    assert result["updated_runs"] == 2
    assert result["curve_points"] == 2
    assert result["terminal_nav"] != result["initial_nav"]  # NAV moved
    assert result["benchmark_dates"] == 2
