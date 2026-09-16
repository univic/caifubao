# -*- coding: utf-8 -*-
"""Tests for the strategy_runner job wiring (parser + fail-closed registry).

DB-dependent run_strategy internals are exercised with monkeypatched models;
the engine decision logic itself is covered in test_strategy_engine.py.
"""

import datetime

import pytest


def _test_execution(date):
    days = {
        "2026-04-03": "2026-04-07",
        "2026-04-10": "2026-04-13",
        "2026-04-13": "2026-04-14",
    }
    return datetime.datetime.fromisoformat(days[date.date().isoformat()])


@pytest.fixture(autouse=True)
def calendar_stub(monkeypatch):
    import app.jobs.strategy_runner as job

    monkeypatch.setattr(job, "_execution_date", _test_execution)


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

    class FakeWindowModel:
        @classmethod
        def objects(cls, **query):
            return _FakeQS([])

    monkeypatch.setattr(model_strategy, "StrategyForwardWindow", FakeWindowModel)
    monkeypatch.setattr(
        strategy_runner,
        "_query_usable_predictions",
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
    assert "no usable predictions" in result["plan"]["reason"]


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

    class FakeWindowModel:
        @classmethod
        def objects(cls, **query):
            return _FakeQS([])

    monkeypatch.setattr(model_strategy, "StrategyForwardWindow", FakeWindowModel)
    monkeypatch.setattr(
        strategy_runner, "_query_usable_predictions", lambda mv, d, h: []
    )
    monkeypatch.setattr(
        strategy_runner, "_query_flags", lambda d, h, model_version=None: {}
    )

    result = strategy_runner.run_strategy(
        date=datetime.datetime(2026, 4, 10, tzinfo=datetime.UTC),
        config={"score_model_version": "flip_wide_shadow_v1"},
    )
    assert result["status"] == "SKIPPED"
    assert "no usable predictions" in result["skip_reason"]
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

    class FakeWindowModel:
        @classmethod
        def objects(cls, **query):
            return _FakeQS([])

    monkeypatch.setattr(model_strategy, "StrategyForwardWindow", FakeWindowModel)
    monkeypatch.setattr(
        strategy_runner,
        "_query_usable_predictions",
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
        def __init__(self, code, trade_status=1, isST=0, trade_amount=None):
            self.code = code
            self.trade_status = trade_status
            self.isST = isST
            self.trade_amount = trade_amount

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
        self.execution_date = _test_execution(date)
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
    assert [d["date"] for d in schedule] == ["2026-04-13", "2026-04-14"]
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
            "date": datetime.datetime(2026, 4, 13, tzinfo=datetime.UTC),
            "nav": 1000.0,
            "daily_return": None,
            "turnover": 1.0,
            "drawdown": 0.0,
            "positions_count": 1,
        },
        {
            "date": datetime.datetime(2026, 4, 14, tzinfo=datetime.UTC),
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

    class FakeWindowModel:
        @classmethod
        def objects(cls, **query):
            return _FakeQS([])

    monkeypatch.setattr(model_strategy, "StrategyForwardWindow", FakeWindowModel)
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
            self.execution_date = _test_execution(date)
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

    class FakeWindowModel:
        @classmethod
        def objects(cls, **query):
            return _FakeQS([])

    monkeypatch.setattr(model_strategy, "StrategyForwardWindow", FakeWindowModel)
    monkeypatch.setattr(
        strategy_runner,
        "_load_quotes_for_codes",
        lambda codes, f, t: {
            "a": {
                "2026-04-13": QuoteView(10.0, 10.5),
                "2026-04-14": QuoteView(10.5, 11.0),
            },
            "b": {
                "2026-04-13": QuoteView(20.0, 21.0),
                "2026-04-14": QuoteView(21.0, 20.0),
            },
        },
    )
    monkeypatch.setattr(
        strategy_runner,
        "_benchmark_returns_for_dates",
        lambda f, t: {"2026-04-13": 0.005, "2026-04-14": -0.002},
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


# ---------------------------------------------------------------------------
# export (roadmap 2.1) — runner-layer mapping, fail-closed, read-only
# ---------------------------------------------------------------------------

_EXPORT_CFG = {"score_model_version": "flip_wide_shadow_v1", "horizon": 20}


class _ExportDoc:
    """A persisted paper run; save/delete raise so read-only is provable."""

    def __init__(self, **overrides):
        self.strategy_name = "flip_wide_paper"
        self.model_version = "flip_wide_shadow_v1"
        self.horizon = 20
        self.config_hash = "hash-a"
        self.date = datetime.datetime(2026, 9, 11)
        self.execution_date = datetime.datetime(2026, 9, 14)
        self.decision_at = datetime.datetime(2026, 9, 11, 10, 40, tzinfo=datetime.UTC)
        self.evidence_kind = "REPLAY"
        self.status = "COMPLETED"
        self.target_holdings = [{"stock_code": "sz000001", "weight": 0.5}]
        self.rebalance = {"added": ["sz000001"], "removed": [], "unchanged": []}
        self.nav_snapshot = {}
        self.config = {"initial_nav": 1_000_000.0}
        self.__dict__.update(overrides)

    def save(self, *args, **kwargs):  # pragma: no cover - must never be called
        raise AssertionError("export must not write")

    def delete(self, *args, **kwargs):  # pragma: no cover - must never be called
        raise AssertionError("export must not delete")


def _patch_export_deps(monkeypatch, docs):
    import app.jobs.strategy_runner as strategy_runner
    import app.model.scoring as model_scoring
    import app.model.strategy as model_strategy

    class FakeRegModel:
        @classmethod
        def objects(cls, **query):
            return _FakeQS([_FakeRegistered({"20": {"directions": {}}})])

    class FakeRunModel:
        @classmethod
        def objects(cls, **query):
            return _FakeQS(docs)

    monkeypatch.setattr(model_scoring, "ScoreModelVersion", FakeRegModel)
    monkeypatch.setattr(model_strategy, "StrategyPaperRun", FakeRunModel)
    monkeypatch.setattr(
        strategy_runner,
        "_query_export_scores",
        lambda d, h, mv: {"sz000001": {"score": 88.0, "percentile": 0.04}},
    )
    monkeypatch.setattr(
        strategy_runner, "_query_stock_names", lambda codes: {"sz000001": "平安银行"}
    )


def test_export_targets_maps_a_completed_run(monkeypatch):
    import app.jobs.strategy_runner as strategy_runner

    _patch_export_deps(monkeypatch, [_ExportDoc()])
    result = strategy_runner.export_targets(
        date=datetime.datetime(2026, 9, 11, tzinfo=datetime.UTC),
        model_version="flip_wide_shadow_v1",
        horizon=20,
    )
    assert result["grade"] == "RESEARCH"
    assert result["status"] == "COMPLETED"
    assert result["base_nav"] == 1_000_000.0
    assert result["base_nav_source"] == "config.initial_nav"
    assert [row["side"] for row in result["rows"]] == ["BUY"]
    assert result["rows"][0]["score"] == 88.0
    assert result["rows"][0]["stock_name"] == "平安银行"


def test_export_targets_fails_closed_without_a_run(monkeypatch):
    import app.jobs.strategy_runner as strategy_runner

    _patch_export_deps(monkeypatch, [])
    with pytest.raises(ValueError, match="no completed paper run"):
        strategy_runner.export_targets(
            date=datetime.datetime(2026, 9, 11, tzinfo=datetime.UTC),
            model_version="flip_wide_shadow_v1",
            horizon=20,
        )


def test_export_targets_fails_closed_on_a_non_completed_run(monkeypatch):
    import app.jobs.strategy_runner as strategy_runner

    _patch_export_deps(monkeypatch, [_ExportDoc(status="SKIPPED")])
    with pytest.raises(ValueError, match="SKIPPED"):
        strategy_runner.export_targets(
            date=datetime.datetime(2026, 9, 11, tzinfo=datetime.UTC),
            model_version="flip_wide_shadow_v1",
            horizon=20,
        )


def test_export_targets_fails_closed_on_ambiguous_config_hashes(monkeypatch):
    import app.jobs.strategy_runner as strategy_runner

    _patch_export_deps(
        monkeypatch,
        [_ExportDoc(config_hash="hash-a"), _ExportDoc(config_hash="hash-b")],
    )
    with pytest.raises(ValueError, match="ambiguous"):
        # No configuration named -> two COMPLETED configs cannot be told apart.
        strategy_runner.export_targets(
            date=datetime.datetime(2026, 9, 11, tzinfo=datetime.UTC),
            model_version="flip_wide_shadow_v1",
            horizon=20,
        )


def test_export_targets_requested_config_without_a_match_fails_closed(monkeypatch):
    """Regression: a named config that matches no run must not silently export
    a different configuration's run."""
    import app.jobs.strategy_runner as strategy_runner

    _patch_export_deps(monkeypatch, [_ExportDoc(config_hash="hash-a")])
    with pytest.raises(ValueError, match="no completed paper run"):
        strategy_runner.export_targets(
            date=datetime.datetime(2026, 9, 11, tzinfo=datetime.UTC),
            config={**_EXPORT_CFG, "initial_nav": 3_000_000.0},
        )


def test_export_targets_disambiguates_by_given_config(monkeypatch):
    import app.jobs.strategy_runner as strategy_runner
    from app.lib.strategy_engine.config import (
        strategy_config_hash,
        validate_strategy_config,
    )

    cfg = {**_EXPORT_CFG, "initial_nav": 2_000_000.0}
    wanted = strategy_config_hash(validate_strategy_config(cfg))
    _patch_export_deps(
        monkeypatch,
        [
            _ExportDoc(config_hash="hash-a"),
            _ExportDoc(config_hash=wanted, config={"initial_nav": 2_000_000.0}),
        ],
    )
    result = strategy_runner.export_targets(
        date=datetime.datetime(2026, 9, 11, tzinfo=datetime.UTC), config=cfg
    )
    assert result["config_hash"] == wanted
    # Amounts come from the matched run's persisted config, not the CLI input.
    assert result["base_nav"] == 2_000_000.0


def test_export_targets_queries_the_run_key(monkeypatch):
    """The candidate query must carry the full run key (a wrong filter would
    otherwise go undetected because the fake ignores kwargs)."""
    import app.jobs.strategy_runner as strategy_runner
    import app.model.scoring as model_scoring
    import app.model.strategy as model_strategy

    seen = {}

    class FakeRegModel:
        @classmethod
        def objects(cls, **query):
            return _FakeQS([_FakeRegistered({"20": {"directions": {}}})])

    class FakeRunModel:
        @classmethod
        def objects(cls, **query):
            seen.update(query)
            return _FakeQS([_ExportDoc()])

    monkeypatch.setattr(model_scoring, "ScoreModelVersion", FakeRegModel)
    monkeypatch.setattr(model_strategy, "StrategyPaperRun", FakeRunModel)
    monkeypatch.setattr(strategy_runner, "_query_export_scores", lambda d, h, mv: {})
    monkeypatch.setattr(strategy_runner, "_query_stock_names", lambda codes: {})

    strategy_runner.export_targets(
        date=datetime.datetime(2026, 9, 11, tzinfo=datetime.UTC),
        model_version="flip_wide_shadow_v1",
        horizon=20,
    )
    assert seen == {
        "strategy_name": "flip_wide_paper",
        "date": datetime.datetime(2026, 9, 11),
        "model_version": "flip_wide_shadow_v1",
        "horizon": 20,
        "evidence_kind__in": ["REPLAY", "FORWARD"],
    }


def test_query_export_scores_maps_predictions(monkeypatch):
    import app.jobs.strategy_runner as strategy_runner
    import app.model.scoring as model_scoring

    class Row:
        def __init__(self, code, score, percentile):
            self.stock_code, self.score, self.percentile = code, score, percentile

    class FakeQS:
        def only(self, *fields):
            assert set(fields) == {"stock_code", "score", "percentile"}
            return [Row("sz000001", 88.0, 0.04)]

    class FakePred:
        @classmethod
        def objects(cls, **query):
            assert query["model_version"] == "flip_wide_shadow_v1"
            assert query["horizon"] == 20
            return FakeQS()

    monkeypatch.setattr(model_scoring, "StockScorePrediction", FakePred)
    scores = strategy_runner._query_export_scores(
        datetime.datetime(2026, 9, 11), 20, "flip_wide_shadow_v1"
    )
    assert scores == {"sz000001": {"score": 88.0, "percentile": 0.04}}


def test_query_stock_names_skips_unnamed_and_empty_codes(monkeypatch):
    import app.jobs.strategy_runner as strategy_runner
    import app.model.stock as model_stock

    class Stock:
        def __init__(self, code, name):
            self.code, self.name = code, name

    class FakeQS:
        def only(self, *fields):
            assert set(fields) == {"code", "name"}
            return [Stock("sz000001", "平安银行"), Stock("sh600000", None)]

    class FakeStock:
        @classmethod
        def objects(cls, **query):
            assert query["code__in"] == ["sz000001", "sh600000"]
            return FakeQS()

    monkeypatch.setattr(model_stock, "IndividualStock", FakeStock)
    assert strategy_runner._query_stock_names([]) == {}
    names = strategy_runner._query_stock_names(["sz000001", "sh600000"])
    assert names == {"sz000001": "平安银行"}


# ---------------------------------------------------------------------------
# export CLI contract (fail-closed exit, artifact labelling, no job run)
# ---------------------------------------------------------------------------

_MIN_EXPORT = {
    "strategy_name": "flip_wide_paper",
    "model_version": "flip_wide_shadow_v1",
    "horizon": 20,
    "config_hash": "deadbeef",
    "date": "2026-09-11",
    "execution_date": "2026-09-14T00:00:00",
    "decision_at": "2026-09-11T10:40:00+00:00",
    "evidence_kind": "REPLAY",
    "status": "COMPLETED",
    "base_nav": 1_000_000.0,
    "base_nav_source": "config.initial_nav",
    "grade": "RESEARCH",
    "disclaimer": "Research / learning / demonstration MVP. Not investment advice.",
    "generated_at": "2026-09-12T00:00:00+00:00",
    "counts": {"buy": 0, "sell": 0, "hold": 0, "total": 0},
    "rows": [],
}


def _patch_export_main(monkeypatch, replacement):
    import app.jobs.strategy_runner as strategy_runner

    monkeypatch.setattr(strategy_runner, "_init_db", lambda: None)
    monkeypatch.setattr(strategy_runner, "export_targets", replacement)
    return strategy_runner


def test_export_main_exits_non_zero_and_prints_no_rows(monkeypatch, capsys):
    def boom(**kwargs):
        raise ValueError("no completed paper run to export for 2026-09-11")

    strategy_runner = _patch_export_main(monkeypatch, boom)
    with pytest.raises(SystemExit) as excinfo:
        strategy_runner.main(["export", "--date", "2026-09-11"])
    assert excinfo.value.code == 1
    captured = capsys.readouterr()
    assert captured.out == ""
    assert "no completed paper run" in captured.err


def test_export_main_rejects_malformed_config_json(monkeypatch, capsys):
    """Malformed --config-json must give the structured error, not a traceback."""
    _patch_export_main(monkeypatch, lambda **kwargs: dict(_MIN_EXPORT))
    import app.jobs.strategy_runner as strategy_runner

    with pytest.raises(SystemExit) as excinfo:
        strategy_runner.main(
            ["export", "--date", "2026-09-11", "--config-json", "{nope"]
        )
    assert excinfo.value.code == 1
    captured = capsys.readouterr()
    assert captured.out == ""
    assert captured.err.strip().startswith("{")
    assert "Traceback" not in captured.err


def test_export_main_never_records_a_job_run(monkeypatch, capsys):
    import app.jobs.strategy_runner as strategy_runner
    from app.lib.utilities import job_run_helper

    _patch_export_main(monkeypatch, lambda **kwargs: dict(_MIN_EXPORT))

    def forbidden(*args, **kwargs):  # pragma: no cover - must never be called
        raise AssertionError("export must not create a job-run record")

    monkeypatch.setattr(job_run_helper, "create_job_run", forbidden)
    strategy_runner.main(["export", "--date", "2026-09-11"])
    assert "side,stock_code" in capsys.readouterr().out


def test_export_main_json_carries_grade_and_disclaimer(monkeypatch, capsys):
    import json

    _patch_export_main(monkeypatch, lambda **kwargs: dict(_MIN_EXPORT))
    import app.jobs.strategy_runner as strategy_runner

    strategy_runner.main(["export", "--date", "2026-09-11", "--format", "json"])
    payload = json.loads(capsys.readouterr().out)
    assert payload["grade"] == "RESEARCH"
    assert "not investment advice" in payload["disclaimer"].lower()


def test_export_main_writes_csv_artifact_with_compliance_comment(
    monkeypatch, capsys, tmp_path
):
    _patch_export_main(monkeypatch, lambda **kwargs: dict(_MIN_EXPORT))
    import app.jobs.strategy_runner as strategy_runner

    out_file = tmp_path / "target.csv"
    strategy_runner.main(["export", "--date", "2026-09-11", "--output", str(out_file)])
    text = out_file.read_text(encoding="utf-8")
    lines = text.splitlines()
    assert lines[0].startswith("# grade=RESEARCH")
    assert _MIN_EXPORT["disclaimer"] in lines[0]
    assert lines[1] == (
        "side,stock_code,stock_name,score,percentile,"
        "target_weight,target_amount_cny,reason"
    )
    # The metadata block still goes to stderr, not into the artifact.
    stderr = capsys.readouterr().err
    assert stderr.strip().startswith("{")
    assert "strategy_name" in stderr
