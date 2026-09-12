# -*- coding: utf-8 -*-
"""Tests for strategy portfolio risk limits (roadmap 1.3).

Pure selection/eligibility logic plus the runner data wiring. Kept in its own
file (rather than appended to test_strategy_engine.py / test_strategy_runner.py)
so concurrent slices do not collide at the same file tail.
"""

import datetime

import pytest

from app.lib.strategy_engine.config import (
    DEFAULT_STRATEGY_CONFIG,
    strategy_config_hash,
    validate_strategy_config,
)
from app.lib.strategy_engine.runner import (
    assemble_daily_plan,
    eligible_codes_from_flags,
)
from app.lib.strategy_engine.selection import select_target_holdings

UNKNOWN = "UNKNOWN"


class _Pred:
    def __init__(self, stock_code, score, percentile=None):
        self.stock_code = stock_code
        self.score = score
        self.percentile = percentile if percentile is not None else 0.9


def _cfg(size=4, mode="top_n", **constraints):
    """A hermetic config: the single-position cap defaults to non-binding so a
    test exercises only the limit it names. The shipped default (0.05) is
    covered by its own test below."""
    cfg = {
        "score_model_version": "v1",
        "selection": {"mode": mode, "portfolio_size": size},
        "constraints": {"max_single_stock_pct": 1.0},
    }
    if mode == "top_percentile":
        cfg["selection"].update({"lower": 0.0, "upper": 1.0})
    cfg["constraints"].update(constraints)
    return cfg


def _weights(holdings):
    return {h["stock_code"]: h["weight"] for h in holdings}


def _flags(**per_code):
    """Build a flags map: _flags(sz000001=dict(trade_amount=1e9), ...)."""
    return {code.replace("_", ""): row for code, row in per_code.items()}


# ---------------------------------------------------------------------------
# single-position cap
# ---------------------------------------------------------------------------


def test_equal_weight_at_or_below_the_cap_is_unchanged():
    preds = [_Pred(f"sz00000{i}", 90 - i) for i in range(4)]
    holdings = select_target_holdings(preds, _cfg(max_single_stock_pct=0.5))
    assert _weights(holdings) == {
        "sz000000": 0.25,
        "sz000001": 0.25,
        "sz000002": 0.25,
        "sz000003": 0.25,
    }


def test_single_position_cap_scales_weights_down_and_keeps_cash():
    preds = [_Pred(f"sz00000{i}", 90 - i) for i in range(4)]
    holdings = select_target_holdings(preds, _cfg(max_single_stock_pct=0.1))
    weights = _weights(holdings)
    # Every weight is capped; no name is dropped for this reason.
    assert len(weights) == 4
    assert set(weights.values()) == {0.1}
    assert sum(weights.values()) == pytest.approx(0.4)  # 0.6 stays in cash


def test_single_position_cap_never_scales_a_name_up():
    preds = [_Pred("sz000001", 90), _Pred("sz000002", 80)]
    holdings = select_target_holdings(preds, _cfg(size=2, max_single_stock_pct=0.9))
    # Equal weight 0.5 is below the cap, so the cap must not raise it.
    assert _weights(holdings) == {"sz000001": 0.5, "sz000002": 0.5}


# ---------------------------------------------------------------------------
# industry concentration cap
# ---------------------------------------------------------------------------


def test_industry_cap_scales_the_over_weighted_industry_down():
    preds = [_Pred(f"sz00000{i}", 90 - i) for i in range(4)]
    industry = {
        "sz000000": "BANK",
        "sz000001": "BANK",
        "sz000002": "BANK",
        "sz000003": "TECH",
    }
    holdings = select_target_holdings(
        preds, _cfg(max_industry_pct=0.4), industry_by_code=industry
    )
    weights = _weights(holdings)
    bank = weights["sz000000"] + weights["sz000001"] + weights["sz000002"]
    assert bank == pytest.approx(0.4, abs=1e-6)
    assert weights["sz000003"] == weights["sz000000"]


def test_missing_classification_forms_its_own_capped_bucket():
    preds = [_Pred(f"sz00000{i}", 90 - i) for i in range(2)]
    # No industry entries at all -> both names land in the UNKNOWN bucket.
    holdings = select_target_holdings(
        preds, _cfg(size=2, max_industry_pct=0.3), industry_by_code={}
    )
    weights = _weights(holdings)
    assert sum(weights.values()) == pytest.approx(0.3, abs=1e-6)


def test_no_industry_cap_leaves_weighting_untouched():
    preds = [_Pred(f"sz00000{i}", 90 - i) for i in range(4)]
    industry = {f"sz00000{i}": "BANK" for i in range(4)}
    holdings = select_target_holdings(preds, _cfg(), industry_by_code=industry)
    assert set(_weights(holdings).values()) == {0.25}


def test_industry_cap_without_industry_data_fails_closed():
    preds = [_Pred("sz000001", 90), _Pred("sz000002", 80)]
    with pytest.raises(ValueError, match="industry"):
        select_target_holdings(preds, _cfg(size=2, max_industry_pct=0.3))


def test_single_and_industry_caps_take_the_stricter_one():
    preds = [_Pred(f"sz00000{i}", 90 - i) for i in range(4)]
    industry = {f"sz00000{i}": "BANK" for i in range(4)}
    holdings = select_target_holdings(
        preds,
        _cfg(max_single_stock_pct=0.2, max_industry_pct=0.4),
        industry_by_code=industry,
    )
    # Industry cap binds: 0.4 / 4 = 0.1, tighter than the 0.2 single cap.
    assert set(_weights(holdings).values()) == {0.1}


# ---------------------------------------------------------------------------
# liquidity floor
# ---------------------------------------------------------------------------


def test_illiquid_name_is_excluded():
    flags = {
        "sz000001": {"trade_status": 1, "trade_amount": 5_000_000.0},
        "sz000002": {"trade_status": 1, "trade_amount": 1_000.0},
    }
    eligible = eligible_codes_from_flags(flags, _cfg(min_trade_amount_cny=1_000_000))
    assert eligible == {"sz000001"}


def test_unknown_traded_amount_is_excluded():
    flags = {"sz000001": {"trade_status": 1}}  # no trade_amount evidence
    eligible = eligible_codes_from_flags(flags, _cfg(min_trade_amount_cny=1_000_000))
    assert eligible == set()


def test_zero_liquidity_floor_constrains_nothing():
    flags = {
        "sz000001": {"trade_status": 1},
        "sz000002": {"trade_status": 1, "trade_amount": 0.0},
    }
    eligible = eligible_codes_from_flags(flags, _cfg(min_trade_amount_cny=0))
    assert eligible == {"sz000001", "sz000002"}


def test_liquidity_floor_excludes_the_name_from_the_plan():
    flags = {
        "sz000001": {"trade_status": 1, "trade_amount": 5_000_000.0},
        "sz000002": {"trade_status": 1, "trade_amount": 1_000.0},
    }
    plan = assemble_daily_plan(
        config=_cfg(size=2, min_trade_amount_cny=1_000_000),
        date=None,
        predictions=[_Pred("sz000001", 90), _Pred("sz000002", 80)],
        previous_holdings=None,
        flags=flags,
    )
    assert plan["skipped"] is False
    assert [h["stock_code"] for h in plan["target_holdings"]] == ["sz000001"]


# ---------------------------------------------------------------------------
# config hashing / validation
# ---------------------------------------------------------------------------


def test_risk_limits_change_the_config_hash():
    base = validate_strategy_config(_cfg())
    changed_single = validate_strategy_config(_cfg(max_single_stock_pct=0.2))
    changed_industry = validate_strategy_config(_cfg(max_industry_pct=0.2))
    changed_liquidity = validate_strategy_config(_cfg(min_trade_amount_cny=1_000_000))
    hashes = {
        strategy_config_hash(cfg)
        for cfg in (base, changed_single, changed_industry, changed_liquidity)
    }
    assert len(hashes) == 4


@pytest.mark.parametrize("bad", [0, 1.5, "0.2", True])
def test_invalid_industry_cap_is_rejected(bad):
    with pytest.raises(ValueError, match="max_industry_pct"):
        validate_strategy_config(_cfg(max_industry_pct=bad))


def test_default_config_has_no_industry_cap():
    cfg = validate_strategy_config(DEFAULT_STRATEGY_CONFIG)
    assert cfg["constraints"].get("max_industry_pct") is None


def test_shipped_default_single_position_cap_is_actually_enforced():
    """Regression for the motivating bug: the default declared 0.05 per-name cap
    used to be ignored. A book narrow enough for it to bind must now be capped."""
    cfg = validate_strategy_config(DEFAULT_STRATEGY_CONFIG)
    assert cfg["constraints"]["max_single_stock_pct"] == 0.05
    preds = [_Pred(f"sz00000{i}", 90 - i) for i in range(4)]
    holdings = select_target_holdings(preds, cfg)
    assert len(holdings) == 4
    assert set(_weights(holdings).values()) == {0.05}


# ---------------------------------------------------------------------------
# runner data wiring (trade_amount + industry map)
# ---------------------------------------------------------------------------


def test_query_flags_carries_traded_amount(monkeypatch):
    import datetime

    import app.jobs.strategy_runner as strategy_runner
    import app.model.scoring as model_scoring
    import app.model.stock as model_stock

    class Pred:
        def __init__(self, code):
            self.stock_code = code

    class PredQS:
        def only(self, *fields):
            return [Pred("sz000001")]

    class FakePred:
        @classmethod
        def objects(cls, **query):
            return PredQS()

    class Quote:
        code = "sz000001"
        isST = 0
        trade_status = 1
        trade_amount = 8_888_888.0
        date = datetime.datetime(2026, 9, 11)

    class FakeQuote:
        @classmethod
        def objects(cls, **query):
            return [Quote()]

    monkeypatch.setattr(model_scoring, "StockScorePrediction", FakePred)
    monkeypatch.setattr(model_stock, "StockDailyQuote", FakeQuote)

    flags = strategy_runner._query_flags(datetime.datetime(2026, 9, 11), 20)
    assert flags["sz000001"]["trade_amount"] == 8_888_888.0


def test_query_industry_map_uses_only_classifications_in_effect_on_signal_date(
    monkeypatch,
):
    import app.jobs.strategy_runner as strategy_runner
    import app.model.industry as model_industry

    class Row:
        def __init__(self, code, industry, assigned_at, change_log=None):
            self.stock_code = code
            self.industry_code_sw_l1 = industry
            self.assigned_at = assigned_at
            self.industry_change_log = change_log or []

    class FakeQS:
        def only(self, *fields):
            assert set(fields) == {
                "stock_code",
                "industry_code_sw_l1",
                "assigned_at",
                "industry_change_log",
            }
            return [
                # created before the signal date, never changed -> usable
                Row("sz000001", "BANK", datetime.datetime(2026, 1, 5)),
                # created after the signal date -> not usable
                Row("sz000002", "TECH", datetime.datetime(2026, 12, 1)),
                # assigned_at unknown -> not provable
                Row("sz000003", "BANK", None),
                # no classification at all
                Row("sz000004", None, datetime.datetime(2026, 1, 5)),
                # created before the signal date BUT changed after it: the
                # writer does not move assigned_at, so the CURRENT code is the
                # later one and must not be back-dated onto this signal date.
                Row(
                    "sz000005",
                    "TECH",
                    datetime.datetime(2026, 1, 5),
                    [
                        {
                            "timestamp": "2026-01-05T00:00:00+00:00",
                            "previous_l1": "BANK",
                        },
                        {
                            "timestamp": "2026-11-02T00:00:00+00:00",
                            "previous_l1": "TECH",
                        },
                    ],
                ),
                # a change entry whose date cannot be read -> fail closed
                Row(
                    "sz000006",
                    "BANK",
                    datetime.datetime(2026, 1, 5),
                    [{"previous_l1": "TECH"}],
                ),
                # only changes before the signal date -> usable
                Row(
                    "sz000007",
                    "BANK",
                    datetime.datetime(2026, 1, 5),
                    [{"timestamp": "2026-03-02T00:00:00+00:00"}],
                ),
            ]

    class FakeClass:
        @classmethod
        def objects(cls, **query):
            assert query["stock_code__in"] == [
                "sz000001",
                "sz000002",
                "sz000003",
                "sz000004",
                "sz000005",
                "sz000006",
                "sz000007",
            ]
            return FakeQS()

    monkeypatch.setattr(model_industry, "StockIndustryClassification", FakeClass)
    signal_date = datetime.datetime(2026, 9, 11)
    assert strategy_runner._query_industry_map([], signal_date) == {}
    codes = [f"sz{i:06d}" for i in range(1, 8)]
    assert strategy_runner._query_industry_map(codes, signal_date) == {
        "sz000001": "BANK",
        "sz000007": "BANK",
    }


def test_classification_in_effect_on_handles_mixed_naive_and_aware_timestamps():
    import datetime

    from app.jobs.strategy_runner import _classification_in_effect_on

    class Row:
        def __init__(self, assigned_at, change_log=None):
            self.assigned_at = assigned_at
            self.industry_change_log = change_log or []

    aware = datetime.datetime(2026, 1, 5, 3, 0, tzinfo=datetime.UTC)
    naive_signal = datetime.datetime(2026, 1, 5)
    assert _classification_in_effect_on(Row(aware), naive_signal) is True
    assert (
        _classification_in_effect_on(Row(datetime.datetime(2026, 1, 6)), naive_signal)
        is False
    )
    assert _classification_in_effect_on(Row(None), naive_signal) is False
    assert _classification_in_effect_on(Row("not-a-date"), naive_signal) is False


# ---------------------------------------------------------------------------
# eligibility constraints must not be silently bypassed
# ---------------------------------------------------------------------------


def test_plan_without_flags_fails_closed_when_a_constraint_is_active():
    with pytest.raises(ValueError, match="universe flag map"):
        assemble_daily_plan(
            config=_cfg(size=2, min_trade_amount_cny=1_000_000),
            date=None,
            predictions=[_Pred("sz000001", 90)],
            previous_holdings=None,
            flags=None,
        )


def test_plan_without_flags_fails_closed_for_an_active_exclusion_flag():
    with pytest.raises(ValueError, match="universe flag map"):
        assemble_daily_plan(
            config=_cfg(size=2, exclude_st=True, max_single_stock_pct=1.0),
            date=None,
            predictions=[_Pred("sz000001", 90)],
            previous_holdings=None,
            flags=None,
        )


def test_plan_without_flags_is_allowed_when_no_constraint_is_active():
    plan = assemble_daily_plan(
        config=_cfg(
            size=2,
            exclude_st=False,
            exclude_bse=False,
            exclude_suspended=False,
            min_trade_amount_cny=0.0,
        ),
        date=None,
        predictions=[_Pred("sz000001", 90)],
        previous_holdings=None,
        flags=None,
    )
    assert plan["skipped"] is False
    assert [h["stock_code"] for h in plan["target_holdings"]] == ["sz000001"]


def test_no_predictions_still_skips_without_a_flag_map():
    plan = assemble_daily_plan(
        config=_cfg(size=2, min_trade_amount_cny=1_000_000),
        date=datetime.datetime(2026, 9, 11),
        predictions=[],
        previous_holdings=None,
        flags=None,
    )
    assert plan["skipped"] is True
