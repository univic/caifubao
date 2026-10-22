# -*- coding: utf-8 -*-
from __future__ import annotations

import pandas as pd
import pytest

from app.lib.prices import (
    CorporateAction,
    build_return_index,
    event_factor,
    normalize_action,
)


def _raw(closes: list[float], dates: list[str] | None = None) -> pd.DataFrame:
    dates = dates or [f"2020-01-{i + 2:02d}" for i in range(len(closes))]
    return pd.DataFrame(
        {
            "open": closes,
            "close": closes,
            "high": [c * 1.01 for c in closes],
            "low": [c * 0.99 for c in closes],
            "volume": [1000] * len(closes),
        },
        index=pd.to_datetime(dates),
    )


def _action(**kwargs) -> CorporateAction:
    payload = {
        "code": "000001",
        "ex_date": pd.Timestamp("2020-01-03"),
        "action_type": "bonus",
    }
    payload.update(kwargs)
    return CorporateAction(**payload)


def test_bonus_keeps_value_flat():
    raw = _raw([10.0, 5.0])
    action = _action(share_ratio=1.0)
    adjusted, violations = build_return_index(raw, [action], code="000001")
    assert violations == []
    assert adjusted["return_index"].iloc[-1] == pytest.approx(1.0, abs=1e-12)


def test_cash_dividend_keeps_value_flat():
    raw = _raw([10.0, 9.5])
    action = _action(action_type="cash", cash_per_share=0.5)
    adjusted, violations = build_return_index(raw, [action], code="000001")
    assert violations == []
    assert adjusted["return_index"].iloc[-1] == pytest.approx(1.0, abs=1e-12)


def test_rights_subscription_is_a_cash_outflow():
    raw = _raw([10.0, 9.0])
    action = _action(action_type="rights", rights_ratio=0.3, rights_price=5.0)
    adjusted, violations = build_return_index(raw, [action], code="000001")
    # value = 1.3*9 - 0.3*5 = 10.2 -> +2%
    assert violations == []
    assert adjusted["return_index"].iloc[-1] == pytest.approx(1.02, abs=1e-12)


def test_share_reform_uses_share_ratio():
    raw = _raw([10.0, 7.5])
    action = _action(action_type="share_reform", share_ratio=0.33)
    adjusted, violations = build_return_index(raw, [action], code="000001")
    assert violations == []
    assert adjusted["return_index"].iloc[-1] == pytest.approx(
        1.33 * 7.5 / 10.0, abs=1e-12
    )


def test_non_event_return_matches_raw_return():
    raw = _raw([10.0, 10.5, 11.0, 10.8])
    action = _action(
        ex_date=pd.Timestamp("2020-01-04"), share_ratio=0.1, cash_per_share=0.2
    )
    adjusted, violations = build_return_index(raw, [action], code="000001")
    assert violations == []
    raw_ret = raw["close"].pct_change(fill_method=None).fillna(0.0)
    idx_ret = adjusted["return_index"].pct_change(fill_method=None).fillna(0.0)
    for i in (1, 3):
        assert idx_ret.iloc[i] == pytest.approx(raw_ret.iloc[i], abs=1e-12)


def test_factor_is_constant_between_events():
    raw = _raw([10.0, 10.1, 10.2, 5.05, 5.1])
    action = _action(ex_date=pd.Timestamp("2020-01-05"), share_ratio=1.0)
    adjusted, _ = build_return_index(raw, [action], code="000001")
    factors = adjusted["adj_factor"].to_numpy()
    assert factors[0] == pytest.approx(factors[1], rel=1e-12)
    assert factors[1] == pytest.approx(factors[2], rel=1e-12)
    assert factors[3] == pytest.approx(factors[4], rel=1e-12)
    assert factors[3] > factors[2]


def test_limit_breach_is_reported():
    raw = _raw([10.0, 12.6])  # +26% on the main board
    adjusted, violations = build_return_index(raw, [], code="000001")
    assert any(v.rule == "daily_limit" for v in violations)


def test_negative_raw_price_is_rejected():
    raw = _raw([10.0, -1.0])
    with pytest.raises(ValueError):
        build_return_index(raw, [], code="000001")


def test_missing_event_date_is_reported():
    raw = _raw([10.0, 10.1])
    action = _action(ex_date=pd.Timestamp("2020-02-01"), share_ratio=1.0)
    _, violations = build_return_index(raw, [action], code="000001")
    assert any(v.rule == "event_date_missing" for v in violations)


def test_invalid_rights_event_is_rejected():
    with pytest.raises(ValueError):
        _action(action_type="rights", rights_ratio=0.0, rights_price=5.0)


def test_normalize_rejects_announcement_after_ex_date():
    with pytest.raises(ValueError):
        normalize_action(
            {
                "code": "000001",
                "ex_date": "2020-01-03",
                "action_type": "cash",
                "share_ratio": 0,
                "cash_per_share": 0.1,
                "rights_ratio": 0,
                "rights_price": 0,
                "announced_at": "2020-01-05",
                "source": "test",
            }
        )


def test_event_factor_is_share_multiplier():
    action = _action(action_type="transfer", share_ratio=1.0)
    assert event_factor(action) == pytest.approx(2.0)
