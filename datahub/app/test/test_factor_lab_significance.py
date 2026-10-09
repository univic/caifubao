# -*- coding: utf-8 -*-
"""Deterministic hypothesis-family checks; no database or live market data."""

import math

import pytest

from app.lib.factor_lab.significance import annotate_multiple_testing


def _report(stats):
    return {
        "factors": {
            name: {
                "horizons": {
                    horizon: {
                        "ic": {"t_stat_nw": t, "n_dates": n},
                        "gates": {"passed": True, "failures": []},
                    }
                    for horizon, (t, n) in horizons.items()
                }
            }
            for name, horizons in stats.items()
        }
    }


def test_bonferroni_counts_all_attempted_pairs_and_dilutes_nominal_signal():
    original = _report(
        {"a": {"5": (2.0, 180), "20": (None, 180)},
         "b": {"5": (-4.0, 180), "20": (0.0, 180)}}
    )
    result = annotate_multiple_testing(original)
    meta = result["multiple_testing"]
    assert meta["evaluated_combinations"] == 4
    assert meta["family_size"] == 4
    assert meta["family_source"] == "evaluated_only"
    assert meta["scope"] == "exploratory_full_sample"
    a = result["factors"]["a"]["horizons"]["5"]["significance"]
    assert a["p_two_sided_nw_normal"] == pytest.approx(math.erfc(2 / math.sqrt(2)))
    assert a["p_bonferroni"] == pytest.approx(4 * a["p_two_sided_nw_normal"])
    assert a["status"] == "not_significant"
    b = result["factors"]["b"]["horizons"]["5"]["significance"]
    assert b["status"] == "significant"
    assert b["reject_null"] is True
    assert meta["significant_combinations"] == 1
    assert result["factors"]["a"]["horizons"]["5"]["gates"]["passed"] is True
    assert "significance" not in original["factors"]["a"]["horizons"]["5"]


def test_missing_nonfinite_and_small_sample_cannot_pass():
    result = annotate_multiple_testing(
        _report({"a": {
            "1": (None, 200),
            "5": (float("inf"), 200),
            "20": (20.0, 30),
            "60": (0.0, 200),
        }})
    )
    entries = result["factors"]["a"]["horizons"]
    assert entries["1"]["significance"]["status"] == "unavailable_nw_t"
    assert entries["1"]["significance"]["p_bonferroni"] is None
    assert entries["5"]["significance"]["status"] == "unavailable_nw_t"
    assert entries["20"]["significance"]["status"] == "insufficient_dates"
    assert entries["20"]["significance"]["reject_null"] is False
    assert entries["60"]["significance"]["p_bonferroni"] == 1
    assert entries["60"]["significance"]["status"] == "not_significant"


def test_explicit_search_family_includes_prior_hypotheses():
    original = _report({"a": {"20": (2.0, 150)}})
    baseline = annotate_multiple_testing(original)
    declared = annotate_multiple_testing(original, hypotheses_count=51)
    a = baseline["factors"]["a"]["horizons"]["20"]["significance"]
    b = declared["factors"]["a"]["horizons"]["20"]["significance"]
    assert a["reject_null"] is True
    assert b["reject_null"] is False
    assert b["p_bonferroni"] == 1
    assert declared["multiple_testing"]["family_source"] == "declared"


@pytest.mark.parametrize("alpha", [0, 1, -0.2, float("nan"), float("inf")])
def test_invalid_alpha_is_rejected(alpha):
    with pytest.raises(ValueError, match="--alpha"):
        annotate_multiple_testing(_report({"a": {"1": (3, 200)}}), alpha=alpha)


@pytest.mark.parametrize("count", [0, 1, -1, 1.0, True])
def test_invalid_family_size_is_rejected(count):
    with pytest.raises(ValueError, match="--hypotheses-count"):
        annotate_multiple_testing(
            _report({"a": {"1": (3, 200), "5": (4, 200)}}),
            hypotheses_count=count,
        )


def test_empty_sweep_is_rejected():
    with pytest.raises(ValueError, match="no factor/horizon"):
        annotate_multiple_testing({"factors": {}})


def test_two_sided_test_is_symmetric_and_alpha_configurable():
    result = annotate_multiple_testing(
        _report({"a": {"5": (2.5, 150)}, "b": {"5": (-2.5, 150)}}),
        alpha=0.01,
    )
    a = result["factors"]["a"]["horizons"]["5"]["significance"]
    b = result["factors"]["b"]["horizons"]["5"]["significance"]
    assert a == b
    assert a["reject_null"] is False
    assert result["multiple_testing"]["alpha"] == 0.01
