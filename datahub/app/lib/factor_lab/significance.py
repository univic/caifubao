# -*- coding: utf-8 -*-
"""Exploratory family-wise significance for a frozen factor-lab sweep.

The IC statistic is computed elsewhere. This module only annotates its
Newey-West t-statistic, using a *two-sided normal approximation*, not an
independent-sample t distribution. It does not modify the economic gates.
"""

import copy
import math

from app.lib.factor_lab.metrics import MIN_DATES


def annotate_multiple_testing(
    report: dict, *, alpha: float = 0.05, hypotheses_count: int | None = None
) -> dict:
    """Attach Bonferroni evidence for the complete *declared* search family.

    If no history count is declared, the evaluated combinations are the family.
    This must not be interpreted as accounting for earlier factor searches,
    parameter sweeps or repeated attempts on the same historical panel.
    """
    if not math.isfinite(alpha) or not 0 < alpha < 1:
        raise ValueError("--alpha must be finite and strictly between 0 and 1")

    output = copy.deepcopy(report)
    pairs = [
        stats
        for factor in output["factors"].values()
        for stats in factor["horizons"].values()
    ]
    attempted = len(pairs)
    if attempted == 0:
        raise ValueError("no factor/horizon combinations to test")
    if hypotheses_count is not None:
        if (
            isinstance(hypotheses_count, bool)
            or not isinstance(hypotheses_count, int)
            or hypotheses_count < attempted
        ):
            raise ValueError(
                "--hypotheses-count must be an integer >= evaluated combinations "
                f"({attempted})"
            )
    family_size = hypotheses_count if hypotheses_count is not None else attempted
    significant = 0
    for stats in pairs:
        ic = stats["ic"]
        dates = ic.get("n_dates") or 0
        t = ic.get("t_stat_nw")
        p = None
        if isinstance(t, (int, float)) and not isinstance(t, bool):
            if math.isfinite(t):
                p = math.erfc(abs(t) / math.sqrt(2))
        adjusted = min(1.0, p * family_size) if p is not None else None
        if dates < MIN_DATES:
            status = "insufficient_dates"
        elif p is None:
            status = "unavailable_nw_t"
        elif adjusted <= alpha:
            status = "significant"
            significant += 1
        else:
            status = "not_significant"
        stats["significance"] = {
            "p_two_sided_nw_normal": p,
            "p_bonferroni": adjusted,
            "reject_null": status == "significant",
            "status": status,
        }
    output["multiple_testing"] = {
        "method": "bonferroni",
        "p_value_method": "two_sided_normal_approximation_of_nw_t",
        "scope": "exploratory_full_sample",
        "alpha": alpha,
        "evaluated_combinations": attempted,
        "family_size": family_size,
        "family_source": (
            "declared" if hypotheses_count is not None else "evaluated_only"
        ),
        "minimum_ic_dates": MIN_DATES,
        "significant_combinations": significant,
        "warning": (
            "Historical exploration, earlier parameter searches and repeated "
            "runs are not counted unless supplied with --hypotheses-count. "
            "Statistical significance does not imply OOS or net trading edge."
        ),
    }
    return output
