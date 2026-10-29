# -*- coding: utf-8 -*-
"""Version-dispatched field semantics for the strategy declaration.

Why this exists
---------------
Hashing a declaration is not the same as preserving its behaviour. ``P0`` wires
new consumers (``rebalance.cadence_days``, ``risk.*``, ``sizing.weighting``); if
those consumers read a v1 declaration, the certified window changes what it does
while ``spec_hash`` stays byte-identical. The five-session regression in
``test_strategy_v1_regression.py`` catches that for the book, and this module
catches it at the declaration level: the set of fields a version consumes is
pinned, and a consumer must ask this module instead of inspecting the field.

Dispatch is by ``spec_version`` only. Selecting behaviour by whether a field is
present would make two declarations with the same bytes behave differently
depending on who wrote them.

This module is pure: no datahub, Mongo or broker imports.
"""

from __future__ import annotations

from caifubao_contracts.strategy_spec_consumers import CONSUMED, UNCONSUMED

#: Fields a ``spec_version: "1"`` declaration actually consumes, frozen at the
#: moment v2 work started. ``rebalance.cadence_days``, ``sizing.weighting`` and
#: the whole ``risk`` block are deliberately absent: v1 validates and hashes
#: them but no consumer reads them, and wiring one without a version bump would
#: silently change the certified window ``01c196b8…``.
V1_CONSUMED = frozenset(
    {
        "spec_version",
        "initial_nav",
        "universe.market",
        "universe.as_of",
        "signal.score_model_version",
        "signal.horizon",
        "selection.mode",
        "selection.lower",
        "selection.upper",
        "selection.portfolio_size",
        "sizing.cash_reserve_pct",
        "constraints.exclude_st",
        "constraints.exclude_bse",
        "constraints.exclude_suspended",
        "constraints.exclude_infeasible_buys",
        "constraints.max_single_stock_pct",
        "constraints.max_industry_pct",
        "constraints.min_trade_amount_cny",
        "rebalance.exit_buffer",
        "rebalance.min_hold_sessions",
        "execution.commission_rate",
        "execution.minimum_commission_cny",
        "execution.sell_stamp_duty_rate",
        "execution.transfer_fee_rate",
        "execution.slippage_per_side",
        "execution.board_lot",
        "execution.unknown_limit_policy",
        "evidence.timing_version",
        "evidence.policy",
    }
)

#: Fields v1 validates and hashes but never reads. Kept explicit so a test can
#: assert they stay unconsumed for v1.
V1_DECLARED_BUT_UNUSED = frozenset({"sizing.weighting"}) | frozenset(
    {
        "risk.max_turnover_pct",
        "risk.max_positions",
        "risk.cash_floor_pct",
        "risk.max_drawdown_pct",
        "risk.max_single_stock_pct",
        "risk.max_gross_exposure_pct",
        "risk.regime_policy",
    }
)

SUPPORTED_VERSIONS = frozenset({"1", "2"})


def consumed_by_version(manifest: dict, version: str) -> frozenset[str]:
    """Fields a version consumes, read from the manifest's version dimension.

    This is the *declared* answer used by the gate; ``consumed_fields`` returns
    the v1 snapshot so wiring cannot silently follow a manifest edit.
    """
    fields = manifest.get("fields") or {}
    return frozenset(
        path
        for path, entry in fields.items()
        if entry.get("status") == CONSUMED
        and version in (entry.get("consumed_by_versions") or [])
    )


def consumed_fields(spec_version: str, manifest: dict) -> frozenset[str]:
    """Return the fields a declaration of this version is allowed to consume.

    v1 returns the frozen snapshot and never looks at the manifest, so a newly
    wired consumer cannot change v1 behaviour without failing the pin test.
    v2 consumes everything declared, so an unconsumed field is an error.
    """
    if spec_version == "1":
        return V1_CONSUMED
    if spec_version == "2":
        fields = manifest.get("fields") or {}
        missing = sorted(
            path
            for path, entry in fields.items()
            if entry.get("status") != CONSUMED
            or "2" not in (entry.get("consumed_by_versions") or [])
        )
        if missing:
            raise ValueError(
                "spec_version 2 forbids fields no v2 consumer reads: "
                + ", ".join(missing)
            )
        return frozenset(fields)
    raise ValueError(
        f"unsupported spec_version {spec_version!r}; supported: "
        f"{sorted(SUPPORTED_VERSIONS)}"
    )


def field_is_consumed(spec_version: str, path: str, manifest: dict) -> bool:
    """Whether a consumer of this version may act on `path`.

    A consumer that needs a field which the version does not consume MUST skip
    it rather than read it, so v1 keeps re-planning every session instead of
    silently honouring a five-session cadence.
    """
    return path in consumed_fields(spec_version, manifest)


def declared_but_unused(manifest: dict) -> frozenset[str]:
    """Manifest fields with no consumer, regardless of version."""
    fields = manifest.get("fields") or {}
    return frozenset(
        path for path, entry in fields.items() if entry.get("status") == UNCONSUMED
    )
