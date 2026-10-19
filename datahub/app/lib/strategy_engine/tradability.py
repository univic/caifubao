# -*- coding: utf-8 -*-
"""Shared session-tradability rule for the production (paper/live-loop) path.

One place decides whether an order could fill at a session's open, so the NAV
engine and the daily runner cannot disagree:

* ``trade_status`` must be exactly 1 (0 / missing / unparsable = suspended);
* the open must be a finite price above zero;
* a BUY is refused while the session is at its upper price limit, a SELL while
  it is at its lower limit;
* an UNKNOWN limit flag is fail-closed for the side it would block — a session
  whose limit verdict cannot be derived is not treated as tradable (fail loud,
  never fail open).

The limit verdict is derived from ``previous_close`` and the code's board limit,
reusing ``factor_lab.panel.price_limit`` (main ±10 %, ChiNext/STAR ±20 %,
BSE ±30 %, ST ±5 %) so the paper path and the research path share one board
classification and must reach the same verdict for the same code and session.

Intentional divergence from research replay: ``datahub/scripts/
factor_lab_account_replay.tradeable_open`` (lower-case side spelling) treats an
UNDERIVABLE verdict as no limit (fail open) so historical replays stay
reproducible; this production path fails CLOSED instead (see the module
docstring of ``strategy-live-loop-minimal``'s proposal). The two share the board
classification but not the unknown-verdict policy.

Known residual hole (documented, not silently accepted): ``is_st`` is
best-effort — ``StockDailyQuote.isST`` can be missing for 2026+ rows — so an ST
name whose ST bit is absent is classified at the main-board 10 % limit and a
+5 % limit-up BUY could fill. The blast radius is bounded by the default
``constraints.exclude_st=true`` (ST names are not selected at all); it is only
reachable when ``exclude_st`` is explicitly disabled on data with a missing ST
bit. Fixing it needs a point-in-time ST source and is a separate slice.
"""

from __future__ import annotations

import math

from app.lib.factor_lab.panel import LIMIT_EPSILON, price_limit

SIDES = ("BUY", "SELL")


def _finite_positive(value) -> bool:
    try:
        return (
            not isinstance(value, bool)
            and math.isfinite(float(value))
            and float(value) > 0
        )
    except (TypeError, ValueError, OverflowError):
        return False


def limit_verdict(
    *,
    stock_code: str,
    open_price,
    previous_close,
    is_st: bool = False,
) -> tuple[bool | None, bool | None]:
    """(limit_up, limit_down) for one session, or (None, None) when unknown.

    Unknown means the previous close is missing/non-positive, so the limit price
    cannot be derived: callers must treat that as blocking for the side they
    check, never as "not at the limit".
    """
    if not _finite_positive(previous_close) or not _finite_positive(open_price):
        return None, None
    board = price_limit(stock_code, is_st=is_st)
    previous = float(previous_close)
    opened = float(open_price)
    upper = previous * (1 + (board - LIMIT_EPSILON) / 100)
    lower = previous * (1 - (board - LIMIT_EPSILON) / 100)
    return opened >= upper, opened <= lower


def tradeable_open(
    quote, side: str, *, stock_code: str = ""
) -> tuple[float | None, str]:
    """Price the side could fill at, plus the reason when it cannot.

    Returns ``(price, "")`` on success or ``(None, reason)`` where reason is a
    stable machine-readable token (``suspended``, ``no_open``, ``at_limit_up``,
    ``at_limit_down``, ``limit_unknown``, ``bad_side``). The reason is meant to
    be surfaced in the plan/report: a refused order is reported, never silently
    dropped.
    """
    normalized = (side or "").strip().upper()
    if normalized not in SIDES:
        raise ValueError(f"side must be one of {SIDES}, got {side!r}")
    if quote is None:
        return None, "no_quote"
    try:
        status = int(quote.trade_status)
    except (TypeError, ValueError, OverflowError):
        return None, "suspended"
    if status != 1:
        return None, "suspended"
    open_price = getattr(quote, "open", None)
    if not _finite_positive(open_price):
        return None, "no_open"
    # An explicit flag supplied by the caller (e.g. the factor-lab panel's own
    # verdict) wins; otherwise derive it from previous_close + board limit.
    limit_up = getattr(quote, "limit_up", None)
    limit_down = getattr(quote, "limit_down", None)
    if limit_up is None or limit_down is None:
        previous_close = getattr(quote, "previous_close", None)
        derived_up, derived_down = limit_verdict(
            stock_code=stock_code or getattr(quote, "stock_code", "") or "",
            open_price=open_price,
            previous_close=previous_close,
            is_st=bool(getattr(quote, "is_st", False)),
        )
        if limit_up is None:
            limit_up = derived_up
        if limit_down is None:
            limit_down = derived_down
    if normalized == "BUY":
        if limit_up is None:
            return None, "limit_unknown"
        if bool(limit_up):
            return None, "at_limit_up"
    else:
        if limit_down is None:
            return None, "limit_unknown"
        if bool(limit_down):
            return None, "at_limit_down"
    return float(open_price), ""
