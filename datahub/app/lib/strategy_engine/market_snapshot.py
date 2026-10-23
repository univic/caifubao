# -*- coding: utf-8 -*-
"""PIT adapter: 60-day CSI300 regime classification for research risk replay.

MarketRegimeService owns source access; shared RiskGate only consumes an
explicit, frozen market snapshot. A classification of 'unknown' is not a
valid substitute for a tradable regime.
"""

from __future__ import annotations

import datetime


def frozen_regime_snapshot(
    decision_date: datetime.datetime,
    *,
    service=None,
    snapshot_as_of: datetime.datetime | None = None,
) -> dict:
    if not isinstance(decision_date, datetime.datetime):
        raise ValueError("decision_date must be datetime")
    if snapshot_as_of is not None:
        if not isinstance(snapshot_as_of, datetime.datetime):
            raise ValueError("snapshot_as_of must be datetime")
        try:
            late = snapshot_as_of > decision_date
        except TypeError as exc:
            raise ValueError("market snapshot timezone does not match decision") from exc
        if late:
            raise ValueError("market snapshot is after decision_date")
    if service is None:
        from app.lib.market_regime import get_service

        service = get_service()
    if getattr(service, "lookback_days", None) != 60:
        raise ValueError("market_regime requires a 60-day lookback policy")
    label = service.classify_date(decision_date)
    if label not in ("bull", "bear", "sideways"):
        raise ValueError("market_regime is unavailable on decision_date")
    return {
        "regime": label,
        "decision_date": decision_date.date().isoformat(),
        "regime_lookback_days": 60,
        "regime_source": "CSI300",
    }
