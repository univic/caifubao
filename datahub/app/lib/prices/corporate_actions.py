# -*- coding: utf-8 -*-
"""Corporate-action ledger schema and event arithmetic.

An event row describes one ex-date action for one code.  Four action types are
supported and they are *not* interchangeable:

``bonus`` / ``transfer`` / ``share_reform``
    The holder receives ``share_ratio`` extra shares per share held.
``rights``
    The holder may subscribe ``rights_ratio`` new shares at ``rights_price``;
    the account pays ``rights_ratio * rights_price`` in cash.
``cash``
    The holder receives ``cash_per_share`` per share.

The per-share value on the ex-date is::

    value = (1 + share_ratio + rights_ratio) * close_ex
            + cash_per_share
            - rights_ratio * rights_price

and the total return is ``value / close_prev - 1``.  This is the account-based
convention used by the real-share ledger; it differs from a pure price-index
factor when a rights issue occurs, because subscribing is a cash outflow.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pandas as pd

ACTIONS = frozenset({"bonus", "transfer", "share_reform", "rights", "cash"})
REQUIRED_COLUMNS = (
    "code",
    "ex_date",
    "action_type",
    "share_ratio",
    "cash_per_share",
    "rights_ratio",
    "rights_price",
    "announced_at",
    "source",
)


@dataclass(frozen=True)
class CorporateAction:
    code: str
    ex_date: pd.Timestamp
    action_type: str
    share_ratio: float = 0.0
    cash_per_share: float = 0.0
    rights_ratio: float = 0.0
    rights_price: float = 0.0
    announced_at: pd.Timestamp | None = None
    source: str = ""
    source_url: str = ""
    note: str = ""

    def __post_init__(self) -> None:
        if self.action_type not in ACTIONS:
            raise ValueError(f"unknown action_type: {self.action_type!r}")
        for name in ("share_ratio", "cash_per_share", "rights_ratio", "rights_price"):
            value = getattr(self, name)
            if value is None or value < 0:
                raise ValueError(f"{name} must be >= 0, got {value!r}")
        if self.action_type == "rights" and self.rights_ratio <= 0:
            raise ValueError("rights event requires rights_ratio > 0")
        if self.action_type == "rights" and self.rights_price <= 0:
            raise ValueError("rights event requires rights_price > 0")

    @property
    def share_multiplier(self) -> float:
        """Shares held after the event per share held before it."""
        return 1.0 + self.share_ratio + self.rights_ratio

    def value_of_one_share(self, close_ex: float) -> float:
        """Account value per pre-event share, in cash-equivalent terms."""
        return (
            self.share_multiplier * close_ex
            + self.cash_per_share
            - self.rights_ratio * self.rights_price
        )

    def event_return(self, close_prev: float, close_ex: float) -> float:
        if close_prev <= 0:
            raise ValueError("close_prev must be positive")
        return self.value_of_one_share(close_ex) / close_prev - 1.0

    def price_factor(self, close_prev: float) -> float:
        """Theoretical ex-price multiplier (price-index convention).

        ``close_prev * factor`` is the theoretical ex-price level.  The return
        index does not use this factor; it applies :meth:`event_return`
        directly so that subscription cash and dividends are explicit.
        """
        if close_prev <= 0:
            raise ValueError("close_prev must be positive")
        if self.action_type == "rights":
            ex_price = (
                close_prev + self.rights_ratio * self.rights_price
            ) / self.share_multiplier
        else:
            ex_price = (close_prev - self.cash_per_share) / self.share_multiplier
        if ex_price <= 0:
            raise ValueError("non-positive theoretical ex-price")
        return close_prev / ex_price


def _parse_date(value) -> pd.Timestamp | None:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    text = str(value).strip()
    if not text:
        return None
    return pd.Timestamp(text)


def normalize_action(row: dict) -> CorporateAction:
    missing = [c for c in REQUIRED_COLUMNS if c not in row]
    if missing:
        raise ValueError(f"corporate action row missing fields: {missing}")
    ex_date = _parse_date(row["ex_date"])
    if ex_date is None:
        raise ValueError("ex_date is required")
    announced = _parse_date(row.get("announced_at"))
    if announced is not None and announced > ex_date:
        raise ValueError("announced_at cannot be after ex_date")
    return CorporateAction(
        code=str(row["code"]).zfill(6),
        ex_date=ex_date,
        action_type=str(row["action_type"]),
        share_ratio=float(row.get("share_ratio") or 0.0),
        cash_per_share=float(row.get("cash_per_share") or 0.0),
        rights_ratio=float(row.get("rights_ratio") or 0.0),
        rights_price=float(row.get("rights_price") or 0.0),
        announced_at=announced,
        source=str(row.get("source") or ""),
        source_url=str(row.get("source_url") or ""),
        note=str(row.get("note") or ""),
    )


def load_actions(path: str | Path) -> list[CorporateAction]:
    """Load and validate a corporate-action CSV for one or more codes."""
    frame = pd.read_csv(path, dtype={"code": str})
    if frame.empty:
        raise ValueError(f"empty corporate-action ledger: {path}")
    actions = [normalize_action(row) for row in frame.to_dict("records")]
    return sorted(actions, key=lambda a: (a.code, a.ex_date))


def event_factor(action: CorporateAction) -> float:
    """Convenience wrapper for the share multiplier of an event."""
    return action.share_multiplier
