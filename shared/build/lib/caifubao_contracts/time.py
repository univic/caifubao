# -*- coding: utf-8 -*-
"""One time contract for every service.

MongoDB stores instants in UTC without an offset, and pymongo returns them as
naive UTC datetimes. The services used to mix three conventions — aware UTC,
naive UTC and naive *local* time (`datetime.now()`), which silently skews by
the container offset and breaks freshness comparisons.

Rules:

* persist with :func:`utc_now_naive` (naive UTC, matching what reads return);
* compare persisted values with naive UTC;
* serialize for clients with :func:`iso_utc` (offset included, so the browser
  parses the instant unambiguously);
* compute trading-calendar days with :func:`beijing_now`, never with the
  container's local clock.
"""

from datetime import datetime, timezone
from zoneinfo import ZoneInfo

UTC = timezone.utc
BEIJING_TZ = ZoneInfo("Asia/Shanghai")


def utc_now() -> datetime:
    """Current instant as an aware UTC datetime."""
    return datetime.now(UTC)


def utc_now_naive() -> datetime:
    """Current instant as a naive UTC datetime (the MongoDB wire convention)."""
    return datetime.now(UTC).replace(tzinfo=None)


def to_utc_naive(value: datetime | None) -> datetime | None:
    """Normalize any datetime to naive UTC; pass ``None`` through."""
    if value is None:
        return None
    if value.tzinfo is None:
        return value
    return value.astimezone(UTC).replace(tzinfo=None)


def to_aware_utc(value: datetime | None) -> datetime | None:
    """Interpret naive values as UTC and return an aware UTC datetime."""
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def iso_utc(value: datetime | None = None) -> str | None:
    """RFC 3339 spelling with an explicit offset (``+00:00``).

    Plain ``date`` values (trade dates, calendar days) are passed through as
    date-only strings — they are calendar labels, not instants.
    """
    if value is None:
        return utc_now().isoformat()
    if not isinstance(value, datetime):
        return value.isoformat()
    return to_aware_utc(value).isoformat()


def beijing_now() -> datetime:
    """Current instant in Asia/Shanghai (trading-calendar decisions)."""
    return datetime.now(BEIJING_TZ)


def beijing_now_naive() -> datetime:
    """Beijing wall-clock time without tzinfo (the stored calendar convention)."""
    return datetime.now(BEIJING_TZ).replace(tzinfo=None)
