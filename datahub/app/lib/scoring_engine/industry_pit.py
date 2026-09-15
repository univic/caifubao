# -*- coding: utf-8 -*-
"""Point-in-time attribution for the industry classification store.

The collection holds one *current* row per stock, so any consumer that
attributes an industry to a past date must first prove the row was already in
effect then. That rule lives here rather than in ``app.model.industry`` because
the datahub image (and CI) copy ``backend/app/model/`` over
``datahub/app/model/``; a helper placed in a model module would be silently
dropped from the deployed image.

``assigned_at`` is written only when a row is created; later changes are
recorded in ``industry_change_log`` instead of moving ``assigned_at``. A row is
therefore attributable to date ``D`` only when it existed on or before ``D`` and
no recorded change happened after it.
"""

import datetime
from collections.abc import Mapping


def as_calendar_date(value):
    """A date for comparison, or None when the value is not usable.

    Compared as calendar dates so mixed naive/aware timestamps cannot raise or
    shift the comparison. ``industry_change_log`` entries store their timestamp
    as an ISO string (the writer uses ``now.isoformat()``), so strings are
    parsed; an unparseable value is unusable rather than assumed.
    """
    if value is None:
        return None
    if isinstance(value, datetime.datetime):
        return value.date()
    if isinstance(value, datetime.date):
        return value
    if isinstance(value, str):
        try:
            return datetime.datetime.fromisoformat(value).date()
        except ValueError:
            return None
    return None


def _field(row, name):
    """Read one classification field from a document or a plain mapping."""
    if isinstance(row, Mapping):
        return row.get(name)
    return getattr(row, name, None)


def classification_in_effect_on(row, as_of_date) -> bool:
    """Whether the row's CURRENT classification is provably in effect on a date.

    Accepts a mongoengine document or a plain mapping (the H20 snapshot runner
    projects industry rows to dicts). Every consumer that reads the live store
    and attributes an industry to a past date — paper strategy, replayed
    scoring, industry metric aggregation, the H20 export — must use this guard:
    an unguarded read is look-ahead.

    A frozen P2a artifact row carries only ``assigned_at``. That is sufficient
    there: the capture window (after the prior close, before the open) bounds
    the row, and a change-log entry cannot post-date the capture because the
    store is only ever written by the sync. The loop below therefore sees an
    empty history for artifact rows rather than a missing rule.
    """
    target = as_calendar_date(as_of_date)
    assigned = as_calendar_date(_field(row, "assigned_at"))
    if target is None or assigned is None or assigned > target:
        return False
    for entry in _field(row, "industry_change_log") or []:
        changed = as_calendar_date(
            entry.get("timestamp") if isinstance(entry, dict) else None
        )
        if changed is None or changed > target:
            return False
    return True
