# -*- coding: utf-8 -*-

import bisect
import datetime
import hashlib
import logging
import math
import os
from collections import defaultdict

import pandas as pd
from pymongo import UpdateOne

from app.lib.scoring_engine.components import (
    aggregate_industry_metrics,
    breakout_or_position_component,
    industry_momentum_component,
    momentum_component,
    quote_price,
    real_relative_strength_component,
    relative_strength_component,
    risk_penalty,
    signal_strength_component,
    trend_alignment_component,
)
from app.lib.scoring_engine.config import (
    DEFAULT_MODEL_VERSION,
    SUPPORTED_HORIZONS,
    get_effective_horizon_config,
)
from app.model.factor import StockFactorDaily
from app.model.industry import IndustryDailyMetrics, StockIndustryClassification
from app.model.scoring import ScoreModelVersion, StockScorePrediction
from app.model.signal import StockSignalDaily
from app.model.stock import FinanceMarket, IndividualStock, StockDailyQuote
from app.lib.utilities import trading_day_helper

logger = logging.getLogger(__name__)

#: Kill switch for the C1 per-day batch path. ``0``/``false``/``no``/``off``
#: restores the legacy per-stock read/write path (used as a rollback lever and
#: by the batch/per-stock equivalence harness).
BATCH_SCORING_ENV = "DATAHUB_SCORING_BATCH"
_BATCH_FALSY = {"0", "false", "no", "off"}


def batch_scoring_default() -> bool:
    return os.getenv(BATCH_SCORING_ENV, "1").strip().lower() not in _BATCH_FALSY


def normalize_date(value: datetime.datetime) -> datetime.datetime:
    return value.replace(hour=0, minute=0, second=0, microsecond=0)


class _Row:
    """Attribute view over one prefetched raw document (perf C1).

    Batch scoring reads whole-market collections with ``as_pymongo()`` so no
    mongoengine Document is hydrated. Components keep their existing
    ``getattr``/attribute code paths against these rows.

    Strictness depends on how the row was built, and only ONE of the three
    paths is fail-loud:

    * history window (``_row_from_frame``) rebuilds the dict from the frame
      columns alone, so reading a de-projected field raises ``AttributeError``
      like a Document with an unknown attribute;
    * full-document and `.only()` reads (``_rows`` -> ``_row_from_doc``) merge
      the complete model default skeleton (``_field_skeleton``), so a
      de-projected field silently reads as the model default (usually
      ``None``) instead of raising.

    Adding a field to a component therefore requires adding it to the matching
    whitelist (``_HISTORY_QUOTE_FIELDS`` / ``_SIGNAL_DECAY_FIELDS`` /
    ``_INDUSTRY_*_FIELDS``); the batch/per-stock equivalence harness is the
    safety net for the silent case.
    """

    __slots__ = ("_data",)

    def __init__(self, data: dict):
        self._data = data

    def __getattr__(self, name):
        try:
            return self._data[name]
        except KeyError as exc:
            raise AttributeError(name) from exc

    def __repr__(self):  # pragma: no cover - debugging aid
        ident = self._data.get("code") or self._data.get("stock_code")
        return f"<_Row {ident}>"


def _signal_order_key(signal):
    """Total, deterministic order key for one day's signals of a stock.

    ``stock_signal_daily`` is unique on ``(stock_code, date, signal_name)`` and
    neither scoring read sorts, so the returned order depends on the index plan
    (``stock_code=X`` vs ``stock_code__in=[...]``). Signal order is persisted in
    ``explanation[].evidence.signals`` and ``input_snapshot.signals.dates``, so
    both paths must impose the same order.
    """
    return getattr(signal, "signal_name", None) or ""


def _raw_data(item) -> dict:
    """Stored values of one queried row (raw dict or Document-like object)."""
    if isinstance(item, dict):
        return dict(item)
    return dict(getattr(item, "_data", None) or vars(item))


def _row_from_doc(item, skeleton: dict) -> _Row:
    data = dict(skeleton)
    data.update(_raw_data(item))
    return _Row(data)


def _row_from_frame(record: dict) -> _Row:
    """Rebuild a Document-like row from one pandas frame record.

    pandas fills a missing numeric value with NaN and materialises datetime
    columns as ``Timestamp``/``NaT``. Both would leak into component results
    (NaN compares false and poisons the momentum/risk/range kernels), so map
    them back to the ``None``/``datetime`` a mongoengine load would produce.
    """
    data = {}
    for key, value in record.items():
        if value is None or value is pd.NaT:
            data[key] = None
        elif isinstance(value, float) and math.isnan(value):
            data[key] = None
        elif isinstance(value, pd.Timestamp):
            data[key] = value.to_pydatetime()
        else:
            data[key] = value
    return _Row(data)


#: Quote fields the prefetched history window must keep (perf C1).
#:
#: History rows are only consumed by momentum (close family), breakout
#: (high/low family), risk and relative strength (close family) and real
#: relative strength (close family + date); the evaluation-day quote itself is
#: prefetched as a full document. Projecting the window keeps a whole-market
#: frame at a few tens of MB instead of ~0.9GB of hydrated documents, which
#: would not fit the 1Gi datahub pod. Any component that starts reading another
#: quote field from history must be added here — the batch/per-stock
#: equivalence harness (test_scoring_batch_equivalence.py) is the safety net.
_HISTORY_QUOTE_FIELDS = (
    "code",
    "date",
    "close",
    "close_hfq",
    "high",
    "high_hfq",
    "low",
    "low_hfq",
)

_HISTORY_QUOTE_SKELETON = {name: None for name in _HISTORY_QUOTE_FIELDS}

#: Rows per frame chunk while materialising the history window. Building one
#: DataFrame from the whole cursor materialises hundreds of MB of raw dicts
#: first; chunking (and releasing the chunk frames right after ``concat``)
#: keeps the peak near one final frame plus the current chunk, which fits the
#: 1Gi datahub pod.
_WINDOW_CHUNK_ROWS = 50_000

#: Signal fields the decay window keeps. Decay grouping reads only
#: direction/signal_name/date/strength (the decayed evidence carries names and
#: strengths), so the potentially large snapshot fields stay on disk. A live
#: (evaluation-day) signal is still prefetched with its full document.
_SIGNAL_DECAY_FIELDS = (
    "stock_code",
    "date",
    "signal_name",
    "direction",
    "strength",
)

#: Industry fields the per-day cache keeps: the classification key/name plus
#: the metrics columns the component reads (stock_count/avg_score for its
#: value, buy_count/watch_count for its evidence).
_INDUSTRY_CLASSIFICATION_FIELDS = (
    "stock_code",
    "industry_code_sw_l1",
    "industry_name_sw_l1",
)
_INDUSTRY_METRIC_FIELDS = (
    "industry_code",
    "horizon",
    "date",
    "stock_count",
    "avg_score",
    "buy_count",
    "watch_count",
)


class _HistoryWindow:
    """Date-descending per-code quote history backed by a prefetched frame.

    Components only need ``len()``, int/slice indexing, iteration,
    ``reversed()`` and ``history + [quote]`` (momentum, breakout, risk and
    relative strength). Rows are ``_Row`` views, so every numeric kernel stays
    byte-identical to the per-stock path (3.5 equivalence harness).
    """

    __slots__ = ("_rows",)

    def __init__(self, rows: list):
        self._rows = list(rows)

    def __len__(self):
        return len(self._rows)

    def __iter__(self):
        return iter(self._rows)

    def __getitem__(self, key):
        if isinstance(key, slice):
            return self._rows[key]
        return self._rows[key]

    def __add__(self, other):
        return self._rows + list(other)

    def __radd__(self, other):
        return list(other) + self._rows


class _DayPrefetch:
    """Whole-market scoring inputs prefetched once per evaluation day (C1).

    Replaces the per-stock N+1 reads (today's quote/factor/signals, the
    per-stock history and signal-decay windows, industry classification and
    metrics, CSI300 and existing predictions) with a constant number of
    day-level queries shared by every stock and horizon:

    1. ``StockDailyQuote`` for the evaluation date;
    2. ``StockDailyQuote`` for ``[window_start, date)`` -> code -> DataFrame;
    3. ``StockFactorDaily`` for the evaluation date;
    4. ``StockSignalDaily`` for the evaluation date and the decay window;
    5. CSI300 quotes over the same history window (per-day cache, C1/3.4);
    6. L1 industry classification + latest metrics per horizon (C1/3.4);
    7. existing predictions for this date/model_version.

    Rows are raw-dict ``_Row`` views (no Document hydration). The history
    window is sized in *trading* days (``max_history + margin``); a code whose
    window holds fewer rows than the caller needs falls back to the exact
    legacy per-code query, so batching can never truncate a sparse stock's
    history.
    """

    def __init__(self, service, date, codes, horizons):
        self.service = service
        self.date = normalize_date(date)
        self.codes = list(codes)
        self.horizons = list(horizons)
        configs = [service._get_horizon_config(h) for h in self.horizons]
        self.max_history = max(
            max(
                config["minimum_quote_count"],
                config["breakout_lookback"],
                config["risk_lookback"],
            )
            for config in configs
        )
        self.decay_window = max(
            config.get("signal_decay_max_days", 5) for config in configs
        )
        self.window_start = service._history_window_start(self.date, self.max_history)
        self._skeletons: dict[int, dict] = {}
        self._day_quotes = None
        self._day_factors = None
        self._day_signals = None
        self._decay = None
        self._window_frame = None
        self._window_positions = None
        self._fallback_history: dict[str, list] = {}
        self._index_quotes = None
        self._index_failed = False
        #: CSI300 rows for a code whose fallback history reaches before the day
        #: window, keyed by code. Bounded by the number of such (rare) codes and
        #: by their fallback-history span (<= max_history quotes, i.e. at most a
        #: few thousand small CSI300 rows), so the retention is negligible
        #: against the window frame itself.
        self._index_by_code: dict[str, list | None] = {}
        self._industry = None
        self._industry_failed = False
        self._existing = None

    # -- row materialization -------------------------------------------------
    def _skeleton(self, model) -> dict:
        key = id(model)
        skeleton = self._skeletons.get(key)
        if skeleton is None:
            skeleton = self.service._field_skeleton(model)
            self._skeletons[key] = skeleton
        return skeleton

    def _rows(self, queryset, model) -> list:
        skeleton = self._skeleton(model)
        if hasattr(queryset, "as_pymongo"):
            queryset = queryset.as_pymongo()
        return [_row_from_doc(item, skeleton) for item in queryset]

    # -- evaluation-day data -------------------------------------------------
    def _load_day(self):
        if self._day_quotes is not None:
            return
        # Build locally and assign at the end: a failed read must not leave a
        # half-initialised context behind for the next stock.
        day_quotes = {
            row.code: row
            for row in self._rows(
                self.service.quote_model.objects(code__in=self.codes, date=self.date),
                self.service.quote_model,
            )
        }
        day_factors = {
            row.stock_code: row
            for row in self._rows(
                self.service.factor_model.objects(
                    stock_code__in=self.codes, date=self.date
                ),
                self.service.factor_model,
            )
        }
        signals = defaultdict(list)
        for row in self._rows(
            self.service.signal_model.objects(
                stock_code__in=self.codes, date=self.date
            ),
            self.service.signal_model,
        ):
            signals[row.stock_code].append(row)
        self._day_quotes = day_quotes
        self._day_factors = day_factors
        self._day_signals = signals

    def quote(self, code):
        self._load_day()
        return self._day_quotes.get(code)

    def factor(self, code):
        self._load_day()
        return self._day_factors.get(code)

    def day_signals(self, code) -> list:
        self._load_day()
        # Same canonical order as the per-stock read (see _get_signals_on_date).
        return sorted(self._day_signals.get(code, []), key=_signal_order_key)

    # -- history window ------------------------------------------------------
    def _window_records_iter(self, queryset):
        """Projected raw dicts for the window, streamed (no Document hydration)."""
        if hasattr(queryset, "as_pymongo"):
            queryset = queryset.as_pymongo()
        for item in queryset:
            data = dict(_HISTORY_QUOTE_SKELETON)
            data.update(_raw_data(item))
            # ``.only()`` already limits production rows; rebuilding from the
            # whitelist keeps test doubles and missing stored keys identical.
            yield {name: data[name] for name in _HISTORY_QUOTE_FIELDS}

    def _load_window(self):
        queryset = (
            self.service.quote_model.objects(
                code__in=self.codes,
                date__gte=self.window_start,
                date__lt=self.date,
            )
            .only(*_HISTORY_QUOTE_FIELDS)
            .order_by("-date")
        )
        frames = []
        chunk = []
        for record in self._window_records_iter(queryset):
            chunk.append(record)
            if len(chunk) >= _WINDOW_CHUNK_ROWS:
                frames.append(pd.DataFrame.from_records(chunk))
                chunk = []
        if chunk:
            frames.append(pd.DataFrame.from_records(chunk))
        if not frames:
            self._window_frame = pd.DataFrame(columns=list(_HISTORY_QUOTE_FIELDS))
            self._window_positions = {}
            return
        frame = frames[0] if len(frames) == 1 else pd.concat(frames, ignore_index=True)
        del frames  # release the chunk frames before sorting/retaining the window
        frame = frame.sort_values(
            ["code", "date"], ascending=[True, False], kind="stable"
        )
        # Keep the frame (columnar, tens of MB) plus a code -> row-position
        # index; materialise at most one stock's rows at a time in history().
        self._window_frame = frame
        self._window_positions = dict(frame.groupby("code", sort=False).indices)

    def history(self, code, limit):
        """Most recent ``limit`` quotes strictly before the evaluation date."""
        limit = int(limit)
        if self._window_positions is None:
            self._load_window()
        positions = self._window_positions.get(code)
        if positions is not None and len(positions) >= limit:
            frame_rows = self._window_frame.take(positions[:limit])
            return _HistoryWindow(
                [_row_from_frame(record) for record in frame_rows.to_dict("records")]
            )
        # Sparse/suspended code: the day window may hold fewer rows than the
        # legacy unbounded per-code read would have returned. Re-read exactly
        # that code so batching never changes its history.
        fallback = self._fallback_history.get(code)
        if fallback is None:
            fallback = self._rows(
                self.service.quote_model.objects(code=code, date__lt=self.date)
                .order_by("-date")
                .limit(self.max_history),
                self.service.quote_model,
            )
            self._fallback_history[code] = fallback
        return _HistoryWindow(fallback[:limit])

    def decay_signals(self, code) -> list:
        """Signals in the decay window, newest first (byte-parity with legacy).

        The window spans the largest configured decay so one query serves every
        horizon; components still gate on their own ``signal_decay_max_days``,
        so a wider window can only yield the same "no decay" outcome.
        """
        if self._decay is None:
            start = self.date - datetime.timedelta(days=self.decay_window + 1)
            decay = defaultdict(list)
            for row in self._rows(
                self.service.signal_model.objects(
                    stock_code__in=self.codes,
                    date__gte=start,
                    date__lt=self.date,
                )
                .only(*_SIGNAL_DECAY_FIELDS)
                .order_by("-date"),
                self.service.signal_model,
            ):
                decay[row.stock_code].append(row)
            self._decay = decay
        return self._decay.get(code, [])

    # -- per-day caches (C1/3.4) --------------------------------------------
    def index_quotes(self):
        """CSI300 quotes for the whole history window (one read per day)."""
        if self._index_failed:
            return None
        if self._index_quotes is None:
            try:
                self._index_quotes = self._rows(
                    self.service.quote_model.objects(
                        code="sh000300",
                        date__gte=self.window_start,
                        date__lte=self.date,
                    ).order_by("date"),
                    self.service.quote_model,
                )
            except Exception:  # noqa: BLE001 - component retries its own read
                logger.warning(
                    "CSI300 prefetch failed; falling back to per-stock reads",
                    exc_info=True,
                )
                self._index_failed = True
                return None
        return self._index_quotes

    def index_quotes_for(self, code, history_quotes, quote):
        """CSI300 quotes for one stock, exactly as the per-stock path read them.

        ``real_relative_strength`` derives its index range from the stock's own
        quote dates (``[min(stock quote dates), evaluation date]``). Filtering
        the day cache to that same range keeps the alpha input identical, and a
        sparse-history fallback that reaches before the window start gets its
        own index read instead of silently dropping the evaluation date to the
        self-proxy fallback. Returns ``None`` on failure so the component
        retries its own read.
        """
        dates = [row.date for row in history_quotes if getattr(row, "date", None)]
        quote_date = getattr(quote, "date", None)
        if quote_date:
            dates.append(quote_date)
        if not dates:
            return []
        start = min(dates)
        end = max(dates)
        if start >= self.window_start:
            cached = self.index_quotes()
            if cached is None:
                return None
            return [row for row in cached if start <= row.date <= end]
        if code not in self._index_by_code:
            # Read the code's whole fallback span once (its history limit is the
            # deepest any horizon needs), then slice it per horizon below.
            fallback_dates = [
                row.date
                for row in self._fallback_history.get(code, [])
                if getattr(row, "date", None)
            ]
            full_start = min(fallback_dates) if fallback_dates else start
            try:
                self._index_by_code[code] = self._rows(
                    self.service.quote_model.objects(
                        code="sh000300", date__gte=full_start, date__lte=end
                    ).order_by("date"),
                    self.service.quote_model,
                )
            except Exception:  # noqa: BLE001 - component retries its own read
                logger.warning(
                    "CSI300 range read failed for %s; component falls back",
                    code,
                    exc_info=True,
                )
                self._index_by_code[code] = None
        per_code = self._index_by_code[code]
        if per_code is None:
            return None
        return [row for row in per_code if start <= row.date <= end]

    def industry_lookup(self, horizon: int):
        """{stock_code: (classification, latest metrics)} for one horizon.

        Returns ``None`` when the prefetch fails so components fall back to
        their own per-stock reads (never a silent "no industry" answer).
        """
        if self._industry_failed:
            return None
        if self._industry is None:
            self._industry = self._load_industry()
        if self._industry_failed:
            return None
        return self._industry.get(horizon, {})

    def _load_industry(self):
        try:
            # Projected + raw rows: only the classification key/name and the
            # newest metrics row per (industry, horizon) are consumed, so the
            # wide documents are never hydrated.
            classifications = {
                row.stock_code: row
                for row in self._rows(
                    self.service.industry_model.objects(stock_code__in=self.codes).only(
                        *_INDUSTRY_CLASSIFICATION_FIELDS
                    ),
                    self.service.industry_model,
                )
            }
            industry_codes = sorted(
                {
                    row.industry_code_sw_l1
                    for row in classifications.values()
                    if getattr(row, "industry_code_sw_l1", None)
                }
            )
            # Legacy reads the single newest metrics row per (industry,
            # horizon) and then checks its stock_count — it does NOT fall back
            # to an older date when the newest row is thin, so mirror that.
            latest = {}
            if industry_codes:
                for metrics in self._rows(
                    self.service.industry_metrics_model.objects(
                        industry_code__in=industry_codes,
                        date__lt=self.date,
                        horizon__in=self.horizons,
                        model_version=self.service.model_version,
                    ).only(*_INDUSTRY_METRIC_FIELDS),
                    self.service.industry_metrics_model,
                ):
                    key = (metrics.industry_code, metrics.horizon)
                    current = latest.get(key)
                    if current is None or metrics.date > current.date:
                        latest[key] = metrics
        except Exception:  # noqa: BLE001 - components retry their own reads
            logger.warning(
                "industry prefetch failed; falling back to per-stock reads",
                exc_info=True,
            )
            self._industry_failed = True
            return {}
        return {
            horizon: {
                code: (row, latest.get((row.industry_code_sw_l1, horizon)))
                for code, row in classifications.items()
            }
            for horizon in self.horizons
        }

    # -- existing predictions -----------------------------------------------
    def existing(self, code, horizon):
        if self._existing is None:
            self._existing = {}
            for prediction in self.service.prediction_model.objects(
                date=self.date,
                model_version=self.service.model_version,
                stock_code__in=self.codes,
                horizon__in=self.horizons,
            ).only("stock_code", "horizon", "status"):
                self._existing[(prediction.stock_code, prediction.horizon)] = prediction
        return self._existing.get((code, horizon))


class StockScoringService:
    """Generate multi-horizon score predictions from existing datahub data."""

    def __init__(
        self,
        stock_model=IndividualStock,
        quote_model=StockDailyQuote,
        factor_model=StockFactorDaily,
        signal_model=StockSignalDaily,
        prediction_model=StockScorePrediction,
        model_version: str = DEFAULT_MODEL_VERSION,
        scoring_config: dict | None = None,
        industry_model=StockIndustryClassification,
        industry_metrics_model=IndustryDailyMetrics,
        batch_prefetch: bool | None = None,
    ):
        self.stock_model = stock_model
        self.quote_model = quote_model
        self.factor_model = factor_model
        self.signal_model = signal_model
        self.prediction_model = prediction_model
        self.industry_model = industry_model
        self.industry_metrics_model = industry_metrics_model
        self.model_version = model_version
        # C1: default to the per-day batch path; DATAHUB_SCORING_BATCH=0
        # restores the legacy per-stock read/write path.
        self.batch_prefetch = (
            batch_scoring_default() if batch_prefetch is None else bool(batch_prefetch)
        )
        # Config precedence: explicit scoring_config (experiment/backfill) >
        # registered ScoreModelVersion config > built-in SCORING_CONFIG.
        # A registered version makes the run reproducible from the registry
        # alone (scoring_runner passes only model_version today).
        if scoring_config:
            self.scoring_config = scoring_config
        else:
            self.scoring_config = self._registered_config(model_version)
        self.market = FinanceMarket.objects(name="ChinaAStock").first()
        self.calendar = self.market.trade_calendar if self.market else []
        # (calendar_list, ascending_normalized_calendar); see _sorted_calendar.
        self._calendar_cache = None

    @staticmethod
    def _registered_config(model_version: str) -> dict:
        """Look up an ACTIVE registered model version's per-horizon override.

        Returns {} when the version is not registered (falls back to built-in
        SCORING_CONFIG) or is retired. Registry lookup is best-effort: a DB
        error must never break scoring, but it is logged for observability.
        """
        try:
            registered = ScoreModelVersion.objects(
                model_version=model_version, status="ACTIVE"
            ).first()
        except Exception:  # noqa: BLE001 - registry is best-effort
            logger.warning(
                "model registry lookup failed for %r; falling back to built-in config",
                model_version,
                exc_info=True,
            )
            return {}
        if registered is None:
            return {}
        return dict(registered.config or {})

    def _sorted_calendar(self) -> list:
        """Ascending, time-normalized trading calendar, computed once.

        ``get_t_plus_n_day`` runs once per stock per horizon (~15.6k calls on a
        full-market day) and rebuilding + re-normalizing the ~8.5k-day calendar
        on every call cost ~3.5 ms each — ~55 s of pure CPU per day (perf
        C5/R4). The cache is keyed on the *identity* of ``self.calendar`` so a
        reassigned calendar (tests, re-bootstrap) is picked up rather than
        silently serving a stale list.
        """
        calendar = self.calendar or []
        cache = getattr(self, "_calendar_cache", None)
        if cache is None or cache[0] is not calendar:
            cache = (calendar, sorted(normalize_date(day) for day in calendar))
            self._calendar_cache = cache
        return cache[1]

    def get_t_plus_n_day(
        self, start_date: datetime.datetime, n: int
    ) -> datetime.datetime:
        """Find the N-th trading day after start_date."""
        start_date = normalize_date(start_date)
        sorted_cal = self._sorted_calendar()
        if not sorted_cal:
            return start_date + datetime.timedelta(days=round(n * 1.5))
        if n <= 0:
            # Not reachable from scoring (horizons are 5/20/60). The previous
            # implementation raised IndexError for n=0 on a non-trading day and
            # otherwise returned the calendar's last day; the identity answer
            # is what every caller would want.
            return start_date

        start_idx = bisect.bisect_left(sorted_cal, start_date)
        if start_idx < len(sorted_cal) and sorted_cal[start_idx] == start_date:
            target_idx = start_idx + n
            if target_idx < len(sorted_cal):
                return sorted_cal[target_idx]
            return sorted_cal[-1]
        # start_date is not a trading day: count from the next trading day.
        if len(sorted_cal) - start_idx >= n:
            return sorted_cal[start_idx + n - 1]
        return sorted_cal[-1]

    @staticmethod
    def _field_skeleton(model) -> dict:
        """Model field defaults as mongoengine applies them when loading.

        Callable defaults (timestamps) become ``None``: batch scoring never
        reads them, and calling them per row would both cost time and stamp a
        meaningless value.
        """
        skeleton = {}
        for name, field in (getattr(model, "_fields", None) or {}).items():
            default = getattr(field, "default", None)
            skeleton[name] = None if callable(default) else default
        return skeleton

    def _history_window_start(self, date, max_history: int) -> datetime.datetime:
        """Start of the prefetched quote window, in *trading* days.

        ``max_history`` trading days back (plus a margin for suspended names)
        covers the deepest lookback any requested horizon needs; the per-code
        sparse fallback in ``_DayPrefetch.history`` keeps equivalence exact.
        """
        if self.calendar:
            calendar = self._sorted_calendar()
            idx = bisect.bisect_left(calendar, date)
            start_idx = max(0, idx - (max_history + 10))
            if start_idx < idx:
                return calendar[start_idx]
        return date - datetime.timedelta(days=int(max_history * 2) + 20)

    @staticmethod
    def _history_limit(config: dict) -> int:
        return max(
            config["minimum_quote_count"],
            config["breakout_lookback"],
            config["risk_lookback"],
        )

    def score_all_stocks(
        self,
        date: datetime.datetime | None = None,
        horizon: int | None = None,
        dry_run: bool = False,
        replace: bool = False,
    ) -> dict:
        """Run scoring for all active stocks on one evaluation date.

        When env DATAHUB_SCORING_MODE=ranked, delegates to the
        cross-sectional rank-normalized path (score_all_stocks_ranked).
        Default (raw) keeps the legacy component-weighted path.

        With ``batch_prefetch`` (default, perf C1) every horizon shares one
        per-day prefetch and writes through a single bulk upsert per horizon;
        ``DATAHUB_SCORING_BATCH=0`` restores the per-stock path.
        """
        if os.getenv("DATAHUB_SCORING_MODE", "raw").strip().lower() == "ranked":
            return self.score_all_stocks_ranked(
                date=date, horizon=horizon, dry_run=dry_run, replace=replace
            )
        if date is None:
            date = trading_day_helper.determine_closest_trading_date(self.calendar)
        date = normalize_date(date)

        horizons = [horizon] if horizon else list(SUPPORTED_HORIZONS)
        stocks = list(self.stock_model.objects(active_status=0))
        results = []
        skipped_complete_horizons = []
        expected_codes = [stock.code for stock in stocks]
        prefetch = None
        for current_horizon in horizons:
            if (
                not dry_run
                and not replace
                and self._is_complete_cohort(
                    stocks, date, current_horizon, scoring_mode="raw"
                )
            ):
                skipped_complete_horizons.append(current_horizon)
                self._aggregate_industry_metrics(
                    date, current_horizon, expected_codes=expected_codes
                )
                continue
            if self.batch_prefetch and prefetch is None and stocks:
                prefetch = _DayPrefetch(self, date, expected_codes, horizons)
            config = self._get_horizon_config(current_horizon)
            failed_codes = []
            pending_writes = []
            for stock in stocks:
                try:
                    if prefetch is None:
                        # Kill switch / harness reference: the legacy per-stock
                        # path reads and persists one document at a time.
                        prediction = self.score_single_stock(
                            stock,
                            date,
                            current_horizon,
                            dry_run=dry_run,
                            replace=replace,
                        )
                        results.append(prediction)
                        continue
                    existing = prefetch.existing(stock.code, current_horizon)
                    if existing is not None and not replace and not dry_run:
                        results.append(existing)
                        continue
                    payload = self._build_raw_prediction_payload(
                        stock, date, current_horizon, config, prefetch
                    )
                    results.append(payload)
                    if not dry_run:
                        pending_writes.append((payload, existing))
                except Exception as exc:
                    failed_codes.append(getattr(stock, "code", "unknown"))
                    logger.exception(
                        "Failed to score %s horizon=%s date=%s: %s",
                        getattr(stock, "code", None),
                        current_horizon,
                        date,
                        exc,
                    )

            # A batch write failure is a horizon-level failure: propagate it
            # instead of masking it as a per-code scoring error.
            self._persist_predictions_bulk(pending_writes)
            if failed_codes:
                raise RuntimeError(
                    f"scoring failed for horizon={current_horizon}: "
                    + ", ".join(failed_codes)
                )
            if not dry_run:
                self._require_complete_prediction_set(stocks, date, current_horizon)
                self._repair_blocked_predictions(date, current_horizon, expected_codes)
                self.assign_ranks(date, current_horizon, expected_codes=expected_codes)
                self._upgrade_recommendations(
                    date, current_horizon, expected_codes=expected_codes
                )
                self._aggregate_industry_metrics(
                    date, current_horizon, expected_codes=expected_codes
                )

        return {
            "date": date,
            "horizons": horizons,
            "scored_count": len(results),
            "skipped_complete_horizons": skipped_complete_horizons,
            "dry_run": dry_run,
        }

    def score_single_stock(
        self,
        stock,
        date: datetime.datetime,
        horizon: int,
        dry_run: bool = False,
        replace: bool = False,
    ):
        """Calculate one horizon-specific prediction for a stock."""
        date = normalize_date(date)
        config = self._get_horizon_config(horizon)
        existing = self._find_existing_prediction(stock.code, date, horizon)
        if existing is not None and not replace and not dry_run:
            return existing

        payload = self._build_raw_prediction_payload(stock, date, horizon, config)
        return self._persist_prediction(payload, existing, dry_run)

    def _build_raw_prediction_payload(
        self,
        stock,
        date: datetime.datetime,
        horizon: int,
        config: dict,
        prefetch: "_DayPrefetch | None" = None,
    ) -> dict:
        """Build one raw-path prediction payload without persisting it.

        Shared by score_single_stock (legacy per-stock reads) and the batched
        score_all_stocks loop so both produce byte-identical payloads.
        """
        if prefetch is not None:
            quote = prefetch.quote(stock.code)
        else:
            quote = self._get_quote_on_date(stock.code, date)
        target_date = self.get_t_plus_n_day(date, horizon)
        if not quote:
            return self._build_blocked_prediction(
                stock=stock,
                date=date,
                horizon=horizon,
                target_date=target_date,
                reason="missing_quote",
            )

        if prefetch is not None:
            factors = prefetch.factor(stock.code)
            signals = prefetch.day_signals(stock.code)
            history_quotes = prefetch.history(stock.code, self._history_limit(config))
        else:
            factors = self._get_factor_on_date(stock.code, date)
            signals = self._get_signals_on_date(stock.code, date)
            history_quotes = self._get_previous_quotes(
                stock.code, date, self._history_limit(config)
            )
        components, penalties = self._build_components(
            quote,
            factors,
            signals,
            history_quotes,
            date,
            horizon,
            config,
            stock.code,
            prefetch=prefetch,
        )
        score = self._calculate_score(components, penalties)
        recommendation = self._recommendation(score, config)
        input_snapshot = self._build_input_snapshot(
            quote=quote,
            factors=factors,
            signals=signals,
            history_quotes=history_quotes,
            config=config,
            blocked_reason=None,
        )
        input_snapshot["scoring_mode"] = "raw"
        explanation = self._build_explanation(
            horizon=horizon,
            score=score,
            components=components,
            penalties=penalties,
            config=config,
        )

        return {
            "stock": stock,
            "stock_code": stock.code,
            "stock_name": stock.name,
            "date": date,
            "horizon": horizon,
            "score": score,
            "recommendation": recommendation,
            "base_price": quote_price(quote),
            "target_date": target_date,
            "status": "PENDING",
            "explanation": explanation,
            "verification": {
                "status": "PENDING",
                "target_date": target_date.isoformat(),
                "expected_quote_count": horizon,
                "verified_quote_count": 0,
                "effective_threshold": config["effective_threshold"],
                "stop_loss_threshold": config["stop_loss_threshold"],
            },
            "input_snapshot": input_snapshot,
            "model_version": self.model_version,
        }

    def assign_ranks(
        self,
        date: datetime.datetime,
        horizon: int,
        *,
        expected_codes: list[str] | None = None,
        predictions: list | None = None,
    ) -> int:
        """Assign cross-sectional rank + percentile to a cohort.

        predictions: optional already-loaded non-BLOCKED prediction objects
        for this cohort (e.g. the objects just persisted by the ranked path).
        When provided, no DB re-read happens (perf task 2.6); the list must be
        complete for the cohort — ranking sorts internally. When None,
        predictions are loaded from the DB (raw path, unchanged behavior).
        """
        in_memory = predictions is not None
        if predictions is None:
            filters = {
                "date": normalize_date(date),
                "horizon": horizon,
                "model_version": self.model_version,
                "status__ne": "BLOCKED",
            }
            if expected_codes is not None:
                filters["stock_code__in"] = expected_codes
            predictions = list(
                self.prediction_model.objects(**filters).order_by(
                    "-score", "+stock_code"
                )
            )
        # In-memory branch: defensively exclude BLOCKED rows too (parity with
        # the DB branch's status__ne: BLOCKED) so a stray BLOCKED object can
        # never receive a rank or shift cohort percentiles.
        ranked = sorted(
            [p for p in predictions if getattr(p, "status", None) != "BLOCKED"],
            key=lambda p: (-(p.score or 0.0), p.stock_code),
        )
        total = len(ranked)
        operations = []
        for idx, prediction in enumerate(ranked, start=1):
            percentile = round(1 - ((idx - 1) / total), 4) if total else None
            if (
                getattr(prediction, "rank", None) == idx
                and getattr(prediction, "percentile", None) == percentile
            ):
                continue
            operations.append(
                UpdateOne(
                    {"_id": prediction.id},
                    {"$set": {"rank": idx, "percentile": percentile}},
                )
            )
            if in_memory:
                # Keep the caller's in-memory objects consistent with what
                # bulk_write persists, so downstream tail steps (e.g.
                # _upgrade_recommendations) can reuse them instead of
                # re-reading the cohort from Mongo (perf C5 remainder).
                prediction.rank = idx
                prediction.percentile = percentile
        if not operations:
            return 0
        result = self.prediction_model._get_collection().bulk_write(
            operations, ordered=False
        )
        return int(getattr(result, "modified_count", 0) or 0)

    def _upgrade_recommendations(
        self,
        date: datetime.datetime,
        horizon: int,
        *,
        expected_codes: list[str] | None = None,
        predictions: list | None = None,
    ) -> None:
        """Re-compute recommendations using hybrid logic after ranks are assigned.

        Called after assign_ranks() so that percentiles are available.
        Updates the recommendation field in-place for all predictions on this
        date/horizon/model_version.

        predictions: optional already-loaded non-BLOCKED prediction objects
        for this cohort (the objects assign_ranks just ranked in memory).
        When provided, no DB re-read happens (perf C5 remainder) — the list
        must be the complete non-BLOCKED cohort with fresh rank/percentile.
        When None, predictions are loaded from the DB (raw path, unchanged).
        """
        in_memory = predictions is not None
        if predictions is None:
            filters = {
                "date": normalize_date(date),
                "horizon": horizon,
                "model_version": self.model_version,
                "status__ne": "BLOCKED",
            }
            if expected_codes is not None:
                filters["stock_code__in"] = expected_codes
            predictions = list(self.prediction_model.objects(**filters))
            if not predictions:
                return

        config = self._get_horizon_config(horizon)
        bulk_ops = []
        for p in predictions:
            new_rec = self._recommendation(
                score=p.score,
                config=config,
                percentile=p.percentile,
            )
            if new_rec != p.recommendation:
                bulk_ops.append(
                    UpdateOne(
                        {"_id": p.id},
                        {"$set": {"recommendation": new_rec}},
                    )
                )
                if in_memory:
                    # Mirror the bulk write on the in-memory object so the
                    # caller's cohort stays consistent with the DB.
                    p.recommendation = new_rec

        if bulk_ops:
            result = self.prediction_model._get_collection().bulk_write(
                bulk_ops, ordered=False
            )
            updated = result.modified_count
        else:
            updated = 0

        logger.info(
            "Hybrid recommendations updated for %s h=%d: %d/%d changed",
            date.strftime("%Y-%m-%d"),
            horizon,
            updated,
            len(predictions),
        )

    def _find_existing_prediction(self, stock_code, date, horizon):
        return self.prediction_model.objects(
            stock_code=stock_code,
            date=date,
            horizon=horizon,
            model_version=self.model_version,
        ).first()

    @staticmethod
    def _prediction_matches_mode(prediction, scoring_mode: str) -> bool:
        snapshot = getattr(prediction, "input_snapshot", None) or {}
        stored_mode = snapshot.get("scoring_mode")
        if stored_mode is None:
            stored_mode = "ranked" if snapshot.get("status") == "RANKED" else "raw"
        return stored_mode == scoring_mode

    @staticmethod
    def _cohort_fingerprint(codes) -> str:
        payload = "\n".join(sorted(codes)).encode("utf-8")
        return hashlib.sha256(payload).hexdigest()

    def _cohort_predictions(self, date, horizon, expected_codes=None):
        filters = {
            "date": normalize_date(date),
            "horizon": horizon,
            "model_version": self.model_version,
        }
        if expected_codes is not None:
            filters["stock_code__in"] = expected_codes
        return list(
            self.prediction_model.objects(**filters)
            .only(
                "stock_code",
                "status",
                "score",
                "rank",
                "percentile",
                "recommendation",
                "input_snapshot",
            )
            .order_by("-score", "+stock_code")
        )

    def _require_complete_prediction_set(
        self, stocks, date, horizon, *, persisted_codes: set[str] | None = None
    ) -> None:
        """Verify every expected stock has a stored prediction for the cohort.

        persisted_codes: optional set of stock codes just written by the
        caller (ranked path, perf task 2.6). When provided, completeness is
        checked against this in-memory set instead of re-reading the cohort
        from Mongo — safe because the ranked path just persisted every
        expected code (any failure raises before this point). When None, the
        DB is queried (raw path, unchanged behavior).
        """
        expected_codes = {stock.code for stock in stocks}
        if persisted_codes is not None:
            stored_codes = persisted_codes
        else:
            stored_codes = {
                prediction.stock_code
                for prediction in self._cohort_predictions(
                    date, horizon, expected_codes
                )
            }
        missing_codes = sorted(expected_codes - stored_codes)
        if missing_codes:
            raise RuntimeError(
                f"scoring cohort incomplete for horizon={horizon}: "
                + ", ".join(missing_codes)
            )

    def _is_complete_cohort(
        self, stocks, date, horizon: int, *, scoring_mode: str
    ) -> bool:
        expected_codes = {stock.code for stock in stocks}
        predictions = self._cohort_predictions(date, horizon, expected_codes)
        by_code = {prediction.stock_code: prediction for prediction in predictions}
        incompatible = sorted(
            code
            for code in expected_codes.intersection(by_code)
            if not self._prediction_matches_mode(by_code[code], scoring_mode)
        )
        if incompatible:
            raise RuntimeError(
                f"scoring mode mismatch for horizon={horizon}; use replace: "
                + ", ".join(incompatible)
            )
        if scoring_mode == "ranked" and predictions:
            expected_fingerprint = self._cohort_fingerprint(expected_codes)
            fingerprints = {
                (getattr(prediction, "input_snapshot", None) or {}).get(
                    "cohort_fingerprint"
                )
                for prediction in predictions
            }
            if fingerprints != {expected_fingerprint}:
                raise RuntimeError(
                    f"ranked cohort membership changed for horizon={horizon}; "
                    "use replace"
                )
        if not expected_codes.issubset(by_code):
            return False

        ranked_predictions = [
            prediction
            for prediction in predictions
            if getattr(prediction, "status", None) != "BLOCKED"
        ]
        for prediction in predictions:
            if getattr(prediction, "status", None) == "BLOCKED" and (
                getattr(prediction, "rank", None) is not None
                or getattr(prediction, "percentile", None) is not None
                or getattr(prediction, "recommendation", None) != "NONE"
            ):
                return False
        total = len(ranked_predictions)
        config = self._get_horizon_config(horizon)
        for idx, prediction in enumerate(ranked_predictions, start=1):
            expected_percentile = round(1 - ((idx - 1) / total), 4) if total else None
            if (
                getattr(prediction, "rank", None) != idx
                or getattr(prediction, "percentile", None) != expected_percentile
                or prediction.recommendation
                != self._recommendation(prediction.score, config, expected_percentile)
            ):
                return False
        return True

    def _repair_blocked_predictions(self, date, horizon, expected_codes) -> int:
        blocked = list(
            self.prediction_model.objects(
                date=normalize_date(date),
                horizon=horizon,
                model_version=self.model_version,
                stock_code__in=expected_codes,
                status="BLOCKED",
            )
        )
        operations = []
        for prediction in blocked:
            changes = {}
            if getattr(prediction, "rank", None) is not None:
                changes["rank"] = None
            if getattr(prediction, "percentile", None) is not None:
                changes["percentile"] = None
            if getattr(prediction, "recommendation", None) != "NONE":
                changes["recommendation"] = "NONE"
            if changes:
                operations.append(UpdateOne({"_id": prediction.id}, {"$set": changes}))
        if not operations:
            return 0
        result = self.prediction_model._get_collection().bulk_write(
            operations, ordered=False
        )
        return int(getattr(result, "modified_count", 0) or 0)

    def _aggregate_industry_metrics(self, date, horizon, *, expected_codes) -> None:
        horizon_predictions = list(
            self.prediction_model.objects(
                date=date,
                horizon=horizon,
                model_version=self.model_version,
                stock_code__in=expected_codes,
            )
        )
        try:
            aggregate_industry_metrics(
                date=date,
                predictions=horizon_predictions,
                model_version=self.model_version,
            )
            logger.info(
                "Industry metrics aggregated for date=%s horizon=%d predictions=%d",
                date,
                horizon,
                len(horizon_predictions),
            )
        except Exception as exc:
            logger.warning(
                "Failed to aggregate industry metrics for horizon=%d: %s",
                horizon,
                exc,
            )

    def _compute_raw_components(
        self,
        stock,
        date: datetime.datetime,
        horizon: int,
        prefetch: "_DayPrefetch | None" = None,
    ) -> dict | None:
        """Compute raw component values for one stock WITHOUT persisting.

        Returns None when the stock has no quote on ``date`` (blocked).
        Otherwise returns {stock_code, components: [{id, raw_value, weight}],
        penalties: [{id, raw_value, weight}], base_price, target_date}.

        ``prefetch`` optionally supplies the C1 per-day prefetched inputs.
        """
        date = normalize_date(date)
        config = self._get_horizon_config(horizon)
        if prefetch is not None:
            quote = prefetch.quote(stock.code)
        else:
            quote = self._get_quote_on_date(stock.code, date)
        target_date = self.get_t_plus_n_day(date, horizon)
        if not quote:
            return None

        if prefetch is not None:
            factors = prefetch.factor(stock.code)
            signals = prefetch.day_signals(stock.code)
            history_quotes = prefetch.history(stock.code, self._history_limit(config))
        else:
            factors = self._get_factor_on_date(stock.code, date)
            signals = self._get_signals_on_date(stock.code, date)
            history_quotes = self._get_previous_quotes(
                stock.code, date, self._history_limit(config)
            )
        components, penalties = self._build_components(
            quote,
            factors,
            signals,
            history_quotes,
            date,
            horizon,
            config,
            stock.code,
            prefetch=prefetch,
        )
        return {
            "stock_code": stock.code,
            "stock_name": stock.name,
            "base_price": quote_price(quote),
            "target_date": target_date,
            "components": [
                {
                    "id": c["id"],
                    "raw_value": c.get("raw_value"),
                    "weight": c.get("weight", 0.0),
                }
                for c in components
            ],
            "penalties": [
                {
                    "id": p["id"],
                    # penalties put the scaled value in normalized_value
                    # (risk_penalty raw_value is plain volatility; the +1.0
                    # ST/suspended surcharge lives in normalized_value)
                    "raw_value": p.get("normalized_value", p.get("raw_value")),
                    "weight": p.get("weight", 0.0),
                }
                for p in penalties
            ],
        }

    def score_all_stocks_ranked(
        self,
        date: datetime.datetime | None = None,
        horizon: int | None = None,
        dry_run: bool = False,
        replace: bool = False,
    ) -> dict:
        """Market-wide scoring with cross-sectional component rank normalization.

        Two phases:
        1. Compute raw component values for every active stock (no writes).
        2. Rank-normalize each component across the cohort to [0, 1], then
           compute score = sum(component_rank * weight) with weights
           normalized to sum to 1. This makes scores cross-sectionally
           comparable regardless of absolute score drift.
        """
        if date is None:
            date = trading_day_helper.determine_closest_trading_date(self.calendar)
        date = normalize_date(date)

        horizons = [horizon] if horizon else list(SUPPORTED_HORIZONS)
        stocks = list(self.stock_model.objects(active_status=0))
        results = []
        skipped_complete_horizons = []
        expected_codes = [stock.code for stock in stocks]
        cohort_fingerprint = self._cohort_fingerprint(expected_codes)
        prefetch = None

        for current_horizon in horizons:
            if (
                not dry_run
                and not replace
                and self._is_complete_cohort(
                    stocks, date, current_horizon, scoring_mode="ranked"
                )
            ):
                skipped_complete_horizons.append(current_horizon)
                continue
            if self.batch_prefetch and prefetch is None and stocks:
                prefetch = _DayPrefetch(self, date, expected_codes, horizons)
            config = self._get_horizon_config(current_horizon)
            raw_by_code = {}
            blocked_codes = []
            failed_codes = []
            # Codes whose stored (non-replaced) prediction is BLOCKED from an
            # earlier run while a quote now exists. They keep their stored
            # row (no re-score under replace=False) and must never join the
            # in-memory ranking set, but they DO count toward cohort
            # completeness (any-status semantics of the DB read this set
            # replaces) — otherwise the tail's completeness check would raise
            # on a cohort the DB-based path completed fine.
            stored_blocked_codes = []
            # Per-horizon list of persisted non-BLOCKED predictions for the
            # legacy path, so its ranking tail can consume in-memory results
            # instead of re-reading the whole cohort from Mongo (perf task
            # 2.6). The batch path writes one bulk upsert per horizon and then
            # reads the persisted cohort once for the tail (C1/3.3), because a
            # bulk write never hydrates the documents it upserts.
            rankable = []
            pending_writes = []
            for stock in stocks:
                try:
                    raw = self._compute_raw_components(
                        stock, date, current_horizon, prefetch=prefetch
                    )
                    if raw is None:
                        blocked_codes.append(stock.code)
                        continue
                    raw_by_code[stock.code] = raw
                except Exception as exc:
                    failed_codes.append(getattr(stock, "code", "unknown"))
                    logger.exception(
                        "Failed to compute components for %s h=%s date=%s: %s",
                        getattr(stock, "code", None),
                        current_horizon,
                        date,
                        exc,
                    )

            if not raw_by_code and not blocked_codes:
                logger.warning(
                    "No computable components for %s h=%d; skipping",
                    date.strftime("%Y-%m-%d"),
                    current_horizon,
                )
                continue

            # --- rank-normalize each component across the cohort ---
            component_ids = sorted(
                {c["id"] for raw in raw_by_code.values() for c in raw["components"]}
            )
            penalty_ids = sorted(
                {p["id"] for raw in raw_by_code.values() for p in raw["penalties"]}
            )
            rank_maps = {}
            for cid in component_ids:
                values = {
                    code: raw["components"][
                        next(
                            i for i, c in enumerate(raw["components"]) if c["id"] == cid
                        )
                    ]["raw_value"]
                    for code, raw in raw_by_code.items()
                }
                rank_maps[cid] = self._rank_normalize(values)
            for pid in penalty_ids:
                values = {
                    code: raw["penalties"][
                        next(
                            i for i, p in enumerate(raw["penalties"]) if p["id"] == pid
                        )
                    ]["raw_value"]
                    for code, raw in raw_by_code.items()
                }
                rank_maps[pid] = self._rank_normalize(values)

            # weights per component id (same for all stocks in cohort)
            weights = {}
            for code, raw in raw_by_code.items():
                for c in raw["components"]:
                    weights.setdefault(c["id"], c["weight"])
                for p in raw["penalties"]:
                    weights.setdefault(p["id"], p["weight"])
            weight_sum = sum(weights.values()) or 1.0

            # --- build and persist scored predictions ---
            directions = config.get("directions") or {}
            # A real construction-layer flip exists only when a NON-penalty
            # component direction is negative (penalties are -1 by default, so
            # they do not indicate a flip). Flipped models keep a signed,
            # strictly sortable score: the lower clamp is removed so a full
            # flip does not collapse the whole market to a 0.0 tie. Default
            # (no flip) models MUST keep the develop floor clamp - with only
            # risk_penalty negative, stocks whose weighted component ranks sit
            # below their penalty rank (ST/fallen names) would otherwise
            # silently go negative and break bit-identical default re-runs.
            penalty_ids = set(penalty_ids)
            has_flip = any(
                float(directions.get(cid, 1.0)) < 0
                for cid in component_ids
                if cid not in penalty_ids
            )
            for code, raw in raw_by_code.items():
                score = 0.0
                for c in raw["components"]:
                    direction = float(directions.get(c["id"], 1.0))
                    score += (
                        rank_maps[c["id"]][code]
                        * (c["weight"] / weight_sum)
                        * direction
                    )
                for p in raw["penalties"]:
                    # penalties default to SUBTRACT: higher raw penalty (more
                    # volatile/ST/suspended) lowers the score, mirroring the
                    # raw path's negative penalty contribution. A direction
                    # override (e.g. construction-layer flip in research
                    # candidates) may flip this sign.
                    direction = float(directions.get(p["id"], -1.0))
                    score += (
                        rank_maps[p["id"]][code]
                        * (p["weight"] / weight_sum)
                        * direction
                    )
                # Upper clamp always applies. Lower clamp applies unless a real
                # component flip is present (see has_flip above). Semantics for
                # flipped models mirror the research evaluator
                # (h20_excess_alpha): raw weighted sum is kept signed and the
                # cohort percentile is derived from ranking that sum.
                if has_flip:
                    score = round(min(100.0, score * 100.0), 2)
                else:
                    score = round(max(0.0, min(100.0, score * 100.0)), 2)

                existing = (
                    prefetch.existing(code, current_horizon)
                    if prefetch is not None
                    else self._find_existing_prediction(code, date, current_horizon)
                )
                if existing is not None and not replace and not dry_run:
                    results.append(existing)
                    # Repair path (replace=False partial cohort): the stored
                    # row could be a BLOCKED record from an earlier run (data
                    # corrected since). BLOCKED rows must never receive ranks
                    # nor affect cohort percentiles (spec: "BLOCKED rows SHALL
                    # neither receive nor affect ranks"), so only a
                    # non-BLOCKED stored row joins the in-memory ranking set.
                    # A stored BLOCKED row still counts toward cohort
                    # completeness (see stored_blocked_codes).
                    if existing.status != "BLOCKED":
                        rankable.append(existing)
                    else:
                        stored_blocked_codes.append(code)
                    continue
                recommendation = self._recommendation(score, config)
                # persist real component values so downstream analysis
                # (factor_eval, calibration, backtest attribution) still works
                explanation_components = [
                    {
                        "id": c["id"],
                        "raw_value": c["raw_value"],
                        "weight": c["weight"],
                        "contribution": round(
                            rank_maps[c["id"]][code]
                            * (c["weight"] / weight_sum)
                            * float(directions.get(c["id"], 1.0))
                            * 100.0,
                            4,
                        ),
                    }
                    for c in raw["components"]
                ]
                explanation_penalties = [
                    {
                        "id": p["id"],
                        "raw_value": p["raw_value"],
                        "weight": p["weight"],
                        "contribution": round(
                            rank_maps[p["id"]][code]
                            * (p["weight"] / weight_sum)
                            * float(directions.get(p["id"], -1.0))
                            * 100.0,
                            4,
                        ),
                    }
                    for p in raw["penalties"]
                ]
                payload = {
                    "stock_code": code,
                    "stock_name": raw["stock_name"],
                    "date": date,
                    "horizon": current_horizon,
                    "score": score,
                    "recommendation": recommendation,
                    "base_price": raw["base_price"],
                    "target_date": raw["target_date"],
                    "status": "PENDING",
                    "explanation": {
                        "summary": "rank-normalized cross-sectional score",
                        "horizon": current_horizon,
                        "score": score,
                        "components": explanation_components,
                        "penalties": explanation_penalties,
                        "thresholds": self._thresholds(config),
                        "model_version": self.model_version,
                    },
                    "verification": {
                        "status": "PENDING",
                        "target_date": raw["target_date"].isoformat(),
                        "expected_quote_count": current_horizon,
                        "verified_quote_count": 0,
                        "effective_threshold": config["effective_threshold"],
                        "stop_loss_threshold": config["stop_loss_threshold"],
                    },
                    "input_snapshot": {
                        "status": "RANKED",
                        "scoring_mode": "ranked",
                        "cohort_fingerprint": cohort_fingerprint,
                    },
                    "model_version": self.model_version,
                }
                if prefetch is not None:
                    # Deferred: one bulk upsert per horizon (C1/3.3).
                    if not dry_run:
                        pending_writes.append((payload, existing))
                    results.append(payload)
                    continue
                try:
                    persisted = self._persist_prediction(payload, existing, dry_run)
                    results.append(persisted)
                    rankable.append(persisted)
                except Exception as exc:
                    failed_codes.append(code)
                    logger.exception(
                        "Failed to persist score for %s h=%d: %s",
                        code,
                        current_horizon,
                        exc,
                    )

            for code in blocked_codes:
                try:
                    existing = (
                        prefetch.existing(code, current_horizon)
                        if prefetch is not None
                        else self._find_existing_prediction(code, date, current_horizon)
                    )
                    if existing is not None and not replace and not dry_run:
                        continue
                    target_date = self.get_t_plus_n_day(date, current_horizon)
                    payload = self._build_blocked_prediction(
                        stock=next((s for s in stocks if s.code == code), None),
                        date=date,
                        horizon=current_horizon,
                        target_date=target_date,
                        reason="missing_quote",
                        scoring_mode="ranked",
                        cohort_fingerprint=cohort_fingerprint,
                    )
                    if prefetch is not None:
                        if not dry_run:
                            pending_writes.append((payload, existing))
                        continue
                    self._persist_prediction(payload, existing, dry_run)
                except Exception as exc:
                    failed_codes.append(code)
                    logger.warning(
                        "Failed to persist blocked prediction for %s: %s", code, exc
                    )

            # Batch path: one bulk upsert for everything computed above. A
            # write failure is a horizon failure and propagates (never masked
            # as a per-code scoring error).
            if prefetch is not None:
                self._persist_predictions_bulk(pending_writes)
            if failed_codes:
                raise RuntimeError(
                    f"ranked scoring failed for horizon={current_horizon}: "
                    + ", ".join(sorted(set(failed_codes)))
                )
            if not dry_run:
                if prefetch is not None:
                    # The bulk write does not hydrate the rows it upserts, so
                    # the tail reads the persisted cohort once (one query per
                    # horizon) and ranks exactly what is stored.
                    persisted_codes = (
                        set(raw_by_code)
                        | set(blocked_codes)
                        | set(stored_blocked_codes)
                    )
                    self._require_complete_prediction_set(
                        stocks,
                        date,
                        current_horizon,
                        persisted_codes=persisted_codes,
                    )
                    self._repair_blocked_predictions(
                        date, current_horizon, expected_codes
                    )
                    rankable = [
                        prediction
                        for prediction in self._cohort_predictions(
                            date, current_horizon, expected_codes
                        )
                        if getattr(prediction, "status", None) != "BLOCKED"
                    ]
                else:
                    # legacy path: in-memory persisted objects ARE the cohort
                    # (failed_codes is empty here), so completeness + ranking
                    # consume them instead of re-reading Mongo (perf task 2.6).
                    persisted_codes = (
                        {p.stock_code for p in rankable}
                        | set(blocked_codes)
                        | set(stored_blocked_codes)
                    )
                    self._require_complete_prediction_set(
                        stocks,
                        date,
                        current_horizon,
                        persisted_codes=persisted_codes,
                    )
                    self._repair_blocked_predictions(
                        date, current_horizon, expected_codes
                    )
                self.assign_ranks(
                    date,
                    current_horizon,
                    expected_codes=expected_codes,
                    predictions=rankable,
                )
                self._upgrade_recommendations(
                    date,
                    current_horizon,
                    expected_codes=expected_codes,
                    predictions=rankable,
                )

        return {
            "date": date,
            "horizons": horizons,
            "scored_count": len(results),
            "skipped_complete_horizons": skipped_complete_horizons,
            "dry_run": dry_run,
        }

    @staticmethod
    def _rank_normalize(values: dict) -> dict:
        """Rank-normalize a dict of code->value to code->[0,1] percentile.

        None and non-numeric (e.g. dict) values are treated as the lowest
        rank (0.0). Ties get the same rank. Normalization is over ALL codes
        (including None ones), so a code with the second-highest real value
        in a 3-code cohort gets 0.5.
        """
        codes = list(values.keys())

        def _numeric(v):
            return isinstance(v, (int, float)) and not isinstance(v, bool)

        real = {k: v for k, v in values.items() if _numeric(v)}
        n = len(codes)
        result = {}
        if real:
            sorted_real = sorted(real, key=lambda c: real[c])
            m = len(sorted_real)
            ranks = {}
            i = 0
            while i < m:
                j = i
                while j + 1 < m and real[sorted_real[j + 1]] == real[sorted_real[i]]:
                    j += 1
                # position in the FULL cohort: real values sit above None ones,
                # so the offset is (n - m)
                full_rank = n - m + i + 1
                rank_high = n - m + j + 1
                rank = (full_rank + rank_high) / 2.0
                for k in range(i, j + 1):
                    ranks[sorted_real[k]] = rank
                i = j + 1
            for code in sorted_real:
                result[code] = (ranks[code] - 1) / (n - 1) if n > 1 else 1.0
        for code in codes:
            if code not in result:
                result[code] = 0.0  # None/non-numeric values rank lowest
        return result

    def _persist_prediction(self, payload: dict, existing, dry_run: bool):
        if dry_run:
            return payload

        if existing is None:
            prediction = self.prediction_model(**payload)
        else:
            prediction = existing
            for key, value in payload.items():
                setattr(prediction, key, value)
            if payload.get("status") == "BLOCKED":
                prediction.rank = None
                prediction.percentile = None
        prediction.save()
        return prediction

    def _persist_predictions_bulk(self, writes) -> None:
        """Bulk-upsert prediction payloads (perf C1/3.3).

        ``writes`` is an iterable of ``(payload, existing)`` pairs. Every
        business field in the payload is ``$set`` on the natural key
        ``{stock_code, date, horizon, model_version}`` (the unique index), so
        the semantics match ``_persist_prediction``: blocked rows lose their
        rank/percentile, ``updated_at`` refreshes on every write and
        ``generated_at`` is stamped on insert. Rows not named by the payload
        (rank/percentile of a PENDING rewrite) are left untouched, exactly as
        the document-level save did.
        """
        writes = list(writes)
        if not writes:
            return
        now = datetime.datetime.now(datetime.UTC)
        operations = []
        for payload, _existing in writes:
            document = dict(payload)
            stock = document.get("stock")
            if stock is not None:
                # ReferenceField stores the referenced document's _id; a raw
                # bulk write must encode it the same way.
                document["stock"] = getattr(stock, "id", stock)
            if document.get("status") == "BLOCKED":
                document.setdefault("rank", None)
                document.setdefault("percentile", None)
            operations.append(
                UpdateOne(
                    {
                        "stock_code": document["stock_code"],
                        "date": document["date"],
                        "horizon": document["horizon"],
                        "model_version": document["model_version"],
                    },
                    {
                        "$set": {**document, "updated_at": now},
                        "$setOnInsert": {"generated_at": now},
                    },
                    upsert=True,
                )
            )
        self.prediction_model._get_collection().bulk_write(operations, ordered=False)

    def _build_blocked_prediction(
        self,
        stock,
        date,
        horizon,
        target_date,
        reason,
        scoring_mode="raw",
        cohort_fingerprint=None,
    ):
        config = self._get_horizon_config(horizon)
        return {
            "stock": stock,
            "stock_code": stock.code,
            "stock_name": stock.name,
            "date": date,
            "horizon": horizon,
            "score": 0.0,
            "recommendation": "NONE",
            "target_date": target_date,
            "status": "BLOCKED",
            "explanation": {
                "summary": f"Scoring blocked: {reason}",
                "horizon": horizon,
                "score": 0.0,
                "components": [],
                "penalties": [],
                "thresholds": self._thresholds(config),
            },
            "verification": {
                "status": "BLOCKED",
                "target_date": target_date.isoformat(),
                "expected_quote_count": horizon,
                "verified_quote_count": 0,
            },
            "input_snapshot": {
                "status": "BLOCKED",
                "scoring_mode": scoring_mode,
                "cohort_fingerprint": cohort_fingerprint,
                "blocked_reason": reason,
                "quote": {"status": "missing"},
                "factor": {"status": "unknown"},
                "signals": {"status": "unknown"},
            },
            "model_version": self.model_version,
        }

    def _build_components(
        self,
        quote,
        factors,
        signals,
        history_quotes,
        date,
        horizon,
        config,
        stock_code,
        prefetch: "_DayPrefetch | None" = None,
    ):
        import datetime as dt

        weights = config["weights"]
        momentum_quotes = history_quotes[: config["momentum_lookback"]]
        breakout_quotes = history_quotes[: config["breakout_lookback"]]
        risk_quotes = history_quotes[: config["risk_lookback"]]

        # Belt-and-braces: both read paths already impose ``_signal_order_key``
        # (see _get_signals_on_date / _DayPrefetch.day_signals), so this keeps a
        # direct caller with an unsorted list deterministic too.
        signals = sorted(signals, key=_signal_order_key)

        # Signal persistence decay: when today has no bullish signal, look back
        decay_max_days = config.get("signal_decay_max_days", 5)
        decay_factor = config.get("signal_decay_factor", 0.7)
        days_since_signal: int | None = None
        last_signal_strengths: list[float] | None = None
        last_signal_names: list[str] | None = None

        has_bullish_today = any(
            getattr(s, "direction", None) == "BULLISH"
            and getattr(s, "signal_name", None)
            for s in signals
        )

        if not has_bullish_today and decay_max_days > 0:
            if prefetch is not None:
                # One prefetched decay window serves every stock/horizon; the
                # days_since_signal gate below keeps a wider window equivalent
                # to the per-horizon query.
                recent = prefetch.decay_signals(stock_code)
            else:
                lookback_start = date - dt.timedelta(days=decay_max_days + 1)
                recent = list(
                    self.signal_model.objects(
                        stock_code=stock_code,
                        date__gte=normalize_date(lookback_start),
                        date__lt=normalize_date(date),
                    ).order_by("-date")
                )
            # Group by date, find most recent date with bullish signals
            by_date: dict = {}
            for sig in recent:
                if getattr(sig, "direction", None) == "BULLISH" and getattr(
                    sig, "signal_name", None
                ):
                    d = getattr(sig, "date", None)
                    if d:
                        d_norm = d.replace(hour=0, minute=0, second=0, microsecond=0)
                        by_date.setdefault(d_norm, []).append(sig)

            if by_date:
                most_recent_date = max(by_date.keys())
                days_since_signal = (normalize_date(date) - most_recent_date).days
                if days_since_signal <= decay_max_days:
                    # Same canonical ordering as the live signals above: the
                    # decay strengths feed the component's evidence, so the
                    # per-stock and batch reads must agree on the list order.
                    decayed = sorted(by_date[most_recent_date], key=_signal_order_key)
                    last_signal_strengths = [
                        float(getattr(s, "strength", 1.0) or 1.0) for s in decayed
                    ]
                    last_signal_names = [
                        getattr(s, "signal_name", None) for s in decayed
                    ]
                else:
                    days_since_signal = None

        components = [
            signal_strength_component(
                signals,
                weights["signal_strength"],
                days_since_signal=days_since_signal,
                last_signal_strengths=last_signal_strengths,
                last_signal_names=last_signal_names,
                decay_factor=decay_factor,
            ),
            trend_alignment_component(
                quote, factors, horizon, weights["trend_alignment"]
            ),
            momentum_component(
                quote,
                momentum_quotes,
                config["momentum_lookback"],
                weights["momentum"],
            ),
            breakout_or_position_component(
                quote, breakout_quotes, weights["breakout_or_position"]
            ),
            relative_strength_component(
                quote,
                history_quotes[: config["momentum_lookback"]],
                weights["relative_strength"],
            ),
            real_relative_strength_component(
                stock_code=stock_code,
                quote=quote,
                history_quotes=history_quotes,
                weight=weights.get("real_relative_strength", 0.0),
                lookback=config.get("momentum_lookback", 10),
                index_quotes=(
                    prefetch.index_quotes_for(stock_code, history_quotes, quote)
                    if prefetch is not None
                    else None
                ),
            ),
            industry_momentum_component(
                stock_code=stock_code,
                date=date,
                horizon=horizon,
                weight=weights.get("industry_momentum", 0.0),
                model_version=self.model_version,
                industry_lookup=(
                    prefetch.industry_lookup(horizon) if prefetch is not None else None
                ),
            ),
        ]
        penalties = [risk_penalty(quote, risk_quotes, weights["risk_penalty"])]
        return components, penalties

    def _get_horizon_config(self, horizon: int) -> dict:
        return get_effective_horizon_config(horizon, self.scoring_config)

    def _calculate_score(self, components: list, penalties: list) -> float:
        score = sum(item["contribution"] for item in components)
        score += sum(item["contribution"] for item in penalties)
        return round(max(0.0, min(100.0, score)), 2)

    def _recommendation(
        self, score: float, config: dict, percentile: float | None = None
    ) -> str:
        """Determine recommendation using cross-sectional percentile.

        When percentile is available (post-ranking), the recommendation is
        driven by the cohort percentile alone — BUY = top buy_percentile,
        WATCH = top watch_percentile, AVOID = bottom avoid_percentile. The
        absolute score thresholds are NOT required, because absolute scores
        drift with weight configuration and lose meaning across cohorts.

        Without percentile (single-stock path), falls back to pure absolute
        thresholds.
        """
        buy_abs = config["buy_threshold"]
        watch_abs = config["watch_threshold"]
        avoid_abs = config.get("avoid_threshold", 20.0)

        if percentile is not None and percentile > 0:
            buy_pct = config.get("buy_percentile", 0.95)
            watch_pct = config.get("watch_percentile", 0.80)
            avoid_pct = config.get("avoid_percentile", 0.20)

            # BUY: top buy_percentile of the cohort
            if percentile >= buy_pct:
                return "BUY"
            # WATCH: next band down to watch_percentile
            if percentile >= watch_pct:
                return "WATCH"
            # AVOID: bottom avoid_percentile
            if percentile <= avoid_pct:
                return "AVOID"
            return "NONE"
        else:
            # Fallback: pure absolute thresholds (single-stock path)
            if score >= buy_abs:
                return "WATCH"  # placeholder — will be upgraded post-ranking
            if score >= watch_abs:
                return "WATCH"
            if score <= avoid_abs:
                return "AVOID"
            return "NONE"

    def _build_explanation(self, horizon, score, components, penalties, config):
        positive = [
            item["label"]
            for item in components
            if item["contribution"] > 0 and item["weight"] > 0
        ]
        if positive:
            summary = "; ".join(positive[:3])
        else:
            summary = "No strong positive scoring evidence."
        return {
            "summary": summary,
            "horizon": horizon,
            "score": score,
            "components": components,
            "penalties": penalties,
            "thresholds": self._thresholds(config),
            "model_version": self.model_version,
        }

    def _thresholds(self, config):
        return {
            "buy": config["buy_threshold"],
            "watch": config["watch_threshold"],
            "avoid": config.get("avoid_threshold", 20.0),
            "effective_return": config["effective_threshold"],
            "stop_loss": config["stop_loss_threshold"],
        }

    def _build_input_snapshot(
        self, quote, factors, signals, history_quotes, config, blocked_reason
    ):
        quote_date = getattr(quote, "date", None)
        factor_date = getattr(factors, "date", None)
        signal_dates = [getattr(signal, "date", None) for signal in signals]
        has_enough_history = len(history_quotes) >= config["minimum_quote_count"]
        status = "READY" if quote and has_enough_history else "PARTIAL"
        if blocked_reason:
            status = "BLOCKED"
        return {
            "status": status,
            "blocked_reason": blocked_reason,
            "quote": {
                "status": "ready" if quote else "missing",
                "date": quote_date.isoformat() if quote_date else None,
            },
            "factor": {
                "status": "ready" if factors else "missing",
                "date": factor_date.isoformat() if factor_date else None,
            },
            "signals": {
                "status": "ready" if signals else "missing",
                "count": len(signals),
                "dates": [date.isoformat() for date in signal_dates if date],
            },
            "history": {
                "status": "ready" if has_enough_history else "partial",
                "quote_count": len(history_quotes),
                "minimum_quote_count": config["minimum_quote_count"],
            },
        }

    def _get_quote_on_date(self, stock_code, date):
        return self.quote_model.objects(
            code=stock_code, date=normalize_date(date)
        ).first()

    def _get_factor_on_date(self, stock_code, date):
        return self.factor_model.objects(
            stock_code=stock_code, date=normalize_date(date)
        ).first()

    def _get_signals_on_date(self, stock_code, date):
        # ``stock_signal_daily`` is unique on (stock_code, date, signal_name) and
        # unsorted, so the read order is whatever the index plan returns. Order
        # is a persisted field (explanation evidence + input_snapshot dates), so
        # pin it — the batch path reads the same rows with ``stock_code__in`` and
        # would otherwise disagree on 0.5% of the cohort.
        return sorted(
            self.signal_model.objects(stock_code=stock_code, date=normalize_date(date)),
            key=_signal_order_key,
        )

    def _get_previous_quotes(self, stock_code, date, limit):
        return list(
            self.quote_model.objects(code=stock_code, date__lt=normalize_date(date))
            .order_by("-date")
            .limit(limit)
        )
