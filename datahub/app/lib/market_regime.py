# -*- coding: utf-8 -*-
"""Market regime classifier.

Classifies each trading day into bull, bear, or sideways using CSI 300
60-day price trend. Used by factor evaluation regime splits (15.5) and
rolling validation regime reporting (17.3).
"""

import datetime
import logging

from app.model.stock import StockDailyQuote

logger = logging.getLogger(__name__)

# The single cached service instance.  Callers that want a shared
# instance (to reuse loaded data) should use get_service().
_instance = None


def normalize_date(value: datetime.datetime) -> datetime.datetime:
    return value.replace(hour=0, minute=0, second=0, microsecond=0)


class MarketRegimeService:
    """Reusable market-regime classifier backed by CSI 300 daily quotes.

    API
    ---
    classify_date(dt) -> str
    classify_range(start, end) -> dict[isoformat, regime]
    get_regime_stats(start, end) -> dict

    Classification rule (matches backend /regime endpoint):
        * 60-day CSI 300 change > +10% → "bull"
        * 60-day CSI 300 change < -10% → "bear"
        * otherwise                    → "sideways"
        * insufficient data            → "unknown"
    """

    REGIME_BULL = "bull"
    REGIME_BEAR = "bear"
    REGIME_SIDEWAYS = "sideways"
    REGIME_UNKNOWN = "unknown"

    def __init__(
        self,
        index_code: str = "sh000300",
        lookback_days: int = 60,
        bull_threshold: float = 0.10,
        bear_threshold: float = -0.10,
        quote_model=None,
    ):
        self.index_code = index_code
        self.lookback_days = lookback_days
        self.bull_threshold = bull_threshold
        self.bear_threshold = bear_threshold
        self.quote_model = quote_model or StockDailyQuote

        # Cached CSI 300 prices keyed by normalized date.
        self._prices: dict | None = None
        self._loaded_range: tuple | None = None  # (start, end) last loaded

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def classify_date(self, dt: datetime.datetime) -> str:
        """Classify a single date. Returns 'bull', 'bear', 'sideways', or 'unknown'."""
        day = normalize_date(dt)
        return self._classify(day)

    def classify_range(
        self,
        start_date: datetime.datetime,
        end_date: datetime.datetime,
    ) -> dict:
        """Return dict mapping date.isoformat() → regime string.

        Only trading days that have CSI 300 data are included.
        """
        start = normalize_date(start_date)
        end = normalize_date(end_date)
        self._ensure_loaded(start, end)

        result: dict = {}
        current = start
        while current <= end:
            regime = self._classify(current)
            # Include the date even if unknown, so callers see all days.
            result[current.isoformat()] = regime
            # Step to the next trading day that appears in our cache.
            # If there is no price at all for this day we try the next day
            # in the range so we do not loop forever.
            current = self._next_trading_day(current)
            if current is None:
                break
            if current > end:
                break

        return result

    def get_regime_stats(
        self,
        start_date: datetime.datetime,
        end_date: datetime.datetime,
    ) -> dict:
        """Return summary counts for each regime in the range.

        Returns:
            {"bull_days": N, "bear_days": N, "sideways_days": N,
             "unknown_days": N, "total": N}
        """
        mapping = self.classify_range(start_date, end_date)
        counts = {
            "bull_days": 0,
            "bear_days": 0,
            "sideways_days": 0,
            "unknown_days": 0,
        }
        for regime in mapping.values():
            key = f"{regime}_days"
            counts[key] = counts.get(key, 0) + 1
        counts["total"] = len(mapping)
        return counts

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _ensure_loaded(self, start: datetime.datetime, end: datetime.datetime) -> None:
        """Load CSI 300 data covering *start* through *end* with enough
        buffer for the lookback window.  Reuses cached data when the
        requested range is already covered."""
        # Determine a safe lookback buffer: lookback + 30 extra days
        # for weekends/holidays — same heuristic as the backend.
        buffer_days = self.lookback_days + 30
        load_start = start - datetime.timedelta(days=buffer_days)
        load_end = end

        if self._loaded_range is not None:
            cached_start, cached_end = self._loaded_range
            if cached_start <= load_start and cached_end >= load_end:
                return  # already have enough data

        logger.info(
            "Loading CSI 300 quotes from %s to %s",
            load_start.isoformat(),
            load_end.isoformat(),
        )
        quotes = list(
            self.quote_model.objects(code=self.index_code)
            .filter(date__gte=load_start, date__lte=load_end)
            .order_by("date")
        )
        if not quotes:
            logger.warning(
                "No CSI 300 quotes found for range [%s, %s]",
                load_start.isoformat(),
                load_end.isoformat(),
            )

        prices: dict = {}
        for quote in quotes:
            day = normalize_date(quote.date)
            price = quote.close_hfq or quote.close
            if price and price > 0:
                prices[day] = float(price)

        self._prices = prices
        self._loaded_range = (load_start, load_end)

    def _classify(self, day: datetime.datetime) -> str:
        """Core classification logic for a single day."""
        if self._prices is None:
            self._ensure_loaded(day, day)

        assert self._prices is not None  # always set by _ensure_loaded

        # Find current price (on or before *day*, tolerance of 7 days).
        current_price = self._find_price(day, search_backward=True)
        if current_price is None:
            return self.REGIME_UNKNOWN

        # Find lookback price (around day - lookback_days, tolerance ±7 days).
        lookback_day = day - datetime.timedelta(days=self.lookback_days)
        lookback_price = self._find_price(lookback_day, search_forward=True)
        if lookback_price is None or lookback_price == 0:
            return self.REGIME_UNKNOWN

        change = (current_price - lookback_price) / lookback_price
        if change > self.bull_threshold:
            return self.REGIME_BULL
        elif change < self.bear_threshold:
            return self.REGIME_BEAR
        else:
            return self.REGIME_SIDEWAYS

    def _find_price(
        self,
        day: datetime.datetime,
        *,
        search_backward: bool = False,
        search_forward: bool = False,
    ) -> float | None:
        """Find a CSI 300 price on or near *day*.

        If *search_backward* is True, looks back up to 7 days (day, day-1,
        …, day-6).  If *search_forward* is True, looks forward up to 7
        days (day, day+1, …, day+6).  At least one direction must be True.
        """
        if not self._prices:
            return None

        for offset in range(7):
            check_day = day - datetime.timedelta(days=offset)
            if search_backward and check_day in self._prices:
                return self._prices[check_day]

            if search_forward:
                fwd_day = day + datetime.timedelta(days=offset)
                if fwd_day in self._prices:
                    return self._prices[fwd_day]

        return None

    def _next_trading_day(self, current: datetime.datetime) -> datetime.datetime | None:
        """Return the next day after *current* that appears in _prices,
        or None if there is no such day."""
        if not self._prices:
            return None
        # Simple scan forward up to 30 days.  In practice this is plenty
        # for mainland China markets (at most 10 non-trading days in a row).
        for offset in range(1, 31):
            candidate = current + datetime.timedelta(days=offset)
            if candidate in self._prices:
                return candidate
        return None


def get_service(**kwargs) -> MarketRegimeService:
    """Return the cached singleton MarketRegimeService instance.

    Keyword arguments are forwarded to MarketRegimeService() on first call.
    After the first call, the cached instance is returned regardless of
    kwargs (for thread safety simplicity).
    """
    global _instance
    if _instance is None:
        _instance = MarketRegimeService(**kwargs)
    return _instance
