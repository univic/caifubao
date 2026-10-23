# -*- coding: utf-8 -*-
"""Shared contract model: re-exported from ``caifubao_contracts.models.stock``.

The definition lives in ``shared/`` so the backend and the datahub service use
one schema; this module only keeps the historical ``app.model.stock`` import
path working.
"""

from caifubao_contracts.models.stock import (  # noqa: F401
    StockExchange,
    MarketOverview,
    FinanceMarket,
    StockDailyQuote,
    DailyQuote,
    StockDataCapabilities,
    BasicStock,
    ExchangeTradedFund,
    IndividualStock,
    StockIndex,
    ValuationIndicator,
    FinancialStatus,
)
