# -*- coding: utf-8 -*-
"""Shared contract model: re-exported from ``caifubao_contracts.models.industry``.

The definition lives in ``shared/`` so the backend and the datahub service use
one schema; this module only keeps the historical ``app.model.industry`` import
path working.
"""

from caifubao_contracts.models.industry import (  # noqa: F401
    StockIndustryClassification,
    IndustryDailyMetrics,
)
