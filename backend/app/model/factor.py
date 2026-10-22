# -*- coding: utf-8 -*-
"""Shared contract model: re-exported from ``caifubao_contracts.models.factor``.

The definition lives in ``shared/`` so the backend and the datahub service use
one schema; this module only keeps the historical ``app.model.factor`` import
path working.
"""

from caifubao_contracts.models.factor import (  # noqa: F401
    FactorDataEntry,
    StockFactorDaily,
)
