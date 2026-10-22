# -*- coding: utf-8 -*-
"""Shared contract model: re-exported from ``caifubao_contracts.models.signal``.

The definition lives in ``shared/`` so the backend and the datahub service use
one schema; this module only keeps the historical ``app.model.signal`` import
path working.
"""

from caifubao_contracts.models.signal import (  # noqa: F401
    Signal,
    SignalData,
    StockSignalDaily,
)
