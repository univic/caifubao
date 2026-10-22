# -*- coding: utf-8 -*-
"""Shared contract model: re-exported from ``caifubao_contracts.models.strategy_replay``.

The definition lives in ``shared/`` so the backend and the datahub service use
one schema; this module only keeps the historical ``app.model.strategy_replay`` import
path working.
"""

from caifubao_contracts.models.strategy_replay import (  # noqa: F401
    StrategyReplay,
)
