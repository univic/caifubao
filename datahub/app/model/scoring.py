# -*- coding: utf-8 -*-
"""Shared contract model: re-exported from ``caifubao_contracts.models.scoring``.

The definition lives in ``shared/`` so the backend and the datahub service use
one schema; this module only keeps the historical ``app.model.scoring`` import
path working.
"""

from caifubao_contracts.models.scoring import (  # noqa: F401
    StockDailyScore,
    StockScorePrediction,
    ScoreExperiment,
    ScoreModelVersion,
)
