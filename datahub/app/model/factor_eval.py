# -*- coding: utf-8 -*-
"""Shared contract model: re-exported from ``caifubao_contracts.models.factor_eval``.

The definition lives in ``shared/`` so the backend and the datahub service use
one schema; this module only keeps the historical ``app.model.factor_eval`` import
path working.
"""

from caifubao_contracts.models.factor_eval import (  # noqa: F401
    FactorEvalReport,
)
