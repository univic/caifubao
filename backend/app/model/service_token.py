# -*- coding: utf-8 -*-
"""Shared contract model: re-exported from ``caifubao_contracts.models.service_token``.

The definition lives in ``shared/`` so the backend and the datahub service use
one schema; this module only keeps the historical ``app.model.service_token`` import
path working.
"""

from caifubao_contracts.models.service_token import (  # noqa: F401
    ServiceToken,
)
