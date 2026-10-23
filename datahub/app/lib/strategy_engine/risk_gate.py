# -*- coding: utf-8 -*-
"""Research compatibility facade for the shared, pure portfolio risk gate.

Domain-local research passes its own ledger snapshot and an explicit market
snapshot; trading runs its own authoritative gate using the shared contract.
No database, broker, or operational halt store is imported here.
"""

from caifubao_contracts.strategy_risk import evaluate_risk

__all__ = ["evaluate_risk"]
