# -*- coding: utf-8 -*-
"""Research compatibility adapter for the shared StrategySpec contract.

Do not import this module from trading: legacy runner and Mongo live in the
research deployment. Trading consumes only the shared immutable contract.
"""

from __future__ import annotations

from caifubao_contracts.strategy_spec import (
    migrate_legacy_config,
    spec_hash,
    to_legacy_config,
    validate_strategy_spec,
)

from app.lib.strategy_engine.config import validate_strategy_config


def legacy_to_spec(config: dict) -> dict:
    """Migrate a validated legacy config without altering historical hash."""
    return migrate_legacy_config(validate_strategy_config(config))


def spec_to_legacy_config(spec: dict) -> dict:
    """Convert supported strategy semantics to the existing research runner."""
    return validate_strategy_config(to_legacy_config(validate_strategy_spec(spec)))


__all__ = [
    "legacy_to_spec", "spec_to_legacy_config", "spec_hash",
    "validate_strategy_spec",
]
