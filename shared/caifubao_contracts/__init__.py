# -*- coding: utf-8 -*-
"""Shared contracts between the backend API and the datahub service.

Both images install this package, so the MongoDB models and the execution-cost
assumptions have exactly one definition (no more mirrored copies and parity
tests that only catch drift after the fact).
"""

from caifubao_contracts import friction, json_util, time  # noqa: E402,F401

__all__ = ["friction", "json_util", "time"]
