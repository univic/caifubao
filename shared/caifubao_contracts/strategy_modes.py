# -*- coding: utf-8 -*-
"""Fail-closed execution-domain routing; no runtime imports or side effects."""

RESEARCH_MODES = frozenset({"BACKTEST", "REPLAY", "RESEARCH_SIMULATION"})
TRADING_MODES = frozenset({"TRADING_PAPER", "LIVE"})


def strategy_domain(mode: str) -> str:
    if mode in RESEARCH_MODES:
        return "research"
    if mode in TRADING_MODES:
        return "trading"
    raise ValueError("unsupported strategy execution mode")


def require_research_mode(mode: str) -> None:
    if strategy_domain(mode) != "research":
        raise ValueError("TRADING_PAPER/LIVE require an independent trading runtime")
