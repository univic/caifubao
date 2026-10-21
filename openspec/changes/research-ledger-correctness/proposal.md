# Research ledger correctness (P0)

## Why

The 2026-10-21 audit of the weekly/monthly replay found that the reported
−98.9% / −93.7% cumulative results were not a clean strategy verdict:

- the replay filled at D+1 open but then marked the same book at D close;
- turnover used a compounded average NAV while each fold had reset to 50k;
- the target capped the basket at 10 names but did not cap actual residual
  holdings (weekly 20, monthly 16);
- the delisted write-off statistic omitted `lots × 100`;
- 21 consecutive missing sessions were treated as a zero-value delisting;
- known delisting-arrangement names were eligible for new entries.

After correcting only the execution order and excluding known
delisting-arrangement names, the monthly five-factor book was +15.15%/yr but
with a −42.81% drawdown, 59.18% positive months and up to 12 actual holdings. It
therefore still fails the 15% / 30% / 55% / 10-name target; it is a lead, not a
strategy.

## What Changes

- Add a research-only ledger with causal T+1 execution: decide at D close,
  fill at D+1 open, mark at D+1 close.
- Enforce a strict actual-position cap; blocked exits block new entries.
- Support one continuous account (initial cash/positions) rather than silently
  resetting capital per fold.
- Model corporate actions (share multipliers and cash per share), explicit
  delisting exits, and write-off values with the board-lot multiplier.
- Report turnover against both initial capital and average NAV, and never infer
  a zero-value delisting from missing quotes alone.
- Add synthetic tests for each invariant. No production API, scoring, scheduler
  or order-router change.

## Non-goals

- No claim that a strategy is profitable; no real orders.
- No new timing-threshold search; the P0 deliverable is a correct ledger.
- No deletion of subsequently delisted names: their realised P&L stays in the
  account, while only decision-time-known delisting-arrangement names are
  blocked from new entries.
