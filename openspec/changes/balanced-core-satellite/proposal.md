# Balanced 50k core-satellite, degraded pipeline semantics and replay viewer

## Why

The research programme has frozen a candidate composite (`riq_v1`) and a
balanced small-account structure, but the operating chain has three gaps that
block a credible 50k workflow:

1. **A two-symbol quote validation failure stops the whole daily chain.**
   On 2026-10-19 the stock quote job wrote 5,212/5,575 rows and then failed
   because two symbols did not validate; signal and scoring were skipped by the
   dependency gate even though the market-wide coverage was 99.96%. Operators
   have no explicit "degraded but usable" state.
2. **There is no replay artifact the UI can show.** Research outputs are
   parquet/JSONL on a research volume; the frontend can show a legacy
   single-stock backtest but not the composite's decisions, fills and daily NAV.
3. **The 50k account layer is missing.** Target weights exist, but no plan
   converts them into board-legal lots, fee-aware cash, blocked orders and a
   reconciliation-ready order list for a 5 万 account.
4. **The research panel is survivorship-biased.** The 2020–2026 panel contains
   no delisted stock and its ST flag is a current snapshot, so 30%+ historical
   returns cannot be treated as expected returns.

The frozen evidence supports a **balanced 50/40/10** structure, but that exact
composition is **new**: the tested static core was fully invested (40/30/30 or
four-leg 25/25/25/25), while this change puts 40% into the `riq_v1` satellite and
10% into cash. At ¥50k the core sleeve is the lot-feasible **three-ETF 40/30/30**
variant (CSI 300 / gold / Nasdaq); the four-leg 25/25/25/25 variant is refused at
¥50k because one treasury lot exceeds a ¥6,250 leg, and is deferred to AUM
≥ ¥100k. This change builds the smallest end-to-end slice that makes that
structure observable and operable without placing real orders.

## What Changes

- **C1 — degraded pipeline semantics.** Add `DEGRADED` as a persisted job state;
  tolerate a bounded number of stock quote validation failures while retaining
  the strict failure for a source outage; accept `DEGRADED` downstream only when
  coverage and factor evidence meet an explicit threshold; treat unavailable
  non-core index history as a reported skip, not a circuit-breaker failure.
- **C2 — replay artifact and forward visibility.** Add a read-only
  `StrategyReplay` research artifact (curve, decisions, fills, metrics) and a
  list/detail API; expose `forward_sessions` as `0/120` for REPLAY artifacts and
  never count replay output as forward evidence.
- **C3 — 50k execution plan.** Add a pure plan builder and CLI that converts
  target weights plus an account snapshot into sell-first/buy-second,
  board-legal, fee-aware orders with cash reconciliation.
- **C4 — survivorship/ST audit and research backfill.** Add a read-only audit and
  an operator-run research backfill for delisted instruments and historical ST
  names; expose coverage and residual bias in the replay artifact. No production
  collection is mutated.
- **C5 — balanced core-satellite replay.** Add a pure core/satellite combiner
  (three-ETF 40/30/30 core at ¥50k; 25/25/25/25 only at ≥ ¥100k; RIQ
  satellite; cash buffer; quarterly rebalance) and a
  research backtest CLI that fetches the four ETF leg prices, combines the
  sleeves and emits a replay artifact.
- **Frontend — strategy replay view.** Add a page that shows the complete
  process: configuration, daily NAV and drawdown curve, per-rebalance decisions
  with targets/adds/removes, fills and costs, metrics and the forward-progress
  badge.

## Non-goals

- No broker adapter, order placement, account mutation or automatic rebalancing.
- No claim that a REPLAY artifact is forward evidence; the 120-session gate is
  unchanged and remains score-model / certified-window only.
- No factor search, parameter tuning, model promotion or change to `riq_v1`.
- No QDII premium/limit model in the first core-satellite slice; it is disclosed
  as a limitation.
- No production `basic_stock`/quote mutation by the survivorship backfill.

## Module Impact

- `datahub/`: job status and dependency semantics, 50k planner, replay artifact
  builder, core-satellite combiner, research survivorship audit/backfill.
- `backend/`: read-only replay list/detail API.
- `frontend/`: strategy replay list/detail views and navigation.
- `openspec/`: this change; `docs/`: runbook and limitations.
