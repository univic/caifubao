# Strategy Exit Hysteresis and Explicit Sell Reasons

## Why

The research paper book re-sorts its whole book every session: on 2026-10-27 it sold
11 of 20 names that were still at the 0.910–0.994 percentile of 5,578 stocks and
bought names at 0.995–1.000. With a 0.302% round-trip cost model that is paying a
round trip to move a tenth of a percentile — measured friction ≈ 42%/year at the
observed turnover.

A 2026-10-27 research scan ([`docs/autoresearch/runs/small-book/holding-scan-and-convertible-bonds-2026-10-27.md`](../../../docs/autoresearch/runs/small-book/holding-scan-and-convertible-bonds-2026-10-27.md))
on the frozen 2020–2026 panel shows the exit band is the lever that matters: going
from a daily full re-sort to quarterly + a 2× exit band cuts annual turnover
11.8 → 2.25, friction 3.6% → 0.7%/yr and narrows excess drawdown −25.8% → −11.4%
(annual excess +27.1% → +35.2% on the same panel).

The spec can currently not express any of this: `rebalance.cadence_days` is validated
but never consumed (documented in `docs/capability-inventory.md`), and there is no
exit band, no minimum holding period and no recorded reason for a sale.

## Capabilities

- `openspec/changes/strategy-exit-hysteresis/specs/strategy-engine/spec.md`
  (ADDED requirements on the strategy engine's selection/rebalance semantics).

## What Changes

- Two **optional** spec fields, both consumed by the planner:
  - `rebalance.exit_buffer` (number ≥ 1.0): a held name stays while its rank is
    inside `ceil(portfolio_size × exit_buffer)`;
  - `rebalance.min_hold_sessions` (integer ≥ 0): a position younger than this is not
    churned out while it still carries a usable score.
- Every rebalance records a per-code reason (`held_in_exit_band`, `held_min_period`,
  `sold_out_of_exit_band`, `sold_ineligible`, `added_by_rank`) and every holding
  carries `{hold_count, entry_reason}` in a new `hold_meta` document field.
- The reason map is persisted as `exit_reasons`, deliberately **outside**
  `rebalance`, so that field keeps its exact `added/removed/unchanged` shape.

## Non-goals

- No new default: a spec that does not declare the two fields keeps byte-identical
  selection behaviour, the same normalized spec, the same legacy config and the same
  spec hash (the certified research window `c1310492…` is pinned by a regression
  test).
- No change to FORWARD classification, window certification, NAV, scoring, or the
  120-session evidence rules; no automatic window opening.
- No `cadence_days` implementation (that field stays validated-but-unconsumed and is
  tracked separately).
- No replacement-margin threshold yet; it needs its own measurement first.

## Impact

- `shared/caifubao_contracts/strategy_spec.py` (allow/validate/strip on the legacy
  projection), `datahub/app/lib/strategy_engine/{selection,runner}.py`,
  `datahub/app/jobs/{strategy_runner,strategy_forward_daily}.py`,
  `datahub/app/model/strategy.py`, tests and this change record.
- Existing specs, certified windows and recorded runs are unaffected: the new fields
  are absent from the running spec, and `hold_meta`/`exit_reasons` are empty on
  legacy runs.
