# Design: Small-account strategy research tools

## Holding scan

The scan consumes a frozen factor-lab panel and its existing `fwd_h{h}` labels.
It does not rebuild or roll blocked labels.  Rebalance dates are every `h`
sessions inside the requested panel.  A current holding stays in the target
book while its rank remains inside the wider exit band; vacancies are filled
from the entry band.  Therefore `buffer > 1` must be capable of changing the
book relative to `buffer = 1`.

Turnover is the one-way replaced fraction between consecutive target books.
Round-trip friction is charged only to that replaced fraction; the initial book
has turnover 1.  The equal-weight control uses the same dates, resolvable-label
universe, turnover definition, and friction rule.  A missing/blocked label is
not rolled to another date and is excluded from both target and control for that
measurement.

## ETF and small-book research replay

The ETF pool command freezes one explicitly dated liquidity snapshot. It does
not reconstruct delisted instruments or historical membership, so using that
pool over earlier dates is a current-snapshot replay, not a verified
point-in-time universe. ETF panel labels use observed per-instrument quote rows
and therefore are exploratory when an exchange calendar is not supplied. A
signal known at a session close fills no earlier than the next session's close
in the close-only simulator. Schedules are anchored to the requested simulation
window so adding leading history cannot move rebalance dates. Historical
results are research measurements, not investment advice.

## Ledger boundary

The local JSONL ledger is append-only at the helper boundary and records copied
inputs so later caller mutation cannot rewrite an emitted entry.  Every entry
created from a historical/as-of replay is `evidence_kind=REPLAY`.  This ledger
is not the immutable forward-capture contract and none of its rows count toward
the production 120-session gate.

## Deployment boundary

The tools remain operator-invoked research commands.  They do not write Mongo,
schedule themselves, expose a REST endpoint, or emit executable orders.
