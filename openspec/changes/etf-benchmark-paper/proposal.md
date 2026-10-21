# Single-ETF paper benchmark

## Why

The 100,000 CNY small-account roadmap needs an auditable buy-and-hold baseline
before adding active signals. The current strategy runner requires stock scores;
the ETF research panel is not a cash/units ledger and omits minimum commissions.

## What Changes

- Add a deterministic, local-file-only single domestic equity ETF replay.
- Size one initial purchase at the next supplied market session's raw open,
  including explicit ETF commission, minimum fee, slippage and lot rounding.
- Report decisions, blocked fills, cash, units, fees, valuation freshness and
  end-state reconciliation artifacts without changing stock scoring or NAV.
- Expose `scripts/caifubao strategy benchmark` locally, without cluster access.
- Adapt frozen raw ETF daily exports, complete exchange calendars and independent
  opening execution declarations; attach full-source provenance to source replay.

## Impact

Only datahub's strategy library, its tests, the unified CLI and public usage
documentation. No database writes, account API changes, broker orders, scheduler
or model promotion. Outputs are REPLAY, never forward evidence.

## Non-goals

ETF selection, automatic data fetching, dividends/splits, live accounts, strategy
optimisation and UI work. Inputs must explicitly declare an action-free interval;
non-empty corporate actions are rejected, not silently ignored.
