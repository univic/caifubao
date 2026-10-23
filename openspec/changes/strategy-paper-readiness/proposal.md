## Why

The research environment can already record paper runs and a certified forward
window, but there is no single answer to "is the chain ready to treat today's
run as forward evidence?". The operator has to inspect several collections
manually, and the known gaps (paper NAV marks raw prices and never credits cash
distributions; a missing halt-file path fails order generation; ETF data did not
exist) are documented only in prose. Opening a 120-session clock on an
incomplete day wastes an unrepeatable window, so readiness must be checkable
before the first certification.

This change adds a read-only assessment and the operator runbook, and ties the
new ETF market-data capability into the same gate. It deliberately does **not**
change the strategy engine: PR #307
(`feature/strategy-engine-unification`) is refactoring `strategy_engine/nav.py`,
`replay.py`, `runner.py`, `tradability.py`, the strategy models and the shared
execution contracts. The readiness check reads only the persisted
`strategy_forward_windows` / `strategy_paper_runs` / quote / scoring / signal /
`data_asset_status` state by its field names, so it layers on top of either
engine shape and adds no file that PR #307 owns. That coupling is explicit: a
refactor that renames a persisted window/run field must update the check with
it, and a renamed field is a schema mismatch rather than a silent pass.

## What Changes

- Add a read-only `paper_readiness` assessment (`load_inputs` + pure
  `evaluate`) covering quote freshness and coverage, FQ coverage, scoring and
  signal freshness, the ACTIVE forward window and its session gaps, the halt
  switch, and ETF market-data freshness/FQ.
- Report `READY` / `DEGRADED` / `NOT_READY` with per-check detail, an explicit
  expected session derived from the A-share calendar and the 15:00 Beijing
  close, and a `limitations` list (raw-price paper NAV and distributions, no
  broker, permanent window gaps, ETF distributions).
- Add `python -m app.jobs.paper_readiness` and
  `scripts/caifubao paper readiness`, exiting non-zero only on `NOT_READY`.
- Add a suspended-by-default research CronJob that runs the assessment after
  the scoring window.
- Add the operator runbook for opening/continuing a paper window in the
  research environment, including the ETF data prerequisite and the
  authorization boundary.

## Impact

- `datahub/app/lib/paper_readiness.py` (new), `datahub/app/jobs/paper_readiness.py`
  (new), `scripts/caifubao`, `k8s/overlays/example-research/datahub-cronjobs.yaml`
- Tests and operator documentation.

No API contract, scoring, forward-evidence rule, strategy execution or data
ownership changes. The assessment writes nothing and never opens or closes a
window.

## Non-goals

- Opening, certifying or closing a forward window; that stays an explicit
  operator action with `--yes`.
- Placing orders, broker adapters or live-account reconciliation.
- Fixing the raw-price paper-NAV distribution gap; that belongs to the
  execution-model work and must be coordinated with PR #307.
- Changing the strategy engine or its ledger (PR #307 owns that refactor).
- Automatic remediation or self-healing of a failed check.
