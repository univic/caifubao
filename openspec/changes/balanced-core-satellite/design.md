# Design: balanced core-satellite and replay visibility

## Evidence and scope

`riq_v1` is frozen: five factors, walk-forward ICIR/sign weights with a 21-session
lag, quarterly rebalance, equal-weight top-10 with a 2N buffer. Its rolling
out-of-sample CAGR is 11–12% with a −48% drawdown; the in-sample figure is 25.7%.
The balanced structure therefore allocates **50% core / 40% satellite / 10% cash**.
Nothing in this change re-optimises those parameters.

## C1 — degraded quote runs

A quote run has three distinct outcomes:

| Outcome | Meaning | Downstream |
|---|---|---|
| `SUCCESS` | every attempted symbol validated | usable |
| `DEGRADED` | coverage ≥ 99% and failures ≤ max(5, 0.2% of attempts) | usable only if the factor phase wrote rows |
| `FAILED` | tolerance exceeded, a core index failed, the circuit breaker tripped, or no rows validated | blocked |

Tolerance is bounded in absolute and relative terms so a source outage (hundreds
of failures) still fails. `DEGRADED` is a first-class persisted status, not a
`SUCCESS` with a warning, so dashboards and dependency gates cannot silently
treat it as clean.

Non-core index history that the source does not provide is counted in
`index_validation.skipped_indices`; a persistent structural absence is
distinguished from a transient gap by a declared allowlist. Core indices
(`sh000001`, `sz399001`, `sz399006`, `sh000300`, `sh000905`) must validate; their
failure is always `FAILED`. A run with only allowlisted non-core skips persists
`DEGRADED`.

The dependency gate accepts `SUCCESS`, or `DEGRADED` with
`coverage_rate >= 0.99` and `update_ma_factor.written_count > 0`. The decision
and the observed numbers are recorded in the downstream job summary.

## C2 — replay artifact

`StrategyReplay` is a research observation, not a forward ledger:

- `replay_id` is a versioned slug `<strategy>_<window>_<aum>_vN`. Rebuilding
  with the same id and the same input hash is byte-identical and may replace the
  document; any input-hash mismatch under an existing id fails closed (use a new
  versioned id).
- `evidence_kind` is always `REPLAY`.
- `curve` is one point per session: `date`, `nav`, `benchmark_nav`, `drawdown`,
  `cash`, `positions_count`, `gross_exposure`.
- `decisions` is one record per rebalance: decision date, execution date,
  target weights, added/removed, cash before/after, estimated cost.
- `fills` records executed lots, price, commission, stamp duty, transfer fee and
  blocked/failed reason.
- `metrics` includes total/annualised return, Sharpe, max drawdown, annual
  turnover, fees, cash drag and benchmark return.
- `forward_sessions` is derived as `0` for every replay; the API returns
  `forward_required=120` and `forward_eligible=false`. The existing
  `StrategyForwardWindow` remains the only certification path, and the
  forward-progress code MUST NOT read `strategy_replays`.
- `data_as_of` is the maximum `curve.date`; `generated_at` is the build instant;
  `schema_version`, `source_artifact`, `input_hash` and `config_hash` travel with
  the artifact.

The artifact is produced by an operator-run datahub research runner
(`datahub/app/jobs/strategy_replay_runner.py`) writing the `strategy_replays`
collection; the backend only reads it. It is a separate surface from the frozen
`factor_lab_paper_run.py` ledger and from `StrategyPaperRun`; it never writes
either, and neither is used as its source of truth. A replay is never exported as
a target list and never advances a forward window.

## C3 — 50k plan

Inputs: account snapshot (`cash`, `positions`), target weights, signal-date
prices, AUM and the execution config. The plan is sell-first/buy-second:

1. Sell names not in the target; if the open is unknown/limit-locked/suspended,
   report blocked and keep the position.
2. Reserve cash for sells' commission/stamp/transfer fee.
3. Buy target names with fee-aware board-lot sizing, spending at most the target
   notional and never more than available cash; a name that cannot fit one
   board minimum is skipped and reported.
4. Recompute expected cash; a negative cash result is a hard failure.

The default config is the 50k balanced account: 10% cash floor, main-board 100
lots, STAR 200 + 1 (BSE codes are rejected, not sized), commission 0.025% (min ¥5), stamp 0.05% on
sells, transfer 0.001% both sides, slippage 0.1%/side. AUM is cash + marked
holdings at signal-date prices; an unvaluable holding fails closed. The plan
reuses the shared `tradeable_open` verdict and records the normalized
`strategy_config_hash`.

## C4 — survivorship audit/backfill

The audit reads the frozen panel and reports: distinct codes per year, codes
that disappear, codes that first appear, ST share and BSE share. A missing
delisted cohort is reported as `survivorship_gap=true` with the count of listed
but absent codes. The backfill CLI is research-only: it fetches tushare
`stock_basic(list_status='D')` and `namechange` for a requested window and writes
a research parquet (not `basic_stock`, not `stock_daily_quote`). The panel export
can consume that parquet as a supplement; without it the artifact carries the
bias flag. A full rerun of the OOS arm is an explicit operator step, not a silent
part of this change.

## C5 — core-satellite

The documented static-core evidence is monthly (20 sessions); this slice uses the
quarterly cadence required by the satellite, so the core metrics are a disclosed
re-simulation rather than the documented monthly figures.

At ¥50k the core is the lot-feasible **40/30/30** variant
(`510300.SH` ¥10,000 / `518880.SH` ¥7,500 / `513100.SH` ¥7,500). The
25/25/25/25 four-leg variant needs a ¥10k+ treasury lot and is not buildable in
a ¥6,250 leg; it is deferred to AUM ≥ ¥100k and the builder refuses it at ¥50k
instead of forcing a partial leg. Satellite: a **new ¥20,000 build** of the frozen `riq_v1`
configuration (top-10, `fwd_h20` weight label, 21-session lag, quarterly, 2N
buffer, sign weighting). The frozen configuration is not re-tuned; the ¥20k build
is separate from the documented ¥50k ledger and carries its own lot/commission
scale limitation. Cash: constant 10% with zero yield, which is also the
defensive leg.

The combiner requires identical core and satellite session sets (a mismatch
fails closed), compounds each sleeve from its
own initial AUM (50/40/10), and rebalances across sleeves at the same quarterly
cadence with a ±5pp drift band. Only the notional needed to restore the target
moves; breaches are restored in a deterministic order (core first, then
satellite, then cash) and the moved notional is charged once at
`max(5, 0.0005 * moved_notional)` with no stamp duty. Uninvested sleeve cash
stays in the sleeve. A missing core price fails closed instead of silently
dropping the sleeve. QDII premium and purchase limits are disclosed as an
unmodelled limitation; the core price source is tushare fund data, not the
production quote store.

## API and UI

`GET /api/strategy/replays` returns paginated summaries (default 20, max 200);
`GET /api/strategy/replays/<id>` returns the full artifact. Both require a user
JWT and reject OpenClaw service tokens; there is no mutation endpoint. The UI has
a list page and a detail page with four regions: header/config, metrics, ECharts
NAV+drawdown+benchmark, and a decision timeline with expandable target/fill
tables. The forward badge shows `REPLAY · 0/120` and explains why replay does not
count; the research-grade disclaimer and the survivorship/QDII limitations sit
next to the metrics.

## Validation

- Datahub: focused tests plus the full `datahub/.venv` suite, CI-pinned Ruff.
- Backend: focused API tests with mongomock or a fake model, Ruff.
- Frontend: `npm run lint` and `npm run build`.
- OpenSpec: `openspec validate --all --strict` (or the repository's validator).
- Review gates: `spec-guardian`, `contract-reviewer`, `qa-reviewer`.

## Risks

- A degraded tolerance can hide a slowly degrading source; the absolute cap and
  the explicit status keep it visible, and the health watcher must alarm on it.
- The replay artifact is a historical measurement, not a promise; the UI must
  show the REPLAY label and the survivorship/QDII caveats next to the metrics.
- Core ETF prices come from tushare `fund_daily`, which is not the production
  quote store; the artifact records the source and as-of date.
