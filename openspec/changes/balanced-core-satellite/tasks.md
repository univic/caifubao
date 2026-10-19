# Balanced core-satellite tasks

> Scope: C1 datahub degradation, C2 replay artifact + API, C3 50k plan, C4
> survivorship audit/backfill, C5 core-satellite, frontend replay view. Research
> only: no broker, no real orders, no forward certification of a replay.

## 1. Spec and gate

- [x] 1.1 Proposal/design/spec deltas cover C1-C5 and the replay UI.
- [x] 1.2 `spec-guardian` read-only review; resolve blockers before code.
- [x] 1.3 `openspec validate --all --strict` (or the repository validator) passes.

## 2. C1 — degraded pipeline semantics

- [x] 2.1 Add `DEGRADED` as a first-class job status (model/helper/constants).
- [x] 2.2 Stock quote tolerance: absolute + relative caps, coverage, strict switch;
      persist failed codes and counts; keep the circuit breaker and zero-row failure.
- [x] 2.3 Core-index vs non-core-index classification; a non-core missing history
      is an index-level skip with run `DEGRADED`, core failure is `FAILED`.
- [x] 2.4 Signal/scoring dependency gates accept `SUCCESS` or a usable `DEGRADED`
      run and record the observed coverage/factor evidence.
- [x] 2.5 Tests: degrade, outage fails, strict mode fails, core index fails,
      degraded accepted/rejected downstream, non-core skip degrades.
- [x] 2.6 Expose `DEGRADED` in the health watcher (alarm/report, not clean) and in
      `/api/datahub/status` (`quote_run.status`, failed codes, coverage).
- [x] 2.7 Docs: operator semantics and thresholds.

## 3. C3 — 50k execution plan

- [x] 3.1 Pure `account_plan` builder: sell-first, board lots, fee-aware sizing,
      cash floor, skips, projected positions, input/config hash.
- [x] 3.2 CLI with JSON/CSV output; read-only; no Mongo.
- [x] 3.3 Tests: STAR 200, unaffordable skip, sells fund buys, reproducibility,
      projected target quantities, negative cash fails, BSE rejected.

## 4. C2 — replay artifact and API

- [x] 4.1 `StrategyReplay` model (datahub) and a read model in backend.
- [x] 4.2 Pure builder that converts an account replay (curve/decisions/fills)
      into the artifact; metrics and limitations included.
- [x] 4.3 Persist by stable `replay_id`; `evidence_kind=REPLAY`,
      `forward_sessions=0`, `forward_eligible=false`.
- [x] 4.4 Read-only list/detail API with pagination and 404.
- [x] 4.5 Tests: artifact completeness, missing price fails, forward never advances,
      API list/detail/404.

## 5. C5 — core-satellite

- [x] 5.1 Pure core sleeve: 40/30/30 at ¥50k (three ETFs), quarterly rebalance,
      board lots, fees, skips, no stamp duty; refuse 25/25/25/25 at ¥50k.
- [x] 5.2 Pure cross-sleeve combiner: 50/40/10, ±5pp band, deterministic
      restore order, costed notional move, missing price fails closed.
- [x] 5.3 ETF fetch CLI (tushare fund_daily/fund_adj) + combined replay exporter.
- [x] 5.4 Build the ¥20,000 satellite sleeve with the frozen config
      (`--aum 20000 --step 60 --names 10 --weight-label fwd_h20`, 21-session
      lag); persist config hash, AUM, label and the scale limitation.
- [x] 5.5 Add the operator-run `datahub/app/jobs/strategy_replay_runner.py`
      (never scheduled) that writes `strategy_replays`.
- [x] 5.6 Tests: initial sleeve NAVs, lot skip, drift-only rebalance, missing price.
- [x] 5.7 Seed one real replay on the research cluster and record its limitations.

## 6. C4 — survivorship audit/backfill

- [x] 6.1 Read-only audit over the panel: yearly codes/appear/disappear, ST/BSE,
      missing listed codes, `survivorship_gap`.
- [x] 6.2 Research backfill CLI for delisted `stock_basic` + `namechange` to a
      research parquet; no production writes.
- [x] 6.3 Wire the flag into the replay artifact; tests with synthetic frames.
- [ ] 6.4 Run a limited acceptance and record residual gaps; full OOS rerun is an
      explicit operator step.

## 7. Frontend

- [x] 7.1 API client and Pinia/store-free view data loading.
- [x] 7.2 List page with strategy, period, AUM, return/drawdown and REPLAY badge.
- [x] 7.3 Detail page: config/metrics, ECharts NAV + drawdown + benchmark,
      decision timeline, expandable fills, limitations.
- [x] 7.4 Router and navigation entry; empty/error/loading states.
- [x] 7.5 `npm run lint` and `npm run build`.

## 8. Validation, review and close

- [x] 8.1 Focused + full datahub pytest, Ruff check/format.
- [x] 8.2 Backend focused tests + Ruff.
- [x] 8.3 Frontend lint/build.
- [x] 8.4 `contract-reviewer` (API/replay/freshness) and `qa-reviewer` (safety,
      regression, hygiene); resolve P1 and acknowledge P2.
- [x] 8.5 Update `docs/operations/strategy-experiments-2026-08.md` and
      `docs/agent-progress.md`; close with the gate checklist.
