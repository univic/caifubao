# Strategy replay (REPLAY) — build, publish and view

This runbook covers the **research-only** replay artifact introduced by
`openspec/changes/balanced-core-satellite/`. A replay is a historical
measurement (`evidence_kind=REPLAY`); it is **not** forward evidence, does not
advance the 120-session gate, and is not investment advice.

## 1. What the artifact contains

One `strategy_replays` document per versioned `replay_id`:

- `curve`: one point per session (`nav`, `benchmark_nav`, `drawdown`, `cash`,
  `positions_count`, and for the combined strategy `core_nav`,
  `satellite_nav`, `cash_nav` next to the absolute `cash`);
- `decisions`: decision date, execution date, target holdings, added/removed,
  cash before/after and estimated cost;
- `fills`: executed orders with quantity, price, commission, stamp duty and
  transfer fee;
- `blocked_orders`: limit-up/limit-down/suspended/unknown refusals;
- `metrics`: total/annualised return, Sharpe, maximum drawdown, annual turnover,
  total fees, average cash share and benchmark return;
- `evidence_kind=REPLAY`, `forward_sessions=0`, `forward_required=120`,
  `forward_eligible=false`, `research_grade=RESEARCH`, the disclaimer,
  `survivorship_gap` and `limitations`.

The backend only reads the collection; the API is read-only and requires a user
JWT. The operator-run datahub runner writes it.

## 2. Build the balanced 50k replay

The balanced profile is **50% core / 40% satellite / 10% cash**. At ¥50k the core
is the lot-feasible **three-ETF 40/30/30** variant (CSI 300 / gold / Nasdaq):
the four-leg 25/25/25/25 variant needs a ¥10k+ treasury lot and is deferred to an
AUM of at least ¥100k.

### 2.1 Satellite sleeve (frozen `riq_v1`, new ¥20k build)

```bash
datahub/.venv/bin/python datahub/scripts/factor_lab_account_replay.py \
  --panel /data/lab_live.parquet \
  --start-date 2022-01-24 --end-date 2026-03-16 \
  --step 60 --names 10 --aum 20000 --label fwd_h20 \
  --lookback 244 --min-ic-dates 120 \
  --detail \
  --output /data/satellite_replay.json
```

The `--detail` arm adds the daily curve, decisions, fills and blocked orders to
`decision_time_daily`. Keep `--names 10`, `--label fwd_h20` and the 21-session
lag (derived from the label) — that is the frozen configuration; do not tune it
in the replay run.

### 2.2 Optional survivorship audit

```bash
datahub/.venv/bin/python datahub/scripts/research_universe_audit.py \
  --panel /data/lab_live.parquet \
  > /data/universe_audit.json
```

The audit writes JSON to stdout; redirect it to a file for the combiner.
Without a delisted/ST supplement the artifact carries
`survivorship_gap=true` and the upper-bound limitation, which is the correct
state for the current panel.

### 2.3 Core sleeve + combined artifact

```bash
datahub/.venv/bin/python datahub/scripts/core_satellite_backtest.py \
  --satellite-json /data/satellite_replay.json \
  --audit-json /data/universe_audit.json \
  --output /data/balanced_50k.json \
  --replay-id balanced_50k_20220124_20260316_50000_v1
```

The core prices are fetched from tushare `fund_daily`/`fund_adj` when
`--core-prices` is not supplied. The combiner requires identical core and satellite session sets; a
missing/extra session or a missing core price fails closed.

### 2.4 Publish the artifact

```bash
cd datahub && .venv/bin/python -m app.jobs.strategy_replay_runner --payload /data/balanced_50k.json
```

The runner is operator-run and is **not** scheduled. It is idempotent for the
same `replay_id` + `input_hash`; a changed input under an existing id fails
closed — create a new versioned id.

## 3. View

- `GET /api/strategy/replays?page=1&page_size=20`
- `GET /api/strategy/replays/<replay_id>`

The UI entry is **策略重放** (`/strategy-replays`). The detail page shows the
config, metrics, NAV/benchmark/drawdown chart, decision timeline (targets,
fills, blocked orders), the `REPLAY · 0/120` badge and the limitations.

## 4. C1 — degraded quote runs

A stock quote run persists `DEGRADED` (never `SUCCESS`) when at least one row
validated, `failed_count <= max(5, ceil(0.002 * attempted_symbols))`, coverage is
at least 99%, and the history circuit breaker did not trip. The signal gate
accepts a `DEGRADED` run only with that coverage and a positive MA-factor write
count. The strict switch restores the old behavior:

```bash
DATAHUB_QUOTE_STRICT_VALIDATION=1
```

Non-core index history that is structurally absent is counted in
`index_validation.skipped_indices`; a persistent absence must be declared in
`DATAHUB_INDEX_SKIP_ALLOWLIST` so it cannot hold the run at `DEGRADED` forever.
Core indices (`sh000001`, `sz399001`, `sz399006`, `sh000300`, `sh000905`) must
always refresh.

## 5. C3 — 50k execution plan

```bash
datahub/.venv/bin/python datahub/scripts/strategy_account_plan.py --input plan_input.json --output plan.json
```

The input is a JSON object with `cash`, `positions`, `target_weights`, `prices`,
`config`, `as_of` and an optional `aum` cross-check. The plan is sell-first,
board-legal (STAR 200 + 1; other boards 100), fee-aware (commission ¥5 minimum,
sell stamp 0.05%, transfer 0.001%/side, slippage 0.1%/side), respects the cash
floor and reports blocked/skipped rows. It never writes Mongo, contacts a broker
or places an order.

## 6. C4 — survivorship/ST audit and research backfill

```bash
# Read-only audit (per-year codes, entrants, leavers, ST/BSE shares, gap flag)
datahub/.venv/bin/python datahub/scripts/research_universe_audit.py --panel /data/lab_live.parquet

# Research-only supplement: delisted stock_basic + namechange intervals
datahub/.venv/bin/python datahub/scripts/research_universe_backfill.py \
  --from-date 2015-01-01 --to-date 2026-09-18 \
  --output /data/research/universe_delisted.parquet
```

The backfill writes only the research parquet (plus optional quote parquet with
`--with-quotes`); it never mutates `basic_stock`, `stock_daily_quote` or Mongo.
Wiring the supplement into the panel export and re-running the OOS arm is an
explicit operator step; do not claim an unbiased return until that rerun exists.

## 7. Known limitations

- **REPLAY**, not forward evidence. The score-driven `StrategyForwardWindow`
  remains the only certification path; a replay never advances it.
- The panel is survivorship-biased (no delisted cohort; ST is a current
  snapshot), so historical returns are an upper bound.
- The core uses tushare fund prices, not the production quote store, and
  executes at the session close in the research approximation.
- QDII premium/discount and purchase limits are not modelled.
- The documented static-core evidence is monthly; this slice uses quarterly
  cadence for the combined account.
- Cross-sleeve rebalancing is a costed notional transfer, not an ETF lot trade.
- The 50k satellite is a new ¥20k build; it is not the documented ¥50k ledger.

## 8. 2026-10-19/20 部署验证记录

- research backend/frontend/datahub 镜像：`sha-3756ab7`（main `3756ab7`）。
- datahub Deployment 与 quote-stock / quote-index / signal / scoring 四个
  CronJob 均已切到该镜像；**CronJob 不会随 Deployment 自动更新**，每次发布后
  必须核对 `spec.jobTemplate.spec.template.spec.containers[0].image`。
- 2026-10-19 补跑结果：quote-stock `SUCCESS`（local-only
  `sh601059`/`sh601198` 进入 `skipped_symbols`）、signal `SUCCESS`、
  scoring `SUCCESS`；quote-index `DEGRADED`（2 个非核心指数结构性缺失）。
- research 后端 `/api/strategy/replays` 返回 seeded replay；前端
  `/strategy-replays` 路由可访问。
- 发布时 `datahub-secret` 的 `TUSHARE_TOKEN` 曾被覆盖；部署前应核对 secret
  至少包含 `MONGODB_PASSWORD` 与 `TUSHARE_TOKEN`。
- 建议在 research 部署配置中固化 `DATAHUB_INDEX_SKIP_ALLOWLIST`，把已知结构性
  缺失的非核心指数列入，避免 health watcher 每天报 DEGRADED。
