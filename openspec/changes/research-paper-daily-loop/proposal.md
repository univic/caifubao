## Why

Research/stable has real-market daily quote, signal and pinned candidate scoring,
but its strategy FORWARD capture is still operator-only: \`strategy_runner run\`
requires an explicit \`--date\`, and a scheduled attempt without a guard could
create REPLAY, rewrite an incorrect day's evidence, or silently skip signals.
Milestone A requires an auditable daily cadence before a 120-session window can
be authorized.

## What Changes

- Introduce a dedicated, same-Beijing-session, StrategySpec-pinned daily FORWARD
  capture entry point; do not change the existing operator/legacy CLI.
- Fail closed on absent/wrong ACTIVE certified window, stale quote/score/signal,
  halt not ready, too-early invocation, missing next trading session or causal
  cutoff; skip a genuine exchange holiday without a strategy write.
- Track SUCCESS/SKIPPED/FAILED in \`datahub_job_runs\`; never silently promote a
  REPLAY/failed run to FORWARD evidence or replace completed forward records.
- Provide a 22:15 Asia/Shanghai suspended CronJob example, with explicit
  read-only StrategySpec ConfigMap and an unconfigured halt mount that requires
  private-environment reconciliation before enablement.

## Capabilities

### New Capabilities

- \`research-paper-daily-loop\`: guarded scheduled research FORWARD capture and
  explicit provenance/operational checks.

### Modified Capabilities

- No change to Trading/Paper runtime, StrategySpec hash contracts or the
  existing research simulator's execution semantics.

## Impact

- \`datahub/app/jobs/strategy_forward_daily.py\`: dedicated scheduled runner.
- \`datahub/app/test/test_strategy_forward_daily.py\`: causal gate regressions.
- \`k8s/overlays/example-research/datahub-cronjobs.yaml\`: default-suspended job.
- \`docs/operations/paper-live-readiness.md\`: activation checklist and known NAV
  limitations; no live deployment, Mongo writes or forward window certification.

## Non-Goals

- Enabling cron schedules in the existing Research cluster; explicit operator
  authorization is required for scheduled writes and 120-session certification.
- True Trading Paper account/orders, broker access, capital or promotion.
- Claiming NAV accounting completeness: dividend/split/ETF distributions,
  projection rebuild and regular NAV materialization need separate acceptance.
