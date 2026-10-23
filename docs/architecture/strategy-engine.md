# Strategy Engine — research implementation and target boundaries

> Status: **incremental implementation**, not an operational trading system.
> Source of normative requirements:
> [strategy-engine-unification](../../openspec/changes/strategy-engine-unification/proposal.md),
> [environment-model](./environment-model.md), and
> [production-capability-roadmap](../../openspec/changes/production-capability-roadmap/proposal.md).
> Do not treat this architecture overview as a promotion approval.

## 1. Domain ownership

| Domain | Authority | Present status |
|:---|:---|:---|
| data | PIT market facts, security master, exchange calendar, versioned snapshots | datahub publishes these inputs |
| research | StrategySpec compatibility, target construction, research simulation, historical replay, derived NAV and immutable evidence | existing implementation is partly under `datahub/app/` (migration debt) |
| trading | operational risk gate, simulated/real orders, fills, holdings, cash, reconciliation and halt | `trading-paper` and `prod` are **not enabled** |
| shared | versioned schema, canonical hashes, pure execution/fee functions and conformance vectors | `shared/caifubao_contracts/` |

Physical Python modules in datahub do **not** transfer trading account ownership
to the data domain. In particular, `datahub/app/lib/execution/costs.py` is a
compatibility re-export for `shared/caifubao_contracts/friction.py`, not
a place to implement orders, brokers, or accounts. The Flask backend must
remain an API/legacy backtest boundary rather than a replacement trading engine.

## 2. Implementation map (PR #307, draft)

```text
data snapshots, score predictions (PIT)
        |
        v
research StrategySpec adapter -> target book / rebalance intent
        |
        +--> shared deterministic execution primitives
        |     (lot rule, fees, slippage model, tradability policy)
        v
research simulator -> ResearchSimulationLedger (fills, rejects, marks)
        |                       |
        +-----------------------+--> research NAV / evidence artifact
                                    (content-addressed, independent of
                                     forward-certified plan fields)

future immutable strategy promotion + evidence
        |
        v
trading runtime (not enabled):
  independent RiskGate -> ExecutionAdapter -> TradingPortfolioLedger
  -> account reconciliation / operator halt / audit
```

Research's `StrategyPaperRun` is a **target/forward-plan record**, not a
trading-paper broker/account record. `StrategySimulationArtifact` is a
distinct research projection and MUST NOT update certified FORWARD plans
(except already-permitted derived NAV snapshots).

## 3. Identity and evidence

A migrated configuration retains `legacy_config_hash` and independently
computes `spec_hash` using the shared, canonical, versioned StrategySpec.
The two values belong to different namespaces even if their bytes happen to
match. A forward window pins both hash value and hash kind; mismatches remain
REPLAY. Backfilled plans, NAV recomputations, and job SUCCESS never advance the
120-session evidence count. A new spec needs a new explicit certification
window; migrating historical records never upgrades evidence.

Artifact payload hashes are deterministic and content-addressed. A stored
research artifact with the same hash but different payload is rejected instead
of silently overwritten. Derived research artifacts must not create, update,
or impersonate trading account state.

## 4. Execution / ledger invariants

- Decisions from session D close are not filled before the next eligible open.
- Target weight does not establish a holding; only ordered, attributable ledger
  events do. Duplicate event identity with an identical payload is a no-op;
  conflicting payloads fail closed.
- One order may have multiple partial fills with distinct fill IDs. Research
  event streams are replayable to equal account projections.
- Cash includes the actual simulated fill-price notional and fee breakdown;
  failures must not partially mutate position or cash state.
- Suspended names and missing marks carry a last-known mark with a stale flag,
  rather than being valued at zero. Corporate actions and delistings require
  explicit adjustment/write-off events.
- Risk approval is not execution. A sell approved by RiskGate never releases
  cash or a position slot before the ledger receives a fill. Multiple submitted
  sells must not exceed the held share quantity.
- A missing or unclassifiable price-limit verdict fails closed by default;
  replay compatibility is an explicitly hashed research-only policy.

## 5. Rollout and verification gates

**S0**: deterministic legacy-to-spec migration, frozen hash vectors, CLI
integration, identical target book/intents for identical snapshots and policy.

**S1**: event-sourced research ledger; NAV parity versus the legacy simulation;
rebuild parity, blocked orders/rejected events, immutable research artifact,
causal timing and accounting tests. The event ledger remains research-only.

**S2**: deterministic risk evaluation, documented priority and rejection
reasons, halt semantics, drawdown and cash limits; tests must show approved
sells do not release cash/position capacity and that replay reproduces the
decision.

**S3**: shared deterministic execution rule conformance, factor-lab and timing
replay consumers, legacy backend freeze, contract-preserving domain adapters
and risk-controlled paper deployment in a separate future change.

The target is NOT complete merely because local unit tests pass. Before
marking this change implemented: all corresponding MODIFIED OpenSpec deltas,
CI checks, cross-mode golden tests, artifact/replay verification, operational
runbook, and independent Spec/QA review must pass. A trading runtime or live
broker adapter is not enabled or authorized by this PR.

## 6. Offline S0 and S2 verification commands

Both commands operate on operator-provided frozen JSON snapshots and neither
connects to research Mongo, trading accounts, or a broker:

```bash
# S0: validate a versioned Spec against decision-time target reference prices
python -m app.jobs.strategy_spec_preflight \
  --spec strategy.json --reference-prices target-prices.json \
  --max-minimum-commission-pct 0.01

# S2: replay risk gate decisions over an explicit ledger + market snapshot
python -m app.jobs.strategy_risk_replay \
  --spec strategy.json --account research-account.json \
  --intents order-intents.json --market market-regime.json
```

Preflight uses the actual reference price and the correct A-share board
minimum. A wide strategy that cannot afford a minimum order on the supplied
target list fails before order intent generation; absent prices are **not**
invented. The default minimum-commission tolerance is 1% of the minimum
order notional and is an explicit parameter, not a hidden market fact.

Risk reason codes include `INSUFFICIENT_HOLDINGS`, `MAX_POSITIONS`,
`CASH_FLOOR`, `TURNOVER_BUDGET`, `DRAWDOWN_BREAKER`,
`REGIME_BLOCKED`, `CONCENTRATION` and `GROSS_EXPOSURE`.
Sells are evaluated before buys, then requests are ordered by descending
`priority` with the original index as the tie-break. Approved sells reserve
shares but do **not** release cash or position slots. Configured exposure
caps require held positions' explicit `market_value`; a non-default regime
policy requires the market snapshot to contain `bull`, `bear` or
`sideways`. Unknown inputs fail closed.

This is still a **research** decision and returns `REPLAY` evidence, never a
trading order approval. The existing legacy research runner rejects non-default
risk policies instead of silently skipping the risk gate. Trading paper/live
enforcement and operator halt persistence remain separate, unimplemented
domain-local responsibilities.

## 7. Research event ledger and recovery

`shared/caifubao_contracts/strategy_ledger_events.py` is the versioned,
side-effect-free event validator. Domain-scoped events state `source_domain`,
`account_id`, `currency=CNY`, `event_id`, `order_id`, `stock_code`,
`side`, and an explicit `execution_date`; filled events also carry
`fill_id`, `quantity`, `price` and fee components. Research events may
carry the pinned `spec_hash` and `decision_date`. They are never an
operational trading account.

Research event kinds are `FILL`, `REJECTED`, `MARK`, `DIVIDEND`,
`SPLIT` and `WRITE_OFF`. Each non-fill corporate adjustment must have an
attributable source/reason; missing market quotes do not fabricate a
`WRITE_OFF`. The event stream is replayable:

```python
from app.lib.strategy_engine.simulation_ledger import ResearchSimulationLedger

restored = ResearchSimulationLedger.rebuild(
    artifact.payload["events"],
    initial_cash=artifact.payload["ledger_snapshot"]["initial_cash"],
)
assert restored.snapshot() == artifact.payload["ledger_snapshot"]
```

Replaying the same immutable event id with the same content is a no-op;
replaying a changed event under the same id fails. The ledger validates
invalid marks, fill reference prices, exit values and account limits before
mutation. No trading database or broker is involved.

When Spec-backed `strategy nav` runs, the derived simulation artifact is
persisted by default. Historical config-backed runs preserve the explicit
`--persist-research-artifact` opt-in. The `strategy report` command
includes a bounded `research_simulation` summary when an artifact is
available, with per-name blocked reasons, filled counts and artifact hash.
A missing artifact is represented as `null` rather than invented evidence.

### Risk default policy and refusal codes

The default risk policy has no turnover or position cap, zero cash floor,
no drawdown threshold, no concentration/gross exposure cap, and
`regime_policy=none`; any new threshold changes the canonical Spec hash.
The supported non-default regime modes are `bull_only` and
`bear_reduce_only`. Valid frozen regime values are `bull`, `bear`,
and `sideways` (60-session CSI 300 classifier); missing classification
**fails closed** when a regime policy is enabled.

| Rejection code | Trigger |
|:---|:---|
| `INSUFFICIENT_HOLDINGS` | Requested sell quantity exceeds shares not already reserved for a sell |
| `MAX_POSITIONS` | New BUY would exceed current occupied slots |
| `CASH_FLOOR` | BUY would use cash reserved by the declared floor |
| `TURNOVER_BUDGET` | Requested turnover exceeds the session budget |
| `DRAWDOWN_BREAKER` | Portfolio drawdown threshold reached; risk-increasing BUY refused |
| `REGIME_BLOCKED` | Frozen market regime fails the specified entry policy |
| `CONCENTRATION` | Name exposure after BUY breaches max single-name share |
| `GROSS_EXPOSURE` | Gross exposure after BUY exceeds cap |
| `HALTED` | Explicitly captured halt takes precedence; no intents approved |

## 8. Implementation close-out notes for PR #307

**Reconciliation source authority:** `reconcile_plan_vs_account` retains
the historical operator CLI and JSON/exit contracts. The new read-only
`reconcile_trading_projection` accepts only `source_domain=trading`,
an explicit `account_id`, filled-position quantities and projected cash.
It overrides any caller-supplied `planned_cash` with the trading projection.
Research position targets, estimated fills or research ledger snapshots are
rejected as substitutes. This adapter does not create a TradingPortfolioLedger
or authorize a broker operation. Normative delta:
`strategy-live-loop-minimal/spec.md` in this change.

**Daily NAV parity:** `simulate_paper_nav` now returns an additional
`nav_parity` audit vector per rebalance session: independent legacy-shaped
cash+positions mark, event-ledger mark and the numeric delta; differences
greater than 0.01 CNY fail loudly. Tests include blocked exits followed by
subsequent liquidation and event-stream rebuild. This checks accounting
equivalence over supplied frozen sessions; it does not assert that a new
multi-year historical dataset has been retrieved or replayed.

**Market and halt authority:** `market_snapshot.frozen_regime_snapshot`
calls the existing 60-day CSI300 classifier at a *decision-date* cut-off;
unknown classifications and post-decision snapshots fail closed.
`strategy_risk_replay.replay_with_market_regime` then passes that
explicit snapshot to the side-effect-free risk gate. Research may read a
separately captured `--halt-snapshot` JSON with operator attribution,
but never reads or changes the live halt file from risk replay. The older
datahub halt command remains a temporary legacy physical implementation,
not the target Trading halt authority.

**Cross-consumer rule migration:** timing replay now consumes shared
slippage, fee, and board-lot functions. Its existing manifest has no
transfer-fee key and therefore retains its historical zero-transfer
semantics unless a versioned policy supplies one. Factor Lab offers the
explicit `shared_v1` cost policy while its default `legacy` calculation
is unchanged. Factor Lab's old lot-counting ledger cannot represent
STAR 200+n (1-share increments), so shared mode fails closed for STAR;
migration to a share-level ledger is a follow-up, not hidden truncation.

**Execution runtime separation:** `strategy_modes` maps
BACKTEST/REPLAY/RESEARCH_SIMULATION to research and
TRADING_PAPER/LIVE to trading. The research simulator refuses the
trading modes. A separate trading-owned adapter, persistent account
ledger and an end-to-end cross-mode golden trace are **not implemented**.
Likewise the repository-wide `artifact-contract` (hash input units,
CI-injected code provenance and field manifests) is not satisfied merely
by an existing research `content_hash`; it requires its own conformity
work before declaring S3 complete.

## 9. Current CI dependency

The public PR dispatches a dry-run workflow in the private overlay repository.
Its failure must be investigated in the **private** Actions run and cannot be
waived as a formatting failure. Repository-dispatch success alone does not
prove that a private workflow completed. Preserve the required check until
the downstream environment is healthy.
