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

## 6. Current CI dependency

The public PR dispatches a dry-run workflow in the private overlay repository.
Its failure must be investigated in the **private** Actions run and cannot be
waived as a formatting failure. Repository-dispatch success alone does not
prove that a private workflow completed. Preserve the required check until
the downstream environment is healthy.
