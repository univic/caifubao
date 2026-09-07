# Production Capability Roadmap

## ADDED Requirements

### Requirement: Public positioning stays research/demo/MVP

All public-facing material (README, docs, UI copy, OpenClaw/API descriptions) MUST
keep the research/demo/MVP positioning and the no-investment-advice disclaimer, even
as internal capability targets live-trading readiness. No public text MAY present
model output as tradable advice or imply real-money operation.

#### Scenario: Public docs keep the demo positioning

- GIVEN the project README and capability documentation
- WHEN they describe system outputs (scores, recommendations, paper results)
- THEN they keep the "research / learning / demonstration MVP, not investment
  advice" framing
- AND no public page claims live-trading or advisory capability

#### Scenario: Research output is labeled as such

- GIVEN a model output produced outside the ≥120-day paper evidence gate
- WHEN it is surfaced to the user (list, report, dashboard)
- THEN it is labeled research/observation-grade
- AND the underlying model is not marked tradable or promoted

### Requirement: Real-money execution is gated and default-off

This roadmap change MUST NOT authorize real-money execution; any future execution
capability MUST be default-off, paper/audit-first, operator-gated, and stoppable
(kill-switch halting order generation), and the strategy layer MUST remain
paper-only until that gate exists.

#### Scenario: Strategy layer stays paper-only absent the gate

- GIVEN no execution gate has been enabled for an environment
- WHEN a daily strategy run occurs
- THEN it only records target holdings / rebalance lists and paper NAV
- AND it never submits orders or mutates a real account

#### Scenario: Enabling execution requires explicit authorization

- GIVEN an environment where execution capability is being enabled
- WHEN the operator attempts to turn it on
- THEN the enabling step requires explicit per-environment authorization
  (documented operator action, not a default)
- AND a paper/audit trail of the decision exists before any order

### Requirement: Promotion to tradable requires ≥120-day paper evidence

A model version MUST NOT be promoted to "tradable/actionable" status until it has
≥120 trading days of forward verified paper evidence (strategy-paper-runner task
4.4), evaluated against its research walk-forward expectation; until then it MAY
only produce research/watch outputs.

#### Scenario: Unvalidated model stays research-grade

- GIVEN a model version with less than 120 trading days of verified paper
  evidence (e.g. flip_wide_shadow_v1)
- WHEN a consumer asks for actionable output
- THEN the output is labeled research/observation-grade
- AND the version is not marked tradable or promoted

#### Scenario: Paper evidence window is counted from operator runs

- GIVEN the daily strategy operator chain (job_family strategy_daily)
- WHEN counting progress toward the 120-day gate
- THEN only COMPLETED (non-SKIPPED/non-FAILED) run dates count
- AND the count and NAV metrics are recorded in the autoresearch ledger
