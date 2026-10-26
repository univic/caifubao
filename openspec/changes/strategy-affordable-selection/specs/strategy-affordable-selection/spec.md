## ADDED Requirements

### Requirement: Explicit affordable opening eligibility

StrategySpec SHALL accept optional boolean `constraints.exclude_infeasible_buys`.
When true, research selection SHALL exclude candidates whose signal-session close
cannot pass the existing minimum-board-lot and minimum-commission preflight under
the configured initial NAV, portfolio size, reserve and single-position cap.
Filtering SHALL precede score-order truncation and SHALL NOT recompute percentiles,
increase the per-name budget, use future prices or disable final preflight.

#### Scenario: Unaffordable high-ranked stock
- **WHEN** the top stock's minimum buy exceeds its budget and the rule is true
- **THEN** it is excluded before selection and the next eligible score-ranked
  candidate may take its place, with an auditable exclusion reason

#### Scenario: Missing or invalid decision-time close
- **WHEN** the rule is true and a candidate has no finite positive signal close
- **THEN** that candidate is excluded without substituting future or raw defaults

#### Scenario: Final weights are more restrictive
- **WHEN** an industry cap reduces final target weights below eligibility budgets
- **THEN** final execution feasibility remains enforced and may reject the plan

### Requirement: Backward-compatible immutable strategy identity

Omitted affordability policy SHALL preserve existing behavior and normalized
spec hash. Enabled policy SHALL survive spec/legacy round trips, affect the hash,
and require a newly certified window; existing evidence SHALL NOT be relabeled.

#### Scenario: Legacy spec omission
- **WHEN** a historical spec omits the new constraint
- **THEN** validation and round trips do not insert it or change its hash

#### Scenario: Invalid policy type
- **WHEN** a spec supplies a non-boolean affordability constraint
- **THEN** validation rejects the spec rather than coercing the value
