# Stock Timing Replay Adapter P1 Tasks

## 1. Spec Gate

- [x] 1.1 Review frozen-cohort, model-pin, percentile causality, execution, and
      non-persistence semantics before implementation.
- [x] 1.2 Resolve all P1/P2 findings.

## 2. Failing tests

- [x] 2.1 Frozen cohort rejects current-active/late/unhashed evidence,
      reconciles provenance counts, retains names delisted after `as_of`, and
      produces deterministic source-bound identity.
- [x] 2.2 Model pin requires exact ACTIVE ranked version/config hash.
- [x] 2.3 Daily score provenance matches the frozen prediction cohort and
      rejects legacy/future-contaminated rows.
- [x] 2.4 Paired replay uses prior-close percentile, shared next-open execution,
      board lots/friction, filled trades, and last-close valuation.
- [x] 2.5 Suspended/limit/missing evidence fails closed, window bounds hold,
      and output is deterministic.
- [x] 2.6 CLI loads real evidence, invokes P0, never persists, and exposes only
      the research JSON output path.

## 3. Implementation

- [x] 3.1 Add pure cohort/model validation and paired replay adapter.
- [x] 3.2 Add Mongo loaders and `timing-replay` CLI orchestration.
- [x] 3.3 Document a versioned manifest example and research-only boundary.

## 4. Validation and gates

- [x] 4.1 Focused datahub tests, then full affected suites.
- [x] 4.2 Ruff check/format and `openspec validate --all --strict`.
- [x] 4.3 Spec-guardian and qa-reviewer after validation; contract-reviewer only
      if an external contract is introduced.
- [ ] 4.4 Branch conflict check, Draft PR, CI green, then ready for review.
