# Paper-Trading Readiness Tasks

## 1. Contract

- [x] 1.1 Freeze the read-only, non-evidence, non-authorization semantics and
  the READY/DEGRADED/NOT_READY rules in the delta spec.
- [x] 1.2 State the PR #307 relationship: the check reads persisted state only
  and does not own any strategy-engine file PR #307 refactors (PR #307 merged
  as `7464d6b`; the check was re-synced to its dual-hash model).

## 2. Implementation

- [x] 2.1 Add `app.lib.paper_readiness` with a pure `evaluate` over
  `ReadinessInputs` and a thin read-only `load_inputs`.
- [x] 2.2 Derive the expected completed session from the A-share calendar and
  the 15:00 Beijing close.
- [x] 2.3 Check quote freshness/coverage, FQ coverage, scoring and signal
  freshness, the ACTIVE forward window and gaps, the halt switch, and ETF
  data/FQ.
- [x] 2.4 Emit `READY`/`DEGRADED`/`NOT_READY` plus the limitations list.
- [x] 2.5 Add `app.jobs.paper_readiness` and `scripts/caifubao paper readiness`
  (non-zero only for NOT_READY).
- [x] 2.6 Add suspended-by-default research CronJobs for the readiness check,
  the daily ETF sync and an optional candidate-model scoring job.
- [x] 2.7 Mirror `classify_evidence_kind`'s dual-hash rule: filter FORWARD runs
  by `config_hash` + `hash_kind` (legacy matches the missing field) and report
  `hash_kind` / `spec_hash` / `legacy_config_hash`.
- [x] 2.8 Research enablement (2026-10-25): configure the halt path (engage/resume
  verified, OFF), enable the daily ETF sync and candidate scoring schedules, and
  leave the readiness CronJob suspended until a window is certified.

## 3. Documentation

- [x] 3.1 Add the research paper-window runbook: prerequisites, manual
  validation day, certification, daily run, weekly NAV/progress, and the
  authorization boundary.
- [x] 3.2 Record the raw-price NAV distribution gap as a known limitation with
  its follow-up owner (execution model / PR #307 coordination).

## 4. Validation

- [x] 4.1 Unit tests for the session derivation, every check status, the
  overall status mapping and the limitations presence.
- [x] 4.2 Ruff (CI rule set + format), focused pytest and the OpenSpec strict
  validation pass.
- [x] 4.3 Read-only readiness runs against research (2026-10-23 and 2026-10-24
  after the rollout; DEGRADED with only the no-window and no-halt WARNs).

## 5. Review and delivery

- [x] 5.1 Spec, contract and QA reviewer gates pass (PR #313).
- [x] 5.2 Branch conflict check against the target base (clean vs develop;
  PR #307 merged before delivery).
- [x] 5.3 Draft PR #313, CI green, merged; readiness dual-hash and active-ETF
  count fixes shipped with the same PR / follow-up #314.
