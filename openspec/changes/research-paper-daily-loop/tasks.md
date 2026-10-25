# Research Paper Milestone A — staged acceptance

## 1. Scheduled research writer (this public PR)
- [x] Add StrategySpec-only guarded daily entry point, Beijing market-session logic, current data/halt readiness and ACTIVE matching window check.
- [x] Reuse the research strategy runner without \`--replace\` or window certification.
- [x] Emit daily job-run statuses and fail nonzero on missing/invalid FORWARD output.
- [x] Add a default-suspended 22:15 research CronJob using an external pinned StrategySpec ConfigMap.
- [x] Add pure causal/freshness/window unit tests.
- [ ] Public CI OpenSpec, datahub tests, Ruff, Kustomize; review and correct any failures.

## 2. Private deployment and authorization (separate PR and operator gate)
- [ ] Reconcile the actual persistent halt claim/path into datahub Deployment and both scheduled readiness/strategy CronJobs.
- [ ] Add explicit private \`DATAHUB_STRATEGY_FORWARD_SUSPEND\` gate, immutable image pin and manifest/rehearsal checks; align public reference after this PR merges.
- [ ] Establish alert routing for failed/missing daily runs and trade-date gaps.
- [ ] Pin and review exact account scale, model version, horizon and StrategySpec hash.
- [ ] Validate one manual day: score/quote/ETF/HALT freshness, dry-run, actual mode, job state and resulting evidence type.
- [ ] Receive separately recorded operator authorization for opening the FORWARD window and scheduled Research writes; only then enable scheduler.

## 3. Complete trustworthy daily NAV (next public code slice)
- [ ] Derive/apply dividend, split, ETF distribution and delisting events in research NAV projection with correct ex-date/availability semantics.
- [ ] Verify NAV cash/positions parity, reproducible rebuild and attribution to ledger events.
- [ ] Run guarded materialization when the next-session prices are fully available, with explicit provenance and historical gap alerts.
- [ ] Confirm portfolio curve has no fabricated returns/zero marks on missing quotes.

## 4. Operational acceptance
- [ ] Observe multiple consecutive actual trading sessions, including restart/retry and skipped market holiday.
- [ ] Assert that all new FORWARD plan dates correspond to time-valid, immutable, matching window evidence; verify missed days are never backfilled.
- [ ] Report daily status, skipped/missing sessions, NAV continuity and breaker state without silent success.
- [ ] Keep Trading Paper and LIVE disabled; don't call the milestone complete until private rollout and NAV accounting have passed acceptance.
