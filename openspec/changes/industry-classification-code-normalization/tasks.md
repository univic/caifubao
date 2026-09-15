# Industry Classification Code Normalization Tasks

## 1. Spec Gate

- [x] 1.1 spec-guardian reviews the canonical-key contract, migration
      semantics, replay point-in-time guard and the scoring-impact statement
      before code changes.
- [ ] 1.2 Resolve all P1/P2 findings and re-review.

## 2. Failing tests

- [ ] 2.1 Sync stores a canonical key for a separated baostock code, matches an
      existing canonical record, and creates no duplicate.
- [ ] 2.2 Migration dry-run writes nothing; rename preserves `assigned_at`,
      `industry_change_log` and `last_synced_at`.
- [ ] 2.3 Migration merges onto an existing canonical record, keeps the
      canonical anchor, unions the history without inventing an entry, adopts
      the legacy classification only when the canonical record has none, and is
      idempotent on a second run.
- [ ] 2.4 Unrecognized keys are reported and left untouched.
- [ ] 2.5 `industry_momentum` resolves an in-effect canonical classification and
      rejects a later `assigned_at` or a change logged after the date, on both
      the direct and prefetched lookup paths, in the per-day prefetch itself,
      in aggregation, and in the H20 snapshot helper.

## 3. Implementation

- [ ] 3.1 Normalize the baostock code in `sync_industry_classification` before
      lookup and persistence.
- [ ] 3.2 Add the idempotent `normalize_stock_codes` migration helper in the
      industry handler (no `save()`, so `last_synced_at` is untouched).
- [ ] 3.3 Move the point-in-time guard to `app.model.industry` and apply it in
      `scoring_service._load_industry`, `industry_momentum_component` and
      `aggregate_industry_metrics`; keep `strategy_runner` delegating to it.
- [ ] 3.4 Expose the migration as an operator command in `industry_sync_runner`
      with `--dry-run` and job-run tracking.
- [ ] 3.5 Update the operator docs with the canonical-key contract, the
      migration command, the rollout order (owning environment first), the
      verification query, and the rule that a window spanning the correction is
      split at that boundary rather than compared as one cohort.

## 4. Validation and gates

- [ ] 4.1 Focused tests, then the affected/full datahub suite.
- [ ] 4.2 Ruff check/format and `openspec validate --all --strict`.
- [ ] 4.3 spec-guardian, contract-reviewer and qa-reviewer after validation;
      resolve all P1 findings.
- [ ] 4.4 Branch conflict check, Draft PR, CI green.

## 5. Operator rollout (out of PR scope)

The owning environment is fixed and migrated first: `stock_industry` is a full
prod-to-dev snapshot upserted by `stock_code`, so a dev-only migration would be
reversed by the next data sync.

- [ ] 5.1 Deploy the image containing the normalization to the owning
      environment, then run `normalize-codes --dry-run` and `normalize-codes`,
      and confirm zero separated and zero unrecognized keys.
- [ ] 5.2 Deploy to dev, run the migration there, and confirm the dev data sync
      no longer reintroduces separated keys.
- [ ] 5.3 Confirm the next scoring session aggregates `industry_daily_metrics`
      and that the following session's `industry_momentum` is non-neutral.
