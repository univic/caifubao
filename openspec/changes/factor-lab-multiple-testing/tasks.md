# Factor-lab multiple-testing tasks

## Contract
- [x] Define family cardinality and the missing-statistic/minimum-sample semantics.
- [x] Document the exploratory full-sample / asymptotic-normal limitation.

## Implementation
- [x] Add pure reporting helper with Bonferroni-adjusted Newey-West p-values.
- [x] Integrate it into `factor_lab_runner evaluate` and persist family metadata.
- [x] Expose validated `--alpha` and `--hypotheses-count` flags.
- [x] Document how to interpret and reproduce the new fields.

## Validation
- [x] Add focused tests for multiple-testing dilution, sign symmetry, missing
      statistics, small samples, explicit family size, and invalid flags.
- [ ] Run CI-pinned Ruff and focused Datahub pytest.
- [ ] Run `openspec validate --all --strict` and required read-only QA gate.
