# Factor-lab rolling out-of-sample tasks

## 1. Contract

- [x] 1.1 Define folder tiling, purge/embargo cutoff and refusal conditions.
- [x] 1.2 Define the stitched-curve metrics and the per-fold/config record.

## 2. Implementation

- [x] 2.1 Extract the daily replay loop into `replay_book` and keep the existing
      `factor_lab_account_replay` variants' output unchanged.
- [x] 2.2 Add `--mode oos` with `--train-sessions`, `--test-sessions`,
      `--embargo`, `--names`, `--aum` and the per-mode `--step` default.
- [x] 2.3 Freeze fold weights from IC dates only at or before the purge+embargo
      cutoff and evaluate the test window only.
- [x] 2.4 Refuse a negative `--embargo` (argparse type and a `report_oos` guard)
      and require an explicit `--step` for mixed `oos`/label-based runs.
- [x] 2.5 Record the effective IC history bounds (`ic_first_date`/`ic_cutoff`) and
      guard the execution-column merge against duplicate panel keys.
- [x] 2.6 Document `oos`/`sprint` in the module docstring and
      `docs/operations/factor-lab.md`, and scope the unchanged-JSON claim to the
      document / `--output` payload (stdout now carries only JSON).
- [x] 2.7 Record the account-replay `--start-date`/`--end-date` in-sample arm and
      its echoed window in the spec delta (implementation owned by the
      integrator).

## 3. Tests and validation

- [x] 3.1 Synthetic-panel tests for the purge cutoff boundary (derived from the
      open-to-open label), fold tiling, the short-panel, negative-embargo and
      mixed-mode refusals and the config/per-fold record.
- [x] 3.2 Cover the windowed account-replay arm in
      `test_factor_lab_account_replay.py` (integrator-owned).
- [x] 3.3 Run the full `datahub/app/test/` suite and the CI ruff checks.
