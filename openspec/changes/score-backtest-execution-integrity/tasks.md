## 1. Contract and tests

- [x] 1.1 Add failing engine tests for next-trading-day adjusted-open execution.
- [x] 1.2 Add failing tests for model-version isolation and unusable statuses.
- [x] 1.3 Add API/CLI tests for conditionally required `model_version`.

## 2. Implementation

- [x] 2.1 Shift all score-driven signals to the next actual trading day.
- [x] 2.2 Execute score-driven orders at adjusted open and preserve pending-order constraints.
- [x] 2.3 Require and propagate model version across API, CLI, and async entry points.
- [x] 2.4 Exclude `BLOCKED`/`FAILED` predictions and record execution timing.
- [x] 2.5 Expose the model-version field in the frontend backtest form.

## 3. Validation and rollout

- [x] 3.1 Run focused backend tests and Ruff checks.
- [x] 3.2 Run frontend lint and production build.
- [x] 3.3 Run `openspec validate --all --strict`.
- [ ] 3.4 Complete contract and QA reviews plus branch-conflict check.
- [ ] 3.5 Complete Draft PR/CI gate and deploy to dev.
- [ ] 3.6 Run one dev single-stock score-driven smoke test without claiming strategy validity.
