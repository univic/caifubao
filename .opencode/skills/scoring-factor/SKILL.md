---
name: scoring-factor
description: Workflow for adding/changing scoring engine factors in caifubao. Covers components, config, service integration, tests, specs, and frontend.
license: MIT
compatibility: opencode
metadata:
  audience: contributors
  project: caifubao
---

## What this skill covers

Adding or modifying a scoring factor in the caifubao A-share quant scoring engine. The engine lives in `datahub/`, is consumed by `backend/` APIs, and rendered by `frontend/`.

## File map (touch in order)

| Step | File | What to do |
|------|------|------------|
| 1. Component | `datahub/app/lib/scoring_engine/components.py` | Add the component function (build_component / build_penalty pattern) |
| 2. Config | `datahub/app/lib/scoring_engine/config.py` | Add weight + lookback to all 3 horizon configs (5/20/60) |
| 3. Service | `datahub/app/lib/scoring_engine/scoring_service.py` | Import component, add to `_build_components`, update `max()` history limit if needed |
| 4. Tests | `datahub/app/test/test_scoring_service.py` | Cover: normal case, fallback/missing-data case, edge case. Use FakeQuote with relevant fields |
| 5. Spec | `openspec/changes/mvp-quant-demo/specs/stock-scoring/spec.md` | Add a Requirement with GIVEN/WHEN/THEN Scenario |
| 6. Tasks | `openspec/changes/mvp-quant-demo/tasks.md` | Add task item (check as [x]) |
| 7. Frontend types | `frontend/src/api/scores.ts` | Add TypeScript interface if exposing new fields |
| 8. Frontend display | `frontend/src/views/` | Wire into MarketView.vue or QuoteDetailView.vue |
| 9. Validation | Terminal | `ruff check && ruff format --check` on datahub, `make test-backend` |

## Component pattern

Every component returns a dict from `build_component`:

```python
def my_component(quote, ...) -> dict:
    value = compute_something(quote)
    if value is None:
        return build_component("my_id", "group", "Label", None, 0.0, weight)
    normalized = clamp(value / threshold)
    return build_component("my_id", "group", "Label", raw_value, normalized, weight, evidence={...})
```

- `component_id`: unique snake_case, matches the config weight key
- `group`: signal / trend / momentum / position / relative_strength / industry / volume / fundamental / risk
- `normalized`: 0.0–1.0 via `clamp()`
- `weight`: from config `weights[component_id]`
- `evidence`: dict with raw numbers for explainability

## Config weights by horizon

Short horizons (Score5) emphasize signal + momentum + volume. Long horizons (Score60) emphasize trend + relative_strength + valuation. Pick weights accordingly:

| Horizon | Priority factors |
|---------|-----------------|
| Score5  | signal_strength, momentum, volume_ratio |
| Score20 | trend_alignment, relative_strength, momentum |
| Score60 | trend_alignment, relative_strength, valuation |

New factor weight should start small (5–15) and be adjusted after calibration.

## Testing with FakeQuote

The test suite uses `FakeQuote` (no DB). To test a component that reads a new field, add the field to the `FakeQuote` constructor in the test setup. Example:

```python
FakeQuote.records.append(
    FakeQuote(code="sh600000", date=..., close=10.0, volume=5000000, peTTM=15.0)
)
```

## Self-check checklist

After implementation, verify:

- [ ] `ruff check datahub/app/lib/scoring_engine/` passes
- [ ] `ruff format --check datahub/app/lib/scoring_engine/` passes
- [ ] `make test-backend` from repo root passes
- [ ] New component has an id matching a config weight key
- [ ] All 3 horizon configs include the new weight
- [ ] Component handles missing data gracefully (returns 0.0 or 0.5 contribution)
- [ ] spec.md has at least one Scenario
- [ ] Frontend types reflect new fields if exposed
