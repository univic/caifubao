# Causal paper timing and retrospective evidence boundary

## Why

The paper runner waits for future VERIFIED outcomes, executes signal-day scores
at that day's open, and sizes opening purchases using that day's close. Existing
paper records therefore cannot establish forward evidence. NAV also mixes
configuration tracks and quote loading turns suspended status 0 into tradable 1.

## What Changes

Introduce paper_causal_v1 config semantics; consume usable scores independently
of outcome verification, persist actual decision_at and the next calendar
execution_date, isolate config tracks, and size orders from opening information.
All records created by this first slice are explicitly REPLAY, even when run on
the latest date. A future change will provide immutable forward capture and its
120-session evidence counter. Legacy records remain historical and are not
silently included or upgraded.

## Impact

Datahub strategy runner/model/NAV/tests and paper runbook/active specs only.
No scoring math, public API, default scoring model, deployment, or database run.

## Non-goals

Live execution, model promotion, forward certification, complete portfolio risk
constraints, cadence/weight rebalancing, corporate-action accounting, and daily
benchmark alignment are subsequent independently validated roadmap slices.
