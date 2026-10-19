# Research paper ledger for factor-composite strategies (REPLAY-only)

## Why

The repository can research a factor composite (`factor-lab` panel, IC/ICIR,
capacity, small-account sizing) and it can run a **score-driven** paper track
(`strategy-paper-runner`, `timing_version=paper_causal_v1`). It cannot run a
factor-driven book on live market data with virtual money while staying inside
the causal/replay contracts: the score-driven runner requires a registered
`ScoreModelVersion` and its own timing version, and the research scripts emit
reports, not records.

Without that surface the only way to observe a composite out of sample is a
documented backtest, and any ad-hoc "paper" record risks becoming
indistinguishable from forward evidence. The promotion gate requires
**immutable forward evidence** (120 sessions); a research ledger must therefore
be explicitly REPLAY-only and must never feed the forward counter or the target
export.

## What Changes

- Add a research-only paper ledger CLI under `datahub/scripts/` that writes an
  append-only decision record per decision date (`evidence_kind=REPLAY`) and
  replays those records into a NAV path.
- Decisions are causal: weights may use only factor ICs whose forward labels
  were realised strictly before the decision session; the target basket is
  selected at the decision session's close and the intended execution session is
  a later session's open.
- Marks replay exactly the recorded decisions: blocked entries are skipped
  (never back-filled, cash stays idle), blocked exits roll forward, costs are one
  documented round trip plus the per-trade minimum commission.
- The ledger stays unregistered and separate from the score-driven paper track:
  it does not create or read `StrategyPaperRun`, certified forward windows or
  the 120-session counter, and it is not consumed by `strategy report|nav|
  forward progress` or the target export.

## Impact

- New: `openspec/changes/strategy-paper-research-ledger/`,
  `datahub/scripts/factor_lab_paper_run.py`, a results entry in
  `docs/operations/strategy-experiments-2026-08.md`.
- Unchanged: `strategy-paper-runner` (score-driven contract and registration are
  not relaxed), `strategy-forward-evidence-capture` (REPLAY already excluded),
  `strategy-target-export`, factor-lab factor definitions and the panel schema.
