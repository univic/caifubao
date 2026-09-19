## 1. Spec + tooling

- [x] 1.1 Author this change (proposal + delta) and get `openspec validate --all --strict` green with it present
- [x] 1.2 Implement `datahub/scripts/factor_lab_paper_run.py` (`decide` / `mark`) with append-only, immutable records carrying `evidence_kind=REPLAY`, an idempotency key and expected cash
- [x] 1.3 Charge one documented round trip per round trip plus the per-trade minimum commission per side
- [x] 1.4 Keep the ledger out of production imports, Mongo writes, model registration and the score-driven paper track

## 2. Verification

- [x] 2.1a Record the first live decision (`decide`) on real panel data (2026-09-18 -> execution 2026-09-21)
- [ ] 2.1b Replay it (`mark`) once the 2026-09-21 session exists; until then no NAV, drawdown or blocked-entry rate has been measured
- [x] 2.2 Append commands, as-of range, cost assumptions, `REPLAY` label and limitations to `docs/operations/strategy-experiments-2026-08.md`
- [x] 2.3 qa-reviewer ran; its 5 P1 + 11 P2 findings were fixed in the follow-up commit and verified on synthetic panels (recorded in the experiments log)
