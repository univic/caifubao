## 1. Spec + tooling

- [ ] 1.1 Author this change (proposal + delta) and get `openspec validate --all --strict` green with it present
- [ ] 1.2 Implement `datahub/scripts/factor_lab_paper_run.py` (`decide` / `mark`) with append-only, immutable records carrying `evidence_kind=REPLAY`, an idempotency key and expected cash
- [ ] 1.3 Charge one documented round trip per round trip plus the per-trade minimum commission per side
- [ ] 1.4 Keep the ledger out of production imports, Mongo writes, model registration and the score-driven paper track

## 2. Verification

- [ ] 2.1 Record the first live decision (`decide`) and replay it (`mark`) on real panel data
- [ ] 2.2 Append commands, as-of range, cost assumptions, `REPLAY` label and limitations to `docs/operations/strategy-experiments-2026-08.md`
- [ ] 2.3 qa-reviewer on the implementation; spec-guardian confirm the delta matches the code
