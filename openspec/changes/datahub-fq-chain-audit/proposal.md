## Why

The stock ex-rights/after-adjustment chain (`adj_factor → fq_factor → *_hfq`) is
accepted today by the writer-switch verifier and the market-wide jump scan, but
there is no single repeatable instrument that answers three questions at once:
is the active universe covered, is the stored arithmetic internally consistent,
and does the stored factor still match its tushare source? During the
2026-10-23 review the chain looked healthy but the only evidence was ad-hoc
queries and a two-stock spot probe; a provider error or an empty coverage set
could also have been read as a pass.

This change adds that instrument as a read-only audit. It is separate from
`fq-adj-factor-fix` (already merged) so the merged ledger stays frozen.

## What Changes

- Add `datahub/app/lib/datahub/data_integrity_keeper/handler/fq_chain_audit.py`
  with pure helpers for source comparison (tolerance, source gaps, rows before
  the first provider factor), stored-HFQ arithmetic, coverage summaries and a
  verdict assembler.
- Add `datahub/scripts/fq_chain_audit.py`: read-only CLI that reports active
  supported-universe coverage (BSE excluded), a bounded deterministic sample
  compared with tushare `adj_factor`, the optional market-wide jump scan, and
  PASS/WARN/FAIL exit codes.
- Fail closed: any sample error with no successfully compared sample, zero/low
  active coverage, a missing active factor, a sampled divergence beyond
  tolerance, or a market-wide factor jump makes the audit FAIL rather than
  PASS.
- Record the 2026-10-23 research-environment findings, the residual risks and
  the re-run method in `docs/operations/fq-chain-audit.md`.

## Impact

- New handler and script, one focused test module, and the operator document.
- No write path, no schema change, no scoring/strategy behavior change.

## Non-goals

- Recomputing or repairing FQ values (the audit is read-only; repair stays the
  existing `backfill`/`force` path).
- Replacing the writer-switch verifier or `check_fq_factor_integrity.py`; the
  audit is an additional, independent evidence source.
- Auditing ETF factors; that is covered by the ETF capability's own checks and
  the daily all-or-nothing FQ contract.
