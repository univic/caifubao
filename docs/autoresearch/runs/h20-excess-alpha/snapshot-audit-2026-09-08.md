# H20 snapshot evidence audit — 2026-09-08

## Conclusion

The −1.27% figure reported as daily in the [#208 handoff](https://github.com/univic/caifubao/pull/208)
is reproducible as a **mean nominal H20 holding-period cohort return**, not a daily
portfolio return. Compounding these overlapping cohorts into a five-month −75%
loss is invalid. This audit found no price mismatch in the comparisons below;
it does not establish full-universe source correctness or strategy profitability.

## Reproduce

From the repository root, with existing local artifacts:

```bash
PYTHONPATH=datahub datahub/.venv/bin/python -m app.jobs.autoresearch_h20_audit \
  --snapshot datahub/research/autoresearch/h20_excess_alpha/snapshot.parquet \
  --from-date 2026-01-05 --to-date 2026-06-02 \
  --output /tmp/h20-audit-new.json
```

Optionally add `--comparison-snapshot /path/to/merged.parquet` and
`--reference-quotes /path/to/quotes.json`. Reference JSON is an array with
`stock_code`, `date`, `open_hfq`, `close_hfq`; raw database extracts are not committed.
The output must be a new file. The command performs no database access, candidate
search, ledger write or certification. Exit zero means a report was produced;
inspect counts rather than treating exit status as a clean-data gate.

The [aggregate JSON](snapshot-audit-2026-09-08.json) records SHA-256 identities,
inspected columns, tolerances and exact counts. Primary snapshot SHA-256 was
recomputed and matches its existing manifest. Input dates normalize to UTC.
The command assumes the existing nominal H20 export convention; it does not infer
or verify the horizon from a manifest. Window bounds select return cohorts only;
integrity checks and execution joins retain the complete snapshot.

## Observed evidence

| Check | Result |
| --- | --- |
| Primary rows / duplicate key rows | 3,525,955 / 0 |
| Primary versus merged snapshot | All 3,525,955 primary keys matched; zero differences in the nine inspected columns |
| Additional merged-only rows | 5,677,504; not independently audited here |
| Entry label self-join | 3,171,235 matched, zero unusable pairs or price mismatches |
| Entry missing date / unmatched dated label | 348,060 / 6,660 |
| Exit label self-join | 3,067,326 matched, zero unusable pairs or price mismatches |
| Exit missing date / unmatched dated label | 349,389 / 109,240 |
| Eligible unusable execution prices / invalid chronology | 0 / 0 |
| Unusable snapshot open / close prices | 349,287 / 349,287 (all rows; includes ineligible observations) |
| Bounded dev quote sample | 45 of 45 keys matched, zero open/close mismatches |

Inspected fields: `stock_code`, `date`, `open_hfq`, `close_hfq`, `eligibility`,
`actual_entry_date`, `actual_exit_date`, `actual_entry_open_hfq`,
`actual_exit_open_hfq`. Snapshot-to-snapshot comparisons are exact after date
normalization; price joins use `rtol=atol=1e-9`. Factors, scores and other columns
were not compared. Missing or unusable evidence is not a passing price check;
the reasons for those gaps have not been individually established.

The previously collected read-only dev sample covers `sh600519`, `sh601398`,
`sz000001` at 15 dates: 2024-01-02, 2024-01-03, 2024-01-31, 2025-01-02,
2025-01-03, 2025-02-10, 2026-01-05, 2026-01-06, 2026-02-03, 2026-03-02,
2026-03-03, 2026-03-31, 2026-06-02, 2026-06-03, 2026-07-02.
Self-joins and merged snapshots may share source errors, and three symbols do
not certify all symbols/dates. The report records the local sample checksum
for traceability, without publishing the raw database extract.

## Return interpretation and follow-up

For inclusive signal dates 2026-01-05 through 2026-06-02, 498,026 eligible,
finite-positive-price, chronologically valid rows form 97 signal-date cohorts.
Each stock return is `exit_open / entry_open - 1 - 0.0035`; stock returns are
averaged equally within each date, then those date means are averaged equally.
The result is **−1.2663523377482762% per nominal H20 cohort**. Cost is the existing
research convention: two commissions, sell stamp duty and two slippage legs.
The exporter advances 20 per-stock quote observations after entry and rolls
forward for tradability. Observed holding durations are 28 / 29 / 103 calendar
days (minimum / median / maximum) in this window.

This is integrity evidence, not an unlocked test-window optimization run. The
candidate/profile, official experiment ledger and evaluator remain unchanged.
Existing evaluator drawdown still compounds overlapping cohort excess returns
(`h20_excess_alpha.py::_evaluate_range`); portfolio daily NAV and dev/research
entry, horizon, universe and cost alignment require a separate change. Audit
results alone do not justify rebuilding snapshots, promoting a strategy or
opening the forward evidence window.

## Validation

Seven synthetic tests cover return units, missing/mismatched/nonfinite evidence,
chronology, duplicate keys, timezone normalization, snapshot overlap, empty
windows and refusing to overwrite an input. Full datahub suite: **592 passed**.
Ruff **0.15.15**, pinned by CI: full lint and format passed. Local Ruff 0.16.5
reports 658 existing-rule violations across the repository; changed files pass
that version too. Strict OpenSpec: **16 passed**. Mandatory final reviews and
PR CI are tracked in the change tasks and PR.
