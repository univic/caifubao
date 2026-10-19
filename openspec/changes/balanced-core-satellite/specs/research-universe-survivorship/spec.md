# Research universe survivorship

## ADDED Requirements

### Requirement: The research panel SHALL report survivorship and ST coverage

A read-only audit SHALL report, per year, the number of distinct codes, codes
that first appear, codes that disappear, the ST share and the BSE share, and the
count of exchange-listed codes absent from the panel. The report SHALL name its
`st_flag_basis` (`current_snapshot` or `namechange_history`); a current-snapshot
ST flag SHALL NOT be presented as point-in-time. When the panel contains no
delisted cohort, the artifact SHALL set `survivorship_gap=true`.

#### Scenario: A panel with no delisted codes is flagged

- GIVEN a panel whose code set never shrinks
- WHEN the audit runs
- THEN the report sets `survivorship_gap=true`
- AND `st_flag_basis=current_snapshot`
- AND the replay artifact carries the flag

### Requirement: The research backfill SHALL be research-only

The backfill CLI SHALL fetch delisted instruments (`stock_basic` with
`list_status='D'`) and name-change history (`namechange`) for a requested window
and write a research parquet. It SHALL NOT write `basic_stock`,
`stock_daily_quote`, or any production collection. It SHALL report the fetched
code count, the date range and the residual gaps.

#### Scenario: The backfill does not touch production collections

- GIVEN a completed backfill
- WHEN the production `basic_stock` and `stock_daily_quote` counts are compared
- THEN they are unchanged
- AND the research parquet exists

### Requirement: A biased panel SHALL not claim unbiased returns

A replay built without the delisted/ST supplement SHALL carry
`survivorship_gap=true` and a limitation stating that returns are an upper bound.
Only a replay built with the supplement and an explicit audit may omit that
limitation.

#### Scenario: The bias travels to the API

- GIVEN a replay built from the current panel
- WHEN the detail endpoint returns it
- THEN the artifact contains `survivorship_gap=true`
- AND the UI shows the upper-bound warning
