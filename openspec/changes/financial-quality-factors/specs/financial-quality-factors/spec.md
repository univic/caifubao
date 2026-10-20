# Financial quality factors

## ADDED Requirements

### Requirement: Financial indicators SHALL be ingested point-in-time and idempotently

The runner SHALL fetch `fina_indicator` per instrument for a requested date
range and upsert one document per `(code, end_date, report_type, ann_date)` into
`stock_financial_indicator`, so restatement vintages are preserved. Tushare's
non-VIP endpoint requires `ts_code`, so the request unit is an instrument and a
range, not a period-only query. It SHALL store `ann_date` and `end_date` and the
selected quality fields, and SHALL reject a row without both dates, reporting
the rejected-row count in the run summary. Re-running a range SHALL update the
same identities without duplication. The runner SHALL be research-only: no
public API, no scheduling by this change, no scoring write.

#### Scenario: Re-running a range is idempotent

- GIVEN a range has already been ingested
- WHEN the runner is executed again for that range
- THEN the collection count for that range is unchanged
- AND the stored vintage is updated, not duplicated

#### Scenario: A row without an announcement date is rejected

- GIVEN a source row with no `ann_date`
- WHEN the runner normalizes it
- THEN the row is rejected and reported in the summary
- AND it cannot enter the research panel

### Requirement: Research joins SHALL use only reports announced by the decision date

For a panel row on decision date D, the merge SHALL use the report with the
greatest `ann_date <= D` (and the greatest `end_date` on ties). A report with
`ann_date > D` MUST NOT contribute any value. When no report exists, the quality
fields SHALL be null, not forward-filled from a later report.

#### Scenario: A later announcement is not visible

- GIVEN a report announced after D
- WHEN the panel row for D is built
- THEN the quality fields for D do not use it

#### Scenario: A pre-announcement gap stays null

- GIVEN a stock with no report announced by D
- WHEN the panel row for D is built
- THEN its quality fields are null
- AND it is not backfilled from a later report

### Requirement: Quality factors SHALL use current-snapshot industry/size neutralization and stay research-only

Quality ranks SHALL be computed within date and within the available CSRC
level-1 industry, with the bottom 30% by market cap excluded. The industry store
keeps `assigned_at` /  `industry_change_log`, but those rows are not backfilled
to the 2020 research window; this change therefore uses the current snapshot and
SHALL report it as a limitation, not as strict point-in-time industry
neutrality. The composite SHALL be the equal-weight
mean of the available quality ranks. The family SHALL be evaluated on the
delisted-supplemented panel with the existing rolling OOS protocol. No factor
SHALL be registered as a production score component.

#### Scenario: A small-cap outlier does not dominate the rank

- GIVEN two same-industry names with identical quality but very different size
- WHEN the rank is computed
- THEN the bottom-30% name is excluded
- AND the remaining name is ranked within its date/industry

#### Scenario: A missing quality leg does not drop the name

- GIVEN a name with ROE but no gross-margin value
- WHEN the composite is computed
- THEN the composite uses the available legs
- AND the name is not silently removed
