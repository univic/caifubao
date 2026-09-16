# Industry Classification Code Normalization

## ADDED Requirements

### Requirement: Industry classification keys are canonical stock codes

The datahub MUST persist `StockIndustryClassification.stock_code` in the same
canonical form used by quotes, factors, signals and predictions: market prefix
immediately followed by six digits, with no separator (`sh600036`). The monthly
CSRC sync MUST normalize the baostock code (for example `sh.600036` or
`sz.000001`) to that form before it looks up an existing record or writes a new
one.

#### Scenario: New classification from a separated baostock code

- GIVEN baostock returns the code `sh.600036`
- AND no classification exists for that stock
- WHEN the industry sync persists the row
- THEN the stored `stock_code` is `sh600036`
- AND the stored L1 code and name come from the CSRC string

#### Scenario: Resync is idempotent

- GIVEN a canonical classification record already exists for a stock
- WHEN the monthly sync returns that stock's separated baostock code again
- THEN the existing canonical record is updated
- AND no second record is created for the same stock

#### Scenario: Canonical baostock code

- GIVEN baostock returns a code that already has no separator
- WHEN the sync normalizes it
- THEN the canonical value is unchanged

### Requirement: Legacy separated keys are migrated to canonical form

The datahub MUST provide an operator-run migration for the `stock_industry`
collection. The legacy shape is exactly the baostock form `xx.NNNNNN`
(`^[a-z]{2}\.\d{6}$`); a key that is neither canonical nor that legacy shape is
reported in the migration summary and left untouched, never guessed at or
dropped. The migration MUST be idempotent, MUST support a dry-run that performs
no write, MUST NOT refresh `last_synced_at` (the 30-day resync cutoff depends
on it), and MUST preserve the point-in-time fields `assigned_at` and
`industry_change_log`.

When a canonical record for the same stock already exists the migration MUST
leave exactly one record, keep a usable classification, and MUST NOT silently
discard the legacy change history. The surviving record keeps the canonical
classification together with its own `assigned_at`; the legacy anchor is
adopted only when the canonical record has no classification to anchor.
Attaching an earlier legacy anchor to a later canonical classification would
let a historical date inherit a classification that did not exist yet.

The migration MUST NOT fabricate a change-log entry: a merge is a key rewrite
for the same stock, not a classification change, and a fresh timestamp would
make the surviving record unusable for every earlier signal date under the
point-in-time attribution rule.

#### Scenario: Dry-run reports without writing

- GIVEN the collection contains separated legacy keys
- WHEN the migration runs in dry-run mode
- THEN it reports the number of records it would rename and merge
- AND no document is modified or deleted

#### Scenario: Rename preserves point-in-time fields

- GIVEN a separated key whose canonical key does not exist
- WHEN the migration runs
- THEN the document is renamed to the canonical key
- AND `assigned_at` and `industry_change_log` are unchanged
- AND `last_synced_at` is not rewritten by the migration

#### Scenario: Merge onto an existing canonical record

- GIVEN both a separated key and its canonical key exist
- WHEN the migration runs
- THEN exactly the canonical record remains
- AND its classification is kept, or adopted from the legacy record when the
  canonical record has none
- AND the surviving anchor is the earlier of the two when both records carry
  the same L1 classification, and the canonical record's own anchor otherwise
- AND the legacy change history is unioned onto the surviving record
- AND no new change-log entry is appended for the merge
- AND the separated document is removed

#### Scenario: Merge adopts a classification only when the canonical record has none

- GIVEN the canonical record exists but holds no L1 classification
- AND the legacy record holds one
- WHEN the migration merges them
- THEN the surviving record adopts the legacy classification
- AND it adopts the legacy `assigned_at` as that classification's anchor

#### Scenario: A sync that ran before the migration does not hide history

- GIVEN the fixed sync created a canonical row with a fresh `assigned_at` while
  the legacy separated row still existed
- AND both rows carry the same L1 classification
- WHEN the migration merges them
- THEN the surviving record keeps the earlier legacy anchor
- AND the classification stays attributable to dates before that sync

#### Scenario: Unrecognized keys are reported, not rewritten

- GIVEN `stock_industry` contains keys that match neither `^[a-z]{2}\.\d{6}$`
  nor the canonical shape
- WHEN the migration runs
- THEN those keys are listed in the summary
- AND no document with such a key is modified or deleted

#### Scenario: A renamed record stays usable for earlier signal dates

- GIVEN a legacy record whose `assigned_at` and change history predate a
  historical signal date
- WHEN the migration renames it to the canonical key
- THEN the strategy runner still attributes its classification to that earlier
  signal date

#### Scenario: Migration is idempotent

- GIVEN a collection whose keys are all canonical
- WHEN the migration runs again
- THEN it reports zero renamed and zero merged records
- AND no document changes

### Requirement: Industry lookups resolve against canonical keys

Because classifications are keyed canonically, the scoring path MUST resolve a
stock's industry from its canonical code rather than falling back to the
neutral component value. The same lookup MUST remain point-in-time safe: the
classification store holds one current row per stock, so a classification may
be attributed to evaluation date `D` only when `assigned_at <= D` and no
`industry_change_log` entry post-dates `D`.

#### Scenario: Industry momentum uses the stock's industry

- GIVEN a canonical classification row in effect on the evaluation date
- AND a prior-session `industry_daily_metrics` row for its L1 industry
- WHEN a ranked scoring run computes the `industry_momentum` component
- THEN the component reports the industry value for that stock
- AND it does not use the neutral fallback

#### Scenario: A later classification is not attributed to an earlier date

- GIVEN a stock's current classification was assigned after evaluation date `D`,
  or its change history records a change after `D`
- WHEN scoring evaluates `D`
- THEN `industry_momentum` uses the neutral fallback for that stock
- AND the per-day industry prefetch omits that classification
- AND aggregation does not build a past-dated `industry_daily_metrics` row from
  a classification that was not yet in effect

### Requirement: The scoring configuration and model version are unchanged

This change MUST NOT alter component weights, thresholds, the scoring formula,
the CSRC classification source, the `*_sw_*` field names, or
`DEFAULT_MODEL_VERSION`. The existing configuration already declares the
`industry_momentum` component; this change only makes the input it already
declares resolvable, so no threshold/weight rule requires a new model version.

Because the component becomes non-neutral from the first session that has
industry metrics, scores are not comparable across the correction boundary:
calibration or comparison windows that span it MUST be split, the boundary MUST
be recorded wherever affected numbers are reported, and pre-correction and
post-correction sessions MUST NOT be presented as one comparable cohort.
Producing a corrected full-market calibration over a historical range is a
separate, operator-authorised replay.

#### Scenario: Configuration and version are unchanged

- GIVEN the horizon scoring configuration and `DEFAULT_MODEL_VERSION` before and
  after this change
- WHEN the two are compared
- THEN every component weight, threshold and percentile is identical
- AND the default model version string is identical

#### Scenario: A window spanning the correction is not reported as one cohort

- GIVEN a calibration or comparison window that starts before the correction
  and ends after it
- WHEN its numbers are reported
- THEN the correction boundary is recorded with them
- AND the pre-correction and post-correction sessions are not presented as a
  single comparable cohort

