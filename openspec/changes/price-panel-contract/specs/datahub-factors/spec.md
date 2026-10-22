# Price Panel Contract

## ADDED Requirements

### Requirement: Corporate actions are an explicit ledger

The datahub MUST represent every share or cash event as a ledger row with
`code`, `ex_date`, `action_type`, `share_ratio`, `cash_per_share`,
`rights_ratio`, `rights_price`, `announced_at`, and `source`.

#### Scenario: Bonus and transfer shares

- GIVEN a bonus row with `share_ratio = 1.0`
- WHEN the ex-date return is computed
- THEN the account value is `(1 + share_ratio) * close_ex + cash_per_share`
- AND a holder of one share has two shares after the event

#### Scenario: Rights issue is not a bonus

- GIVEN a rights row with `rights_ratio = 0.3` and `rights_price = 5.0`
- WHEN the ex-date return is computed
- THEN the account value includes the subscription cash outflow
  (`- rights_ratio * rights_price`)
- AND the event MUST NOT be treated as a share multiplier alone

#### Scenario: Invalid event

- GIVEN a rights row with a missing price or a negative ratio
- WHEN the ledger is loaded
- THEN loading fails with a validation error

### Requirement: Return index is positive and point-in-time

The adjusted series MUST be a total-return index built from raw prices and the
ledger; it MUST NOT be a vendor 前复权/后复权 price.

#### Scenario: Non-event day

- GIVEN an ordinary trading day with no event
- WHEN the index is built
- THEN the index return equals the raw close-to-close return

#### Scenario: Ex-date

- GIVEN an event on the ex-date
- WHEN the index is built
- THEN the index return equals the account-based event return
- AND the adjustment factor is constant between events

#### Scenario: Limit check

- GIVEN a non-event day whose return exceeds the board limit plus tolerance
- WHEN the index is built
- THEN a `daily_limit` violation is reported

#### Scenario: Event return may exceed the raw limit

- GIVEN a bonus event whose adjusted return exceeds the raw limit because of the
  share multiplier
- WHEN the index is built
- THEN no limit violation is reported for that ex-date

### Requirement: The builder refuses invalid panels

`scripts/build_price_panel.py` MUST write the panel and manifest only when the
violation list is empty; otherwise it MUST exit non-zero and print the violations.

#### Scenario: Missing ex-date

- GIVEN an event whose `ex_date` is not in the raw price index
- WHEN the builder runs
- THEN it reports `event_date_missing` and exits non-zero

#### Scenario: Reproducible manifest

- GIVEN a successful build
- WHEN the manifest is written
- THEN it contains the raw and ledger hashes, the output hash, the event list, and
  an empty violation list

### Requirement: Research consumes the contract

Research scripts MUST read the canonical panel and record its manifest; they MUST
NOT fetch or infer an adjusted series independently.

#### Scenario: 000977 research

- GIVEN the seeded ledger for 000977
- WHEN `build_adjusted.py` runs
- THEN it produces `000977_panel.parquet` with zero violations
- AND the research loader reads that panel
