# Research ledger correctness

## ADDED Requirements

### Requirement: Research execution SHALL be causal T+1

A decision made at the close of session D SHALL fill at the open of D+1 and
mark the resulting position at the close of D+1. The ledger MUST NOT fill and
mark in the same session, and MUST report every blocked fill instead of silently
dropping it.

#### Scenario: First session is cash

- GIVEN a target decided at the close of D
- WHEN the ledger processes D
- THEN the account remains in cash at the D close
- AND the fill happens at the D+1 open

#### Scenario: Cash is conserved

- GIVEN a buy or sell fill
- WHEN the ledger applies price, commission, stamp duty and transfer fee
- THEN cash changes by exactly the traded notional minus/plus costs
- AND negative cash is rejected

### Requirement: Actual holdings SHALL never exceed the configured cap

The ledger SHALL cap actual positions, not only the target list. If an exit is
blocked, the ledger MAY hold the residual position but MUST NOT open a new name
while the account is at the cap.

#### Scenario: A blocked exit blocks a new entry

- GIVEN ten held names and a blocked exit
- WHEN the next target includes an eleventh name
- THEN the new name is not bought
- AND a `max_positions` or blocked-exit reason is recorded

### Requirement: Capital SHALL be continuous unless explicitly modelled per fold

The ledger SHALL accept initial cash and positions so a single account can run
across folds. Turnover SHALL be reported against both initial capital and average
NAV; a per-fold reset MUST be labelled as such and MUST NOT be presented as one
continuous curve.

#### Scenario: One account across two decision windows

- GIVEN an account with cash and positions after window one
- WHEN window two starts
- THEN it uses the ending cash and positions
- AND no capital reset is inserted

### Requirement: Corporate actions and delisting exits SHALL be explicit

Share multipliers and cash-per-share distributions SHALL be applied on their
ex-date. A delisting exit SHALL use an explicit exit price or be reported as a
write-off whose value is `lots × board_lot × price`. Missing quotes alone MUST
NOT be treated as a zero-value delisting.

#### Scenario: Write-off value uses board lots

- GIVEN 100 shares written off at a last price of 10
- WHEN the ledger reports the write-off
- THEN the reported value is 1,000, not 10

#### Scenario: A long suspension is not a delisting

- GIVEN a held name with no quote for 21 sessions
- WHEN the ledger marks the account
- THEN it carries the last known price and flags the staleness
- AND it does not silently set the position to zero
