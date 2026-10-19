# Strategy Live Loop Minimal

## ADDED Requirements

### Requirement: Limit-locked sessions MUST NOT fill an order

The production paper path MUST refuse a BUY on a session whose open is at the
upper price limit and a SELL on a session whose open is at the lower price
limit, using the board-aware limit classification already owned by the research
panel (main board ±10 %, ChiNext/STAR ±20 %, BSE ±30 %, ST ±5 % on the main
board, with the same 0.1 pp tolerance) derived from the session open against the
previous close. For the same code and session the production fill gate and the
research panel MUST reach the same limit verdict. A refused order MUST be
reported with its side and reason; it MUST NOT be silently dropped.

#### Scenario: A planned BUY is refused on a limit-up open

- GIVEN a planned BUY for a name whose open sits at the upper price limit
- WHEN the session's orders are executed
- THEN no BUY fill is produced for that name
- AND the refusal is reported with the BUY side and the limit-up reason
- AND cash is not spent

#### Scenario: A planned SELL is refused on a limit-down open

- GIVEN a held name whose open sits at the lower price limit
- WHEN a rebalance removes it from the target
- THEN no SELL fill is produced for that name
- AND the refusal is reported with the SELL side and the limit-down reason
- AND the position is carried forward instead of being marked as exited

#### Scenario: A session at neither limit still fills

- GIVEN an open strictly inside the board's limit band
- WHEN the side is executed
- THEN the fill happens at the slippage-adjusted open as before

### Requirement: Unavailable limit information MUST fail closed

The production paper path MUST refuse an order when the limit verdict for its
session cannot be derived (no explicit flag and no usable previous close). The
refusal MUST apply to the side that a limit would block, and the report MUST
state that the verdict was unknown. An unknown verdict MUST NOT be treated as
"not at the limit", and it MUST NOT abort unrelated orders, marks or the rest of
the run.

#### Scenario: A BUY with no derivable verdict is refused loudly

- GIVEN a planned BUY whose session has no limit flag and no previous close
- WHEN the session's orders are executed
- THEN no BUY fill is produced
- AND the refusal is reported with an unknown-limit reason

#### Scenario: Unknown limit information does not stop other names

- GIVEN one name with a derivable in-band verdict and another with no derivable
  verdict
- WHEN the session's orders are executed
- THEN the derivable name fills normally
- AND only the unknown name is refused and reported

### Requirement: BUY sizing MUST respect board minimums and the transfer fee

BUY quantity MUST be board-legal: STAR/科创板 (sh688/sh689) requires a 200-share
minimum and then accepts 1-share increments; ChiNext/创业板 and the main board
trade in 100-share lots. Sizing MUST reserve the full cost of the order —
slippage-adjusted value, commission (minimum-commission aware) and the
two-way A-share transfer fee (过户费) — so an order is skipped only when the
board minimum itself cannot fit. A SELL MUST be exempt from the buy minimum so an
odd-lot position can always be closed out.

#### Scenario: STAR buys start at 200 shares

- GIVEN a STAR-market code and a budget that fits 300 shares
- WHEN the BUY is sized
- THEN the quantity is a 200 + n form (300 here), never a 100-share lot

#### Scenario: A budget below the STAR minimum produces no order

- GIVEN a STAR-market code and a budget below the cost of 200 shares
- WHEN the BUY is sized
- THEN no order is produced (the minimum is never violated downward)

#### Scenario: Main board and ChiNext keep 100-share lots

- GIVEN a main-board or ChiNext code and a budget that only fits 150 shares
- WHEN the BUY is sized
- THEN the quantity is 100 shares, not 150

#### Scenario: The exit is never blocked by the buy minimum

- GIVEN a position of any size that must be closed
- WHEN the SELL is sized
- THEN the whole position is offered, regardless of board lot

### Requirement: The transfer fee MUST be part of the execution cost model

The A-share transfer fee (过户费) MUST be charged on both sides of a trade in the
paper cost model, at the current rate, and MUST be reserved when sizing an
opening BUY and accounted for on both legs of a round trip. For the same trade,
the research cost model and the paper cost model MUST charge the same fee
fractions, and a configured execution cost MUST enter the strategy `config_hash`
so a fee change is a distinct configuration.

#### Scenario: A buy and its exit both pay the transfer fee

- GIVEN a filled BUY and its later SELL of the same position
- WHEN the fill ledger is inspected
- THEN both legs carry a non-zero transfer-fee cost

#### Scenario: The transfer fee reduces the sized quantity

- GIVEN a budget that fits a whole number of shares without the transfer fee
- WHEN the BUY is sized with the transfer fee reserved
- THEN the quantity is reduced (or the order is skipped) so total spend respects
  the budget

#### Scenario: A fee change changes the config hash

- GIVEN two validated configs that differ only in an execution cost
- WHEN their config hashes are compared
- THEN the hashes differ

### Requirement: Plan-versus-account reconciliation MUST be structured and fail loud

A read-only reconciliation MUST diff the planned target portfolio against an
account snapshot and report, per name, the planned and actual quantity and their
drift; the cash drift; names missing from the account; names held but not
planned; and a list of tolerance breaches. The result MUST be JSON-serialisable
for logging. When any drift exceeds its configured tolerance — or the account
holds an unplanned name, or cash cannot be verified — the reconciliation MUST
fail loudly (non-zero exit at the CLI) rather than report success.

#### Scenario: A matching account reconciles cleanly

- GIVEN a plan and an account whose quantities and cash match
- WHEN they are reconciled
- THEN no quantity or cash drift is reported
- AND the breaks list is empty and the result is JSON-serialisable

#### Scenario: A quantity mismatch is a loud break

- GIVEN an account holding fewer shares of a planned name than the plan
- WHEN they are reconciled
- THEN the per-name drift is reported with its tolerance and breach flag
- AND the breaks list is non-empty and the CLI exits non-zero

#### Scenario: Missing and unexpected names are reported

- GIVEN a planned name absent from the account and a held name absent from the plan
- WHEN they are reconciled
- THEN both are listed as missing-in-account and unexpected-in-account
- AND both produce breaks (tolerance cannot excuse a name mismatch)

### Requirement: Order generation MUST honour a default-OFF halt switch

A persisted halt flag MUST gate order generation. The flag MUST default to OFF,
MUST be stored in a file whose path is supplied explicitly (there is no silent
default path), and MUST record who changed it, when, and why, together with a
change history. Order generation MUST check the halt FIRST and, when engaged,
MUST fail loudly with a HALTED result that produces no orders and no plan —
never a silent empty success. Engaging and lifting the halt MUST be available as
operator operations, and the halt MUST NOT fabricate forward evidence: it MUST
NOT advance the 120-session counter and MUST NOT change what counts as FORWARD.

#### Scenario: A halted runner produces no orders

- GIVEN an engaged halt flag
- WHEN order generation is invoked (including a dry-run preview)
- THEN it fails with a HALTED result naming the operator and reason
- AND no plan, no holdings and no orders are produced

#### Scenario: Lifting the halt restores order generation

- GIVEN a halt that was engaged and then resumed with attribution
- WHEN order generation is invoked again
- THEN it proceeds as an un-halted run

#### Scenario: The halt survives a process boundary

- GIVEN a flag file written by one process
- WHEN another process checks the halt
- THEN it reads the same engaged state and refuses order generation

#### Scenario: An unconfigured halt store fails closed

- GIVEN no halt path configured
- WHEN order generation is invoked
- THEN it fails loudly instead of running un-halted
