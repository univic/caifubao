# Strategy Live Loop Minimal — trading-ledger reconciliation transition

## MODIFIED Requirements

### Requirement: Plan-versus-account reconciliation MUST be structured and fail loud

A read-only reconciliation MUST compare a domain-owned *executed account plan*
(the target quantities and planned cash projected from the TradingPortfolioLedger)
against a broker/account snapshot. The trading ledger projection MUST be the
only authoritative plan-side source in TRADING_PAPER/LIVE; it MUST NOT read
ResearchSimulationLedger events, target weights, or research databases to
manufacture executed holdings. It MUST report per-symbol quantity drift,
cash drift, missing and unexpected holdings and tolerance breaches. Missing
or unverifiable trading projection and account cash MUST fail closed.
Existing operator CLI output and exit-code contracts MUST be preserved for
legacy read-only reconciliation until a separately deployed trading adapter
takes ownership. The reconciliation MUST NOT mutate either source.

#### Scenario: Trading plan state is derived from its own ledger

- GIVEN a TradingPortfolioLedger projection with filled quantities and cash
- WHEN an observed account snapshot is reconciled against it
- THEN the trading projection, not a research target, determines planned shares
- AND cash and positions are checked and all breaches are structured

#### Scenario: Research ledger cannot masquerade as trading plan

- GIVEN a research simulation projection or only target portfolio weights
- WHEN trading reconciliation is requested
- THEN it fails closed with a source-domain or missing-evidence error
- AND no trading account drift is silently reported as zero

#### Scenario: Legacy operator reconciliation remains read-only

- GIVEN an existing `reconcile_plan_vs_account` CLI invocation
- WHEN the compatibility path runs
- THEN its JSON schema and non-zero exit on drift remain unchanged
- AND it does not persist a ledger, submit orders or read a broker
