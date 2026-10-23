## ADDED Requirements

### Requirement: Read-only paper-trading readiness assessment

The system SHALL provide a readiness assessment that reads the persisted market,
score, signal and strategy state, returns per-check statuses, and performs no
write of any kind (no run record, no window change, no asset-status update).

#### Scenario: Assessment performs no writes

- GIVEN any database state
- WHEN the readiness assessment runs
- THEN the returned document contains only reads
- AND no strategy run, forward window or status document is created or updated

#### Scenario: Expected session respects the close

- GIVEN the A-share trade calendar and a Beijing timestamp before 15:00 on a
  session
- WHEN the expected completed session is derived
- THEN it is the previous session
- AND after 15:00 it is the current session
- AND on a non-session day it is the most recent past session

### Requirement: Readiness checks cover the evidence chain

The assessment SHALL check quote freshness and active-stock coverage, FQ
coverage of the latest quoted universe, scoring and signal freshness, the
ACTIVE forward window (including missing sessions), the halt switch, and ETF
market-data freshness and FQ coverage.

#### Scenario: Stale quotes are not ready

- GIVEN the latest active-stock quote is older than the expected session
- WHEN the assessment runs
- THEN the quote check is FAIL and the overall status is NOT_READY

#### Scenario: Coverage excludes unsupported universe

- GIVEN active BSE stocks have `data_capabilities.daily_quote=false`
- WHEN quote coverage is computed
- THEN those stocks are excluded from the denominator
- AND the check measures the supported universe rather than reporting a
  permanent coverage hole

#### Scenario: Missing FQ is not ready

- GIVEN an active stock has a quote on the latest session without `fq_factor`
- WHEN the assessment runs
- THEN the FQ check is FAIL and the overall status is NOT_READY

#### Scenario: Forward-window gap degrades readiness

- GIVEN an ACTIVE forward window whose start date precedes the assessment
- AND one or more sessions in that range have no FORWARD run with the window's
  config hash
- WHEN the assessment runs
- THEN the window check is WARN with the missing session count
- AND the overall status is DEGRADED, not READY

#### Scenario: Halt switch state is reported

- GIVEN no halt-file path is configured
- WHEN the assessment runs
- THEN the halt check is WARN and says order generation fails closed
- AND an engaged halt is reported as WARN with its recorded reason

#### Scenario: ETF data is part of readiness

- GIVEN the ETF universe is empty
- WHEN the assessment runs
- THEN the ETF check is WARN and the overall status is DEGRADED
- AND stale ETF quotes or ETF quotes without fund_adj FQ are FAIL

### Requirement: Overall status and process exit code are explicit

The assessment MUST return `READY` only when every check is PASS, `NOT_READY`
when any check is FAIL, and `DEGRADED` when the only non-PASS results are WARN.
The CLI MUST exit non-zero only for `NOT_READY`.

#### Scenario: Degraded is not fatal

- GIVEN every check is PASS except an unconfigured halt path (WARN)
- WHEN the CLI runs
- THEN it prints `DEGRADED` and exits 0

#### Scenario: Not ready is fatal

- GIVEN at least one check is FAIL
- WHEN the CLI runs
- THEN it prints `NOT_READY` and exits 1

### Requirement: Known limitations are surfaced with the verdict

Every assessment result SHALL include the current limitations: the current
(pre-#307) paper NAV marks raw prices and does not credit cash distributions,
paper targets are not a real account, forward-window gaps are permanent, and
ETF distributions share that pre-#307 NAV gap. The distribution limitation is
explicitly scoped to the NAV implementation this check observes; it is retired
when the coordinated execution-model change (PR #307) makes the ledger credit
cash distributions and share multipliers.

#### Scenario: Limitations accompany a READY verdict

- GIVEN all checks are PASS
- WHEN the assessment returns `READY`
- THEN the limitations list is still present and non-empty
- AND the READY verdict does not claim that paper results are tradable evidence

### Requirement: Readiness inputs are the persisted paper-window state

The assessment MUST read the persisted `strategy_forward_windows` and
`strategy_paper_runs` collections by their stable fields (`model_version`,
`horizon`, `config_hash`, `start_date`, `evidence_kind`, `date`) and SHALL NOT
import or depend on the strategy engine's in-process objects.

#### Scenario: Engine refactor does not change the inputs

- GIVEN the strategy engine is refactored while the persisted window/run field
  names are preserved
- WHEN the assessment runs
- THEN its window and progress checks work unchanged
- AND a renamed persisted field is a visible schema mismatch rather than a
  silent pass

### Requirement: Readiness never substitutes for authorization or certification

The assessment MUST NOT open, close or certify a forward window, MUST NOT count
itself as forward evidence, and MUST NOT relax the explicit operator
authorization required to open a window.

#### Scenario: Ready does not open a window

- GIVEN the assessment returns READY and no ACTIVE window exists
- WHEN the operator inspects the state afterwards
- THEN no forward window was created
- AND the window check still reports that certification is required

### Requirement: Research scheduling is explicit and suspended by default

The public research overlay SHALL provide the daily ETF sync and the readiness
assessment as CronJobs, and both SHALL be suspended by default so an operator
enables them deliberately after validating one manual run.

#### Scenario: Public manifest renders suspended

- GIVEN the example research overlay
- WHEN it is rendered with kustomize
- THEN both jobs exist with `suspend: true`
- AND the readiness job has no write-capable command
