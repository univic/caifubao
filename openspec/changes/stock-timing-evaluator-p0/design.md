# Design: Stock Timing Evaluator P0

## Decision boundary

The evaluator answers "does one fixed timing rule generalise across this frozen
cohort?" It does not answer "which stock should be bought now?". The primary
result is therefore a pooled report, not a leaderboard.

## Decision record

A pure constructor accepts already-resolved score evidence, position state, an
injected UTC decision clock, and the authoritative ordered trading calendar.
P0 supports one signal basis: full-cohort `percentile` in `[0, 1]`.

- Flat + `percentile >= entry_percentile` -> `ENTER`.
- Held + `percentile <= exit_percentile` -> `EXIT`.
- Held without an exit -> `HOLD`.
- Flat without an entry, missing percentile, unusable score status, or
  freshness other than `FRESH` -> `NO_TRADE`.

The record pins `stock_code`, `signal_date`, injected `decision_at`, immediate
next trading-calendar `execution_date`, `expires_at` (that session's close),
position state, score/percentile, thresholds, `data_as_of`, freshness, model
version, config hash, evidence kind, and reason tokens. It always carries
`research_only=true` and `validation_status=UNVALIDATED`. `decision_at` must be
timezone-aware and satisfy `signal_session_close <= decision_at <
next_session_open`. P0 requires an explicit `signal_close_at` equal to the
authoritative China A-share session close and fixes expiry to the immediate
execution session close; it does not guess or extend either boundary. A
non-session signal date, an unavailable/mismatched session boundary, or calendar
exhaustion fails closed. Identical injected inputs produce identical records.
The record is not an order or a persisted shared artifact.

## Pool evaluation

The evaluator accepts an explicit cohort plus normalized timing and same-stock
buy-and-hold result pairs produced with `save_result=False`. The CLI adapter runs
both sides with the same initial cash, first tradable next-open entry, last-close
valuation, board lots, and friction. Input is canonicalized to sorted unique
codes. The cohort identity is lowercase SHA-256 of UTF-8 canonical JSON with
sorted keys and compact separators over `codes`, RFC 3339 UTC `as_of`, and
`source`. The report also pins model version, configuration hash, historical
window, and a caller-supplied `delisted_completeness` assertion whose allowed
values are `VERIFIED`, `NOT_VERIFIED`, and `UNKNOWN`; the evaluator never infers
that assertion from surviving rows.

Each normalized side must name the same requested stock and carry identical
comparison assumptions for initial cash, historical window, next-open execution,
last-close valuation, positive board lot, and friction object. A mismatch fails
that row with a stable code; an index benchmark cannot substitute for the
same-stock buy-and-hold side. Only filled SELL records with positive quantity
and execution price count as completed trades. Daily observations count only
when they carry a finite non-negative equity value and fall inside the declared
historical window; every trade date must also stay inside that window.

A row is `SUCCESS` only when both sides complete; otherwise it is `FAILED` with
a stable reason code and counts against coverage. `observed_sessions` is the
number of aligned daily-equity observations on both sides. `completed_trades` is
the timing side's completed SELL count. A successful row is
`evidence_eligible` only at 120+ observed sessions and 5+ completed trades.
`coverage = successful_count / requested_count`.

The pooled evidence gate requires at least 50 requested codes, at least 50
evidence-eligible codes, and coverage >= 90%. The report provides aggregate
excess-return distribution and positive-excess share, never a winner. It always
remains `research_only=true` and `validation_status=UNVALIDATED`, even when its
mechanical gates pass.

## Scoring-mode resolution

Scoring configuration and mode are independent. Configuration precedence stays
explicit `scoring_config`, ACTIVE registry config, then built-in config. Mode
precedence is:

1. ACTIVE registry mode, when pinned;
2. explicit runtime `scoring_mode`, when the registry leaves mode unspecified;
3. legacy environment/default behaviour for an unregistered model or a registry
   entry with no mode.

A conflict with a pinned registry mode fails before any write. A retired entry
is unregistered. Registry lookup failure preserves legacy fallback only for
`DEFAULT_MODEL_VERSION`; a non-default named version fails closed because its
construction cannot be proven. `score_single_stock` remains raw-only and fails
closed when effective mode is ranked; ranked models require complete-cohort
scoring. Predictions store the effective mode in their input snapshot.

## Stop-loss timing

Across `SCORE_THRESHOLD`, `SCORE_MOMENTUM`,
`MULTI_HORIZON_CONSENSUS`, and `TOP_N_ROTATION`, a close breach is known only
after that session closes. It may set a pending `SELL`, but cannot execute at
the breached close or that session's already-passed open. The next tradable
adjusted open is the first eligible execution price; blocked sessions retain the
pending order. All four paths use the existing shared directional tradability
predicate: a session is executable only when adjusted open is finite and
positive, trade status is known and not suspended, and the direction is not
blocked by the applicable limit-up/limit-down rule. Missing quote/open/status or
unknown status is blocked, not tradable. A score exit already pending at an open
executes first. If the range ends first, the stop exit remains unexecuted;
end-of-range valuation is not labelled as the stop-loss fill.
