# Design

Keep date as the signal date for CLI and uniqueness compatibility. Add actual
UTC decision_at, calendar execution_date, and evidence_kind=REPLAY (legacy
missing provenance also means retrospective). Add timing_version=paper_causal_v1
to normalized strategy config, so prior holdings and NAV use an exact config
hash and cannot consume old timing outputs. Existing completed records are
idempotent; replacement produces only retrospective evidence.

Read PENDING/TRACKING/VERIFIED/INSUFFICIENT_DATA scores; verification results are
never ranking inputs. BLOCKED/FAILED scoring records are unusable. Load flags
for that same cohort, preserving unknown/suspended status. Resolve next session
strictly from the shared ChinaAStock calendar; reject non-session signal dates
and exhausted calendar coverage.

NAV --from/--to remain signal-date bounds; quote and benchmark loading extends
to the last selected execution date. Attach execution-day NAV back to its signal
record. Reject legacy dates, mixed configs, duplicate execution sessions, and
missing execution-day quote coverage rather than write misleading NAV. The
caller must pass the same full strategy config, including capital, as run.

Opening portfolio value uses opening prices or prior known marks; end-of-day
close marking occurs after trades. Preserve the existing simple entry/exit
strategy. Full risk/weight/cadence and corporate actions remain explicit gaps.
