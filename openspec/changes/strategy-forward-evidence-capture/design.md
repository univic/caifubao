# Design — Forward evidence capture and 120-session counting

## 1. Evidence classification (FORWARD vs REPLAY)

A `StrategyPaperRun` is certified **FORWARD** iff ALL of:

1. `decision_at` recorded at creation time;
2. `decision_at < open(execution_date)` where `execution_date =
   next_execution_date(signal_date, calendar)` and `open(d)` = 09:30 CST of d
   (= 01:30 UTC). This is calendar-anchored: a run for a stale signal date has a
   past `execution_date`, so any later `decision_at` is after its open → REPLAY
   automatically. An on-time run after the signal session close has
   `execution_date` in the future → FORWARD. Because scores for a signal date
   exist only after that session's close, no intra-session information can leak
   into a FORWARD decision.
3. the signal date lies in `[window.start_date, window.end_date]` of the ACTIVE
   certified window for the record's exact `config_hash`;
4. no pre-existing record with the same unique key is being replaced.

Anything else — legacy/missing provenance, backfills of stale dates,
replacement runs, runs after `execution_date` opened, records outside a window
— stays `evidence_kind=REPLAY` (model default).

## 2. Immutability

- `StrategyPaperRun.evidence_kind` choices become `["REPLAY", "FORWARD"]`
  (default REPLAY for legacy/missing).
- The runner refuses (fail-closed, ValueError) to overwrite an existing FORWARD
  record: `run --replace` or rerun with a FORWARD target raises; only a
  REPLAY/legacy record may be replaced (still producing REPLAY).
- NAV is derived data: `nav` recompute may rewrite `nav_snapshot` on any record
  (REPLAY or FORWARD) but MUST NOT change `target_holdings`, `status`,
  `evidence_kind`, `decision_at`, `execution_date`, `config_hash`. Forward
  evidence = captured plan + config + timestamps; NAV curve is recomputable
  from independent quotes and never counts toward the session counter.

## 3. Certified forward window (append-only)

New collection `strategy_forward_windows`, one ACTIVE window per
`(model_version, horizon, config_hash)`:

```
{model_version, horizon, config_hash,
 start_date,          # first signal date allowed to be FORWARD (operator-chosen)
 decision_at,         # when the window was opened
 status: ACTIVE|CLOSED}
```

- Opened by an explicit operator command (`forward certify --model-version …
  --horizon … --config-json …`), which validates the registry model + config
  and records `start_date` = the date the window opens (today's trading
  session; never backdated — but may be set to a chosen past session only if
  the operator certifies that all runs for that window will be on-time
  forwards going forward; the default recommendation is the current session).
- Opening is append-only: once ACTIVE, the same key cannot be reopened with a
  different `start_date`; closing happens only when a config/score change
  requires a new window.
- Config change (different `config_hash`) or score-model change → operator
  closes the old window and opens a new one; old-window FORWARD records keep
  their kind but no longer count once the window is CLOSED and superseded (the
  promotion gate uses a single window).

## 4. 120-session counter and progress

- Count = number of distinct FORWARD COMPLETED signal dates with
  `start_date <= date <= end_date` in the ACTIVE window's exact key.
- SKIPPED/FAILED/RUNNING and any REPLAY record never count; job SUCCESS records
  are never consulted.
- `forward progress` command reports: count / 120, first/last signal date,
  contiguous leading span, gaps (missing sessions within the window — allowed,
  counted separately, never fabricated), and window status. Promotion (roadmap
  0.3) requires count >= 120 on one window plus the NAV-based evaluation
  against the research walk-forward expectation.

## 5. Operator cadence (no scheduling in this change)

Each trading session after scoring completes, operator runs
`strategy_runner run --date <session>` with the certified config; the runner
classifies FORWARD (same-session, pre-execution) or REPLAY (late) and writes
the record. A later slice wires this to a CronJob (roadmap 0.1 cadence).
