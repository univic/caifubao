# Design — Forward evidence capture and 120-session counting

## 1. Evidence classification (FORWARD vs REPLAY)

A `StrategyPaperRun` is certified **FORWARD** iff ALL of:

1. `decision_at` recorded at creation time;
2. `decision_at < open(execution_date)` where `execution_date =
   next_execution_date(signal_date, ChinaAStock market calendar)` (the #202
   helper, strictly the next market session; fail-closed on non-session dates /
   exhausted calendar) and `open(d)` = 09:30 CST of d (= 01:30 UTC). This is
   calendar-anchored: a run for a stale signal date has a past
   `execution_date`, so any later `decision_at` is after its open → REPLAY
   automatically. An on-time run after the signal session close has
   `execution_date` in the future → FORWARD.
3. the signal date is at or after the ACTIVE certified window's `start_date`
   for the record's exact (model_version, horizon, config_hash);
4. no pre-existing **COMPLETED (plan)** record with the same unique key is
   being overwritten. A SKIPPED/FAILED document MAY be rewritten without
   `--replace` — a same-day SKIPPED → evening FORWARD rerun is the intended
   forward capture path and MUST be classified FORWARD, never REPLAY.

Certification premise (explicit): usable scores for a signal date exist only
after that session's close (scoring cadence). The FORWARD predicate above does
not itself enforce that premise, so a future slice that scores intraday would
violate it; the operator cadence (run after the signal session's close) and
this premise are part of the certification contract.

Anything else — legacy/missing provenance, backfills of stale dates,
replacement runs, runs after `execution_date` opened, records outside a window
— stays `evidence_kind=REPLAY` (model default).

## 2. Immutability

- `StrategyPaperRun.evidence_kind` choices become `["REPLAY", "FORWARD"]`
  (default REPLAY for legacy/missing).
- The runner refuses (fail-closed, ValueError) to overwrite an existing FORWARD
  COMPLETED record: `run --replace` or rerun with a FORWARD target raises; a
  REPLAY/legacy COMPLETED record may be replaced (still producing REPLAY), and
  a SKIPPED/FAILED document may be rewritten without `--replace` (see §1).
- **Continuity (P2-1)**: queries that span the paper track MUST include FORWARD
  records exactly like REPLAY — the previous-holdings query used for the
  rebalance diff (`strategy_runner` run path) and the `run_nav` COMPLETED-run
  set. Evidence-kind filters MUST NOT silently exclude FORWARD records; only
  evidence certification and plan immutability differ between the kinds.
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
  and records `start_date` = the **certification session only** (today's
  trading session; never backdated — REPLAY can never upgrade, and roadmap
  names 06-10/09-04 as non-starts, so there is no backdating carve-out).
- **ACTIVE uniqueness per (model_version, horizon)** (P2-2a): certifying a new
  window MUST close any ACTIVE predecessor on that pair, whatever its
  config_hash — score-source or strategy-config changes mean evidence cannot
  span configurations, and only one paper track per model/horizon is live.
- **Explicit restart under the same key** (P2-2b): the operator MAY close an
  ACTIVE window and open a fresh one under the same
  (model_version, horizon, config_hash) with a new `start_date` (e.g. after a
  prolonged pause); the counter then restarts from the new start. Closing is
  recorded in the window document (`status=CLOSED`), append-only; a fresh
  window is a new document.
- Old-window FORWARD records keep their kind but no longer count once their
  window is CLOSED and superseded (the promotion gate uses a single window).

## 4. 120-session counter and progress

- Count = number of distinct FORWARD COMPLETED signal dates with `start_date
  <= date <= as-of` where as-of is the reporting date while the window is
  ACTIVE, or the window's close date once CLOSED. The window document has no
  `end_date` field — status ACTIVE/CLOSED is the lifecycle, and the counter
  stops at close.
- SKIPPED/FAILED/RUNNING and any REPLAY record never count; job SUCCESS records
  are never consulted.
- `forward progress` command reports: count / 120 sessions, first/last signal
  date, contiguous leading span, gaps (missing sessions within the window —
  allowed, counted separately, never fabricated), and window status.
  Promotion (roadmap 0.3) requires count >= 120 on one window plus the
  NAV-based evaluation against the research walk-forward expectation.

## 5. Operator cadence (no scheduling in this change)

Each trading session after scoring completes, operator runs
`strategy_runner run --date <session>` with the certified config; the runner
classifies FORWARD (same-session, pre-execution) or REPLAY (late) and writes
the record. A later slice wires this to a CronJob (roadmap 0.1 cadence).
