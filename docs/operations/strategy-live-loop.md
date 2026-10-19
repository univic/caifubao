# Strategy Live Loop (pre-broker) — halt switch and reconciliation

Operator runbook for the minimal live loop: what now gates a paper fill, how to
**stop order generation immediately**, and how to diff the last plan against an
account snapshot. This is a research/demo system: nothing here places an order
or is investment advice. Forward-evidence rules stay in
[`strategy-forward-window.md`](strategy-forward-window.md).

## 1. Halt / kill switch (roadmap 2.4)

Order generation checks a persisted, **default-OFF** halt flag *first*. While it
is engaged the runner produces **no plan and no orders** (a dry-run preview is
refused too), the job is recorded `FAILED`, and the CLI prints
`"status": "HALTED"` and exits `2`. A halt never advances the 120-session
forward counter and never changes what counts as FORWARD evidence.

The flag path is **never guessed**. Supply it with the
`CAIFUBAO_STRATEGY_HALT_FILE` environment variable (set on the datahub pod) or
an explicit `--halt-file PATH`. With no path configured, order generation fails
closed.

```bash
# Stop order generation now (records who/when/why).
caifubao strategy halt engage --by "$OPERATOR" --reason "manual emergency stop"

# Inspect the current state (halted?, who set it, when, why, history).
caifubao strategy halt status

# Resume (records who/when/why).
caifubao strategy halt resume --by "$OPERATOR" --reason "incident cleared"

# One run can pin the same file explicitly.
caifubao strategy run 2026-09-11 --halt-file /var/lib/caifubao/strategy-halt.json
```

The flag file is JSON:

```json
{
  "halted": true,
  "changed_by": "operator-a",
  "changed_at": "2026-09-20T01:02:03+00:00",
  "reason": "manual emergency stop",
  "history": [{"action": "halt", "changed_by": "operator-a", "changed_at": "...", "reason": "..."}]
}
```

A missing file means OFF — **deleting the flag file returns the runner to normal
order generation**, so treat it as durable state; only an absent *path*
(neither `CAIFUBAO_STRATEGY_HALT_FILE` nor `--halt-file`) fails closed. A
corrupt/unreadable file also fails closed (it is never read as "not halted").
Nothing halts automatically yet — an automated trigger is out of scope for this
slice.

## 2. Limit-locked fills and board minimums (roadmap 2.2)

The paper NAV path now refuses a **BUY at a limit-up open** and a **SELL at a
limit-down open**, using the same board-aware limits as the research panel
(main ±10 %, ChiNext/STAR ±20 %, BSE ±30 %, ST main ±5 %, 0.1 pp tolerance),
derived from the open against the previous close. If the verdict cannot be
derived (no previous close), the order is refused for the side a limit would
block. `nav` prints the refusals as `blocked_orders` plus a `blocked` list with
side and reason — never silently dropped.

**Where the gate applies — and where it does not.** Gating happens at
**fill/NAV time**, on the execution-session quote. `strategy run` and
`strategy export` produce the *plan* from signal-date information, when the
execution-day open and limit verdict do not exist yet, so the plan and export
output are **not** limit-gated. Blocked orders are only printed by the ephemeral
`nav` CLI; they are **not persisted** to `StrategyPaperRun` in this slice, so
`report`/`export` will not show them.

Note the research/production divergence: the research replay treats an
underivable verdict as tradable (fail open) to keep historical replays
reproducible, while this production path fails closed.

ST classification is best-effort: `StockDailyQuote.isST` can be missing for
recent rows, and an ST name with an absent ST bit is treated at the main-board
±10 % band. The blast radius is bounded by the default
`constraints.exclude_st=true` (ST names are not selected); only disabling that
filter on data with a missing ST bit can expose it.

Opening BUYs also respect board minimums: STAR/科创板 (`sh688`/`sh689`) needs at
least 200 shares and then 1-share increments; ChiNext/创业板 and the main board
trade in 100-share lots. SELL sizing is exempt from the buy minimum so an
odd-lot position can always be closed out. The A-share transfer fee (过户费) is
charged on both sides, is reserved when sizing, and enters the strategy
`config_hash` when overridden:

```bash
caifubao strategy run 2026-09-11 --dry-run \
  --config-json '{"execution":{"transfer_fee_rate":0.00001}}'
```

## 3. Reconciliation: planned vs actual (roadmap 2.3)

Read-only diff of the last plan against an operator-supplied account snapshot.
No REST endpoint, no scheduler, no writes.

```bash
caifubao strategy reconcile \
  --plan plan.json --account account.json \
  --quantity-tolerance 0 --cash-tolerance 0 --output reconcile.json
# exits 0 when clean, non-zero ("reconciliation tolerance breach") otherwise
```

`plan.json` is the target holdings list **with target quantities** (convert the
exported target weights to shares against the account NAV before reconciling);
`account.json` is
`{"positions": {"sh600000": 1000}, "cash": 1234.5, "planned_cash": 1234.5,
"as_of": "2026-09-11"}`. The result reports per-name quantity drift (planned,
actual, drift, tolerance, breach), cash drift, `missing_in_account`,
`unexpected_in_account` and a `breaks` list, and is JSON-serialisable for
logging. A missing planned name, an unplanned holding, unverifiable cash, or
drift above tolerance is a break. The `--quantity-tolerance`/`--cash-tolerance`
defaults are `0`: state your tolerance explicitly when you accept one.
