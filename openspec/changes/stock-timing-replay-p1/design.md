# Design: Stock Timing Replay Adapter P1

## Boundary

The adapter converts real stored evidence into the normalized pair already
accepted by P0. It does not replace the P0 evaluator and does not rank stocks.
The pure replay core accepts injected rows so causality and execution can be
tested without a database; the CLI is the only Mongo-facing layer.

## Frozen cohort

The input manifest has one `as_of`, one stable `source`, an explicit
`membership_basis=point_in_time`, and a provenance object containing the
immutable source artifact URI, its lowercase SHA-256, source member count,
subsequently-delisted count, suspended count, and affirmative coverage flags
for subsequently delisted and suspended names. `delisted_completeness` must be
`VERIFIED`; P1 refuses weaker assertions rather than relabelling them. The
source artifact hash is included in the source identifier passed to P0, so a
changed membership artifact changes P0's cohort identity even when its URI and
codes happen to be unchanged.

Every member supplies `stock_code`, `listed_on`, optional `delisted_on`, and
`evidence_at`. `evidence_at` cannot be later than manifest `as_of`; `listed_on`
must be on/before `as_of`; a name already delisted on/before `as_of` is not a
member. A name delisted after `as_of` remains in the cohort. The manifest
`as_of` must be earlier than the first replay session open. Codes are canonicalized
by the P0 evaluator, whose existing hash remains the cohort identity.

This schema deliberately does not query `IndividualStock.active_status`: the
current master has no historical listing dates and cannot prove past cohort
membership.

Ranked prediction membership is a separate, daily point-in-time fact. The
manifest therefore carries one prediction-cohort record for every signal date,
keyed by date, with an immutable artifact URI/hash, cohort fingerprint, member
count, and `data_as_of` no later than that session close. Every stored
prediction must repeat exact `freshness=FRESH`, the same fingerprint, artifact
hash and `data_as_of` in its input snapshot. Missing or stale daily
provenance fails closed. In particular,
legacy ranked rows that only carry a current-active cohort fingerprint are not
accepted as causal replay evidence.
The canonical hashes of the complete daily prediction-cohort map and trading
calendar are included in the fixed P0 configuration, so either evidence input
changes the P0 report identity.

## Model pin

The CLI resolves exactly one `ScoreModelVersion` by the manifest's
`model_version`. It must be `ACTIVE`, `scoring_mode=ranked`, and its stored
`config_hash` must equal the manifest `model_config_hash`. The manifest also
pins `horizon`, `entry_percentile`, `exit_percentile`, and friction. No runtime
default or environment mode may substitute for these fields.

## Replay mechanics

For each member the loader reads historical `StockDailyQuote` and
`StockScorePrediction` rows for the pinned window/model/horizon. Each score row
must be dated exactly to its signal session and its input snapshot must match
that date's manifest prediction-cohort record. Snapshot freshness must be
exactly `FRESH`; `data_as_of` must be timezone
aware and no later than the authoritative signal-session close. The pure core
requires adjusted open/close prices and treats missing/invalid price, unknown
trade status, suspension, and directional price-limit blocks as non-tradable.

The timing arm observes day D's stored percentile only after D closes and may
first fill at a later session's adjusted open. A flat arm enters when
`percentile >= entry_percentile`; a held arm exits when
`percentile <= exit_percentile`. A blocked fill remains pending; a newer causal
signal may replace it. Missing, non-finite, out-of-range, non-ranked, mismatched,
or unusable score rows emit no new signal.

The buy-and-hold arm forms its entry intent before the first replay session and
fills at the first tradable adjusted open in the declared window. The timing
arm cannot act on its first signal until a later session open. Both arms value at every usable
session's adjusted close and keep an open position marked to the last close at
the window end; there is no artificial closing-price liquidation.

Both arms share CNY cash, 100-share board lots, commission with minimum fee,
sell-side stamp duty, and directional slippage strictly below 100%, so an
execution price cannot be zero or negative. Every actual trade is emitted
with `status=FILLED`, positive quantity, and execution price. The adapter emits
P0's exact assumptions object and never calls a persistence path.

## Failure semantics

Manifest/model failures abort the command before replay. Per-stock missing
quotes or other replay failures become failed P0 rows and count against
coverage. Output is deterministic for identical injected evidence and remains
research-only/unvalidated.
