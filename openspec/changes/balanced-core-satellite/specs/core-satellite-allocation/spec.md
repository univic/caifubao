# Core-satellite allocation

## ADDED Requirements

### Requirement: The balanced 50k account SHALL use a lot-feasible core

At an AUM of ¥50,000 the balanced profile SHALL allocate **50% core / 40%
satellite / 10% cash**. The core SHALL be the lot-feasible three-ETF variant
`40% 510300.SH / 30% 518880.SH / 30% 513100.SH` (¥10,000 / ¥7,500 / ¥7,500).
The ¥25,000 four-leg 25/25/25/25 variant is **not buildable at ¥50k** because one
100-share lot of the treasury ETF (`511010.SH`, around par) alone exceeds a
¥6,250 leg; it SHALL be deferred to an AUM of at least ¥100,000 and SHALL NOT be
silently approximated. The 10% cash sleeve is the defensive leg at ¥50k.

#### Scenario: The sleeve weights sum to the AUM

- GIVEN an AUM of 50,000
- WHEN the combined replay is built
- THEN the initial core, satellite and cash NAVs are 25,000, 20,000 and 5,000
- AND the core legs are 10,000 / 7,500 / 7,500
- AND the treasury leg is absent from the 50k artifact

#### Scenario: The treasury variant is not silently forced

- GIVEN an AUM of 50,000
- WHEN a caller requests the 25/25/25/25 core
- THEN the builder reports that the treasury leg is unaffordable
- AND does not emit a partial or negative-cash core

### Requirement: The core SHALL respect board lots, fees and quarterly rebalancing

The core replay SHALL buy board-legal lots (100-share lots; STAR 200 + 1 when
applicable), charge commission with the minimum and slippage, and rebalance to
its target weights at the declared quarterly cadence. A buy that cannot fit one
board minimum SHALL be skipped and reported; cash SHALL never go negative. ETF
orders SHALL NOT charge stamp duty. The tranfer fee SHALL apply only when the
execution config sets it.

#### Scenario: A leg that cannot fit a lot is reported

- GIVEN a core leg whose one lot costs more than the available sleeve cash
- WHEN the core rebalances
- THEN no buy is emitted for that leg
- AND the skip is reported

### Requirement: Cross-sleeve rebalancing SHALL be explicit, banded and costed

The combiner SHALL rebalance toward 50/40/10 at the declared quarterly cadence
with a **±5 percentage-point drift band**: no cross-sleeve trade is emitted while
every sleeve is inside its band. When one or more sleeves breach, breaches SHALL
be restored in the deterministic order core, then satellite, then cash; only the
notional needed to restore the target is moved, and the moved notional is charged
once at `max(5, 0.0005 * moved_notional)` with no stamp duty. A move is a
notional transfer between sleeves, not an ETF lot trade; the receiving sleeve's
own lot rules apply when it next buys. Uninvested sleeve cash stays in that
sleeve and is not swept. If a sleeve has no usable price on a rebalance session,
the combiner SHALL fail closed.

#### Scenario: A cross-sleeve rebalance moves only the drift

- GIVEN a core sleeve that has grown to 57% of the account
- WHEN the quarterly cross-sleeve rebalance executes
- THEN only the excess over 50% is moved
- AND the cost is charged on that moved notional

#### Scenario: Inside the band, nothing is traded

- GIVEN sleeve weights of 52/39/9
- WHEN the quarterly review runs
- THEN no cross-sleeve trade is emitted
- AND no cross-sleeve cost is charged

### Requirement: The satellite SHALL be pinned to one frozen configuration

The satellite SHALL be a **new ¥20,000 build** of the frozen `riq_v1`
configuration: five factors (`reversal_10`, `amihud_20`, `trend_60`,
`volatility_20`, `rsi_14`), walk-forward ICIR or causal-sign weights with a
21-session lag, quarterly rebalance, top-10 equal-weight with a 2N buffer,
`fwd_h20` weight label, `evidence_kind=REPLAY`. Its `config_hash`, AUM (¥20,000)
and the label used for weight estimation SHALL be recorded in the artifact. The
build SHALL report the lot and minimum-commission scale limitation rather than
silently scaling the documented ¥50k curve.

#### Scenario: The satellite config is traceable

- GIVEN a combined replay
- WHEN its satellite sleeve is inspected
- THEN the config hash, AUM, names and weight label are present
- AND the artifact states any scale/lot limitation

### Requirement: The combined replay SHALL disclose unmodelled constraints

The artifact SHALL record that QDII premium/discount and purchase limits are not
modelled, that the core uses tushare fund prices rather than the production quote
store, and that the historical result is a research measurement. It SHALL also
record the core price source and its as-of date.

#### Scenario: The limitations travel with the result

- GIVEN a combined replay
- WHEN it is read by the API or UI
- THEN the limitations are present in the artifact
- AND they are shown next to the metrics
