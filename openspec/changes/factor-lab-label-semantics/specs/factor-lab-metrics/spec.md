# Factor Lab Metrics Semantics

## ADDED Requirements

### Requirement: Reported edges and gates are net, sign-aware and overlap-aware

The factor lab SHALL form quantile buckets on the union of the two legs'
resolvable observations, so an observation whose entry a long cannot buy but a
short can still trades the short leg. Each leg's mean SHALL use its own
tradability-filtered label, and the long-only per-quantile statistic SHALL skip
rows whose long label is null.

The factor lab SHALL report a long-short spread that pays one round trip per leg:
`top_minus_bottom` equals the gross spread minus `2 × round_trip_cost()`, and the
per-quantile `avg_net_return` equals the gross bucket return minus one round
trip. Subtracting the same cost from both legs SHALL NOT be used, because it
cancels and reports a gross spread as net.

The profit-concentration gate SHALL measure the best single trade's share of the
long book's positive P&L — the quantity `autoresearch/profile.yaml` gates on —
and SHALL NOT use bucket sizes, which are equal by construction.

The walk-forward decay SHALL be signed: a factor that flips sign out of sample
SHALL score a large positive decay, not zero.

Turnover SHALL be measured between the books `horizon` sessions apart, not
between adjacent sessions.

The IC report SHALL publish a Newey-West t-statistic with `lag = horizon - 1`
alongside the i.i.d. one, because an h-day label makes h consecutive ICs overlap.
The IC itself is a rank statistic on the gross label and SHALL NOT be reported as
a net-of-cost quantity: a uniform cost preserves ranks and shifts no IC.

#### Scenario: Zero-edge factor

- GIVEN a factor uncorrelated with the forward label
- WHEN its net long-short spread is computed
- THEN the spread is `-2 × round_trip_cost()`, not zero

#### Scenario: Long-blocked but short-tradable observation

- GIVEN an observation whose T+1 entry is limit-up (long label null) but which
  can be shorted
- WHEN the long-short spread is computed
- THEN the observation is included in the short leg's bottom bucket
- AND it is excluded from the long leg's statistics

#### Scenario: Out-of-sample sign flip

- GIVEN a factor with positive train IC and negative validation/test IC
- WHEN walk-forward decay is computed
- THEN the decay exceeds the 0.2 gate and the factor fails

#### Scenario: Overlapping labels

- GIVEN an IC series built from h-day overlapping labels
- WHEN the IC report is produced
- THEN it publishes both the i.i.d. t-statistic and the Newey-West one with
  `lag = h - 1`
- AND the Newey-West statistic is the smaller in absolute value when the series
  is positively autocorrelated

#### Scenario: Horizon cadence

- GIVEN an h-day holding period
- WHEN the top book's turnover is measured
- THEN consecutive books are compared `h` sessions apart
- AND the report records the rebalance cadence
