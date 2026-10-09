# Factor Lab Sweep Significance

## ADDED Requirements

### Requirement: Sweep reports family-wise corrected exploratory evidence

The Factor Lab `evaluate` report SHALL use each factor/horizon pair's existing
Newey-West t-statistic to compute an explicitly labelled **two-sided,
asymptotic standard-normal** p-value, and SHALL report the Bonferroni-adjusted
p-value `min(1, p * family_size)` for each pair. Its family SHALL include
every factor/horizon pair evaluated in this invocation, including failed and
unavailable statistics. A negative t-statistic SHALL be tested as fairly as
a positive t-statistic. This is an exploratory in-sample diagnostic, not a
strategy approval or a change to existing hard gates.

#### Scenario: A nominally significant trial loses significance in a sweep

- GIVEN a Newey-West t-statistic of 2.0 and ten tested pairs
- WHEN alpha is 0.05
- THEN the adjusted p-value exceeds 0.05
- AND the pair is labelled not significant without changing its economic gates

#### Scenario: Missing statistic still counts toward the family

- GIVEN two tested pairs, one with no valid t-statistic
- WHEN the sweep is annotated
- THEN the family size is two
- AND the missing pair is unavailable rather than silently excluded

### Requirement: Missing data cannot appear statistically confirmed

A pair SHALL NOT pass the significance flag when its Newey-West t-statistic
is missing or nonfinite, or when `n_dates` is below the existing factor-lab
minimum (120). The JSON SHALL distinguish `insufficient_dates`,
`unavailable_nw_t`, `not_significant`, and `significant`.

#### Scenario: Tiny sample with an extreme t-statistic

- GIVEN an extreme t-statistic but only 30 dates
- WHEN the sweep is annotated
- THEN the pair has status `insufficient_dates`
- AND its significance flag is false

### Requirement: The hypothesis family is explicit and validated

The CLI SHALL expose `--alpha` (strictly between zero and one) and
`--hypotheses-count` (optional positive integer at least as large as the
number of pairs actually evaluated). If not explicitly supplied, family size
SHALL equal the number of evaluated pairs and the report SHALL warn that any
unreported or earlier attempts are not counted. The JSON SHALL identify
whether family size was declared or derived from this invocation.

#### Scenario: Declared search history is larger than the current run

- GIVEN one evaluated factor/horizon pair and `--hypotheses-count 51`
- WHEN the report is generated
- THEN the Bonferroni multiplier is 51 rather than 1

#### Scenario: Invalid family sizes are refused

- GIVEN three evaluated pairs and `--hypotheses-count 2`
- WHEN the report is generated
- THEN the command fails with a clear error, not a misleading corrected result
