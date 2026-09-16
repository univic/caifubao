# Strategy Portfolio Risk Limits

## ADDED Requirements

### Requirement: Position limits MUST be enforced, not merely declared

`max_single_stock_pct` MUST bound every emitted target weight. When equal
weighting would give a name more than that cap, the engine MUST scale the target
weights down to the cap and leave the difference in cash; it MUST NOT ignore the
cap, drop names to force the average down, or scale any name up. A configured
limit that cannot be honoured MUST fail the run rather than silently degrade.

#### Scenario: Equal weight below the cap is unchanged

- GIVEN a selection whose equal weight is at or below `max_single_stock_pct`
- WHEN target weights are computed
- THEN every weight equals the equal weight
- AND the invested total is unchanged from the equal-weight plan

#### Scenario: Equal weight above the cap is scaled down into cash

- GIVEN a selection whose equal weight would exceed `max_single_stock_pct`
- WHEN target weights are computed
- THEN every target weight equals the cap
- AND the uninvested remainder stays in cash
- AND no selected name is dropped for this reason

### Requirement: Industry concentration MUST be bounded

`max_industry_pct`, when configured, MUST bound the summed target weight of any
single level-1 industry classification. The only available taxonomy is the
baostock CSRC (证监会) level-1 code held in
`StockIndustryClassification.industry_code_sw_l1`; the `sw` field names are
legacy and MUST NOT be read as Shenwan. The industry cap MUST apply in addition
to, never instead of, the single-position cap, and no selected name may be
dropped to satisfy it.

#### Scenario: An over-weighted industry is scaled down

- GIVEN a selection where one industry's summed equal weight would exceed
  `max_industry_pct`
- WHEN target weights are computed
- THEN that industry's summed weight does not exceed the cap
- AND the shortfall stays in cash
- AND no name is dropped from that industry

#### Scenario: Both caps bind together

- GIVEN a selection where the single-position cap and the industry cap both bind
- WHEN target weights are computed
- THEN each weight is the smallest of the equal weight, the single-position cap,
  and the industry cap divided by that industry's largest bucket count
- AND no selected name is dropped
- AND the remainder stays in cash

#### Scenario: No industry cap configured leaves weighting unchanged

- GIVEN a config without `max_industry_pct`
- WHEN target weights are computed
- THEN industry membership does not change any weight

### Requirement: Missing classification MUST be capped, never ignored

The engine MUST, when `max_industry_pct` is configured, group names whose level-1
classification cannot be resolved for the signal date into one `UNKNOWN` bucket
that is subject to the same cap as a named industry. Supplying no classification
map at all is a caller error and MUST fail closed; supplying a map that cannot
resolve some names MUST bucket those names as `UNKNOWN` rather than exempting
them.

#### Scenario: Unresolvable classification forms its own capped bucket

- GIVEN `max_industry_pct` configured and a map that cannot resolve the selected
  names for the signal date
- WHEN target weights are computed
- THEN those names are grouped into one `UNKNOWN` bucket
- AND that bucket is capped exactly like a named industry

#### Scenario: No classification map at all fails closed

- GIVEN `max_industry_pct` configured and no classification map supplied
- WHEN target weights are computed
- THEN the run fails closed with a specific error
- AND no uncapped target weights are emitted

### Requirement: Eligibility constraints MUST NOT be silently bypassed

The engine MUST NOT emit target weights that skip a configured eligibility
constraint. When `min_trade_amount_cny` is greater than zero, or when any
exclusion flag is active, a plan MUST be assembled from an explicit universe flag
map; assembling a plan with no flag map while such a constraint is active MUST
fail closed rather than silently selecting from the whole prediction set.

#### Scenario: A plan cannot be built without the universe map

- GIVEN a config with an active eligibility constraint (a positive liquidity
  floor, or an enabled exclusion flag)
- WHEN a plan is assembled without a flag map and predictions exist
- THEN the run fails closed with a specific error
- AND no unfiltered target holdings are produced

#### Scenario: An active floor is applied whenever a plan is built

- GIVEN `min_trade_amount_cny` greater than zero and a flag map is supplied
- WHEN the eligible set is built
- THEN every selected name's traded amount meets the floor

### Requirement: The liquidity floor MUST exclude untradable names

When `min_trade_amount_cny` is greater than zero, a name MUST NOT be selectable
unless its traded amount on the signal date meets or exceeds that floor. A name
whose traded amount is unknown MUST be treated as not selectable, so missing
liquidity evidence can never widen the tradable set.

#### Scenario: An illiquid name is excluded

- GIVEN an eligible name whose signal-date traded amount is below
  `min_trade_amount_cny`
- WHEN the eligible set is built
- THEN that name is not selectable
- AND it does not appear in the target holdings

#### Scenario: Unknown traded amount is excluded

- GIVEN an eligible name with no traded-amount evidence for the signal date
- WHEN the eligible set is built
- THEN that name is not selectable

#### Scenario: A zero floor leaves liquidity unconstrained

- GIVEN `min_trade_amount_cny` equal to zero
- WHEN the eligible set is built
- THEN traded amount does not exclude any otherwise-eligible name

### Requirement: Risk limits MUST NOT introduce look-ahead

Every limit MUST be evaluated from information available on or before the signal
date. A current-snapshot field MUST NOT be used to reconstitute a historical
state: delisting or trading status MUST NOT be inferred from `active_status`, and
an industry classification MUST be used only when it can be shown to have been
assigned on or before the signal date. A classification that cannot be shown to
predate the signal date MUST be treated as unresolvable, not assumed.

#### Scenario: Current snapshot status is not used as a filter

- GIVEN a candidate whose current record is marked delisted or inactive
- WHEN the eligible set and target weights are computed for a historical signal
  date
- THEN the current snapshot status does not by itself exclude or include it

#### Scenario: A classification assigned after the signal date is not used

- GIVEN an industry classification whose assignment date is after the signal date
- WHEN the industry cap is evaluated for that signal date
- THEN that name is treated as unresolvable and falls in the `UNKNOWN` bucket
- AND it is not attributed to the later classification

### Requirement: Emitted weights MUST satisfy the caps after rounding

Target weights MUST be emitted such that the caps hold for the emitted values,
within a per-name tolerance no larger than one unit in the last emitted decimal
place. Rounding MUST NOT raise a weight above a cap.

#### Scenario: Rounded weights still satisfy the caps

- GIVEN weights that satisfy the caps before rounding
- WHEN they are emitted at the configured precision
- THEN each emitted weight is at or below the single-position cap
- AND each industry's emitted total is at or below the industry cap within the
  per-name tolerance
- AND the emitted total never exceeds the investable fraction

### Requirement: Risk limits MUST be part of the pinned configuration

The resolved risk limits MUST be part of the strategy configuration that is
normalized, validated, and hashed, and the schema MUST accept every limit this
change introduces. Changing any risk limit MUST therefore change the
configuration hash, so certified forward evidence cannot span two different
risk-limit configurations. Absent a schema change the new key MUST be rejected as
unknown rather than silently dropped. Limits absent from the configuration MUST
remain absent, so existing pinned hashes stay reproducible.

#### Scenario: Changing a limit changes the config hash

- GIVEN two configs that differ only in `max_single_stock_pct`,
  `max_industry_pct`, or `min_trade_amount_cny`
- WHEN each is normalized and hashed
- THEN the two hashes differ

#### Scenario: An unset limit does not enter the default config

- GIVEN the shipped default configuration
- WHEN it is normalized and hashed
- THEN `max_industry_pct` is absent
- AND the hash of the shipped default is unchanged by this change

#### Scenario: An invalid limit is rejected, never coerced

- GIVEN a limit outside its valid range or of the wrong type
- WHEN the config is validated
- THEN validation fails with a specific error
- AND no default is silently substituted for the supplied value
