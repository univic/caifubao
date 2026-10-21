# Financial quality factors (research-only)

## Why

The price/volume signal space is exhausted. The delisted-adjusted RIQ OOS falls
from +11.14% to +1.78%/yr, and the first new-signal sweep (valuation, intraday,
event) found no usable edge. The next genuinely different source is **reported
financial quality**: profitability, margins, asset turnover, leverage and
growth, which are not derived from the same price/volume data.

Tushare `fina_indicator` is available and includes `ann_date` / `end_date`, so a
point-in-time join is possible. The repository has no financial statement
collection or quality factor today.

## What Changes

- Add a research-only `stock_financial_indicator` collection and a runner that
  fetches `fina_indicator` per instrument over a date range and idempotently
  upserts selected quality fields.
- Define the PIT rule: for a decision date D, use the greatest `ann_date <= D`
  (greatest `end_date` on ties). A report or restatement announced after D may
  not enter; the quality fields stay null before the first announcement.
- Merge the PIT financial snapshot into the frozen panel and add current-snapshot
  CSRC L1 industry/size quality ranks (ROE, gross margin, net margin, asset
  turnover, low leverage, revenue/profit growth, operating cash) plus a quality
  composite.
- Evaluate the quality family on the delisted-supplemented panel with the same
  rolling OOS protocol; record the result even if negative.

## Non-goals

- No public API, scoring-model registration, frontend or promotion.
- No real orders.
- No claim of forward evidence; the output is `REPLAY`.
- No use of a report or restatement not announced by the decision date.
- Strict PIT industry neutrality is not claimed: the store holds only a current
  classification, so the industry snapshot's look-ahead is a documented
  limitation.

## Impact

`datahub/` model, runner, research merge/factors, tests; `docs/` experiment log;
`openspec/` this change. The production quote/scoring path is unchanged.
