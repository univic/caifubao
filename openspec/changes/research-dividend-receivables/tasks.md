## Research NAV / Issue #319 acceptance

- [x] Define domain-neutral version-compatible entitlement and settlement event kinds.
- [x] Keep Research receivable accrual separate from spendable cash and NAV.
- [x] Validate event identities, record/ex/pay chronology and held quantity before mutation.
- [x] Add cash conservation, early settlement, sale-after-ex and rebuild tests.
- [ ] CI checks; independent code review of corporate action accounting.
- [ ] PIT authoritative stock/ETF corporate-action feed with durable identities, tax treatment, announced/record/ex/pay availability and completeness.
- [ ] Rework the NAV simulator to apply corporate events on ALL affected trading days, including non-rebalance days.
- [ ] Validate shares-split/bonus stock fractional/cash-in-lieu paths and delisting/write-offs.
- [ ] Persist daily NAV/receivables provenance without changing immutable FORWARD plans.
- [ ] Test missing data/holiday/cross-account contamination and daily replay/restore.
- [ ] Enable scheduled NAV projection only after all remaining accounting acceptance is green.
