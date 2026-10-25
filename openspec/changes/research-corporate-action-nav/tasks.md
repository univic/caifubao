# Milestone A corporate actions (Issue #319)

- [x] Apply supplied dividend entitlements to the Research simulation ledger
      on ex-date, using record-date closing holdings.
- [x] Convert accrued cash to available funds on/after pay-date.
- [x] Process supplied share splits before ex-date open fills.
- [x] Fail-closed for missing required valuation sessions, invalid and
      duplicate events; test replay and NAV conservation.
- [ ] Pass public datahub tests, Ruff, OpenSpec.
- [ ] Build authoritative PIT stock and ETF corporate-action collection,
      including announcement/revision/ex/pay provenance and tax treatment.
- [ ] Corporate-action completeness verification independent of target holdings
      and deterministic source fingerprint / input hash.
- [ ] Integrate declared input feed into Research NAV daily materializer and
      separate immutable research simulation artifacts.
- [ ] Full trading-session marks and delisting/cash-in-lieu/limit-block goldens.
- [ ] Alert on stale data and corporate-action input gaps.
- [ ] Validate multi-day Research runtime before calling Milestone A complete.
