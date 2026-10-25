# Milestone A — auditable provider query receipts

- [x] Add independent per-code successful-empty, positive or failed-query
      receipts in a Tushare replay-only collector.
- [x] Preserve a per-receipt digest and canonical event content SHA-256.
- [x] Fail closed for null, timeout, duplicate, future/revised as-of,
      missing record/pay and impossible split/cash distributions.
- [x] Add operator-only archive CLI with digest-addressed atomic JSON output.
- [x] Add pure mocked-provider/query receipt and file immutability tests.
- [ ] CI and code review.
- [ ] Vendor revision history and independent PIT availability audit.
- [ ] Include ETF fund distributions with correct cash and split rules.
- [ ] Live, operator-approved Research NAV publication and cash/holding parity
      acceptance with no automatic backdated FORWARD evidence.
