## Milestone A: corporate-action source integrity

- [x] Pure source hash/instrument/date completeness gate; default not connected
      to scheduled NAV writer.
- [x] Reject partial, failed, stale, tampered and REPLAY corporate-action inputs.
- [x] Validate record/ex/pay dates, source implementation status, values,
      duplicates and canonical output sorting.
- [x] Return explicit UNVERIFIED_UPSTREAM until independent source audit.
- [x] Add pure validation cases for incomplete coverage and source failures.
- [ ] CI validation, code review and datahub test suite.
- [ ] Authoritative PIT source ingestion with persisted immutable publication
      envelopes, Tushare/ETF distribution audits and failed-query receipts.
- [ ] Integrate a *verified* publisher with #323 simulator action interface.
- [ ] Proven absence of actions for each queried instrument, plus periodic
      vendor reconciliation and revision/announcement verification.
- [ ] Daily NAV materialization, delta/ledger parity and operational alerting.
- [ ] Actual Research schedule/halt PVC/window operator acceptance.
