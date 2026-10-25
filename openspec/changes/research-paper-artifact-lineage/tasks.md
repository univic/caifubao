## B2 Research trace

- [x] Verify stored Research simulation artifact SHA and immutable strategy
      identity; fail closed for absent, overlapping, mismatched derivations.
- [x] Attribute fills and rejection events only for exact decision/execution
      date, research account and spec hash; include stable event IDs.
- [x] Bound event payloads and redact account internals/unneeded raw marks.
- [x] Expose trace only through the existing user-JWT-protected decision API.
- [x] Present research-only fill, rejection, and dated session ledger tables.
- [x] Backend integration and pure helper tests for tampering, mixed cohort,
      duplicate identities, overlapping artifact and missing decision date.
- [ ] All CI green and review merge.
- [ ] Date all future Research split ledger events to improve traceability.
- [ ] Connect complete corporate-action source and materialized NAV after A.
- [ ] Validate actual Research records/lineage with read-only cluster smoke.
