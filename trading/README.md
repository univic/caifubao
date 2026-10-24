# Trading runtime boundary (Issue #308)

This package is the **independent, disabled-by-default** Trading domain
foundation; it does not import `datahub.app`, Mongo, research simulation
ledger, external brokers, credentials or network APIs. The initial account
is a deterministic paper-only state machine; it processes explicit offline
fills rather than placing orders or generating fills from approval.

`TradingHaltStore` owns its own operator action history. Missing or malformed
halt state is **fail closed**; first initialization requires an explicit halt
transition, then an attributed resume. All paper risk previews and explicit
fills consult halt first. In contrast the old datahub halt file is only
legacy order-plan operations and research replay consumes frozen snapshots.

`TradingPaperAccount` consumes the pure shared cost, board-lot, T+1, ledger
event and risk contracts. Acquisition-session lots prevent selling same-session
buys; fresh fills must be chronological while old duplicates remain idempotent.
A complete next projection is validated before account state is committed.
Halt snapshots require an attributed, ordered history beginning with an engaged
halt and ending in the current state. It is an **in-memory state core** for testing, not a
durable authoritative account book. In particular it does not issue orders,
observe broker acknowledgments, model live order state, reconcile with custody,
or persist fills after process restarts. That work requires an explicit
trading-owned transactional event store and paper exchange adapter, added
behind its own rollout gate. Do not enable LIVE based on this package.

Run offline tests with:
```sh
pip install -e ./shared
PYTHONPATH=trading python -m unittest discover -s trading/tests -p 'test_*.py'
```

This changes **no** trading deployment defaults. Research and trading may
share deterministic conformance vectors but never account persistence.
