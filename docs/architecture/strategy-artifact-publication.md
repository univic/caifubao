# Strategy research artifact publication (Issue #309)

This is an explicit, standalone **V2 research** publication path, built on the
strict `artifact-contract` primitives introduced by #307.

A producer takes a frozen legacy payload and an explicit field-by-field
`numeric_units` map: each numeric leaf MUST declare one of `cny_cent`,
`price_1e4_cny`, `fraction_ppm`, `basis_points`, `shares`, `sessions`
or `count`. Lossy precision is rejected instead of rounded. `inputs` MUST
identify the frozen Spec, model, market cutoffs, calendar, benchmark and policy
sources used by the actual producer. The publication job may be invoked for
`paper`, `replay` and `backtest` records; the payload is fully hashed,
not replaced with an opaque digest.

The resulting envelope carries the canonical seven-key hashed source,
`artifact_id`, `artifact_hash`, `generated_at`, `as_of`, `producer`,
`grade=RESEARCH`, `input_snapshot`, and `field_manifest`. Source revision is
read from CI-injected `CAIFUBAO_BUILD_SHA` or `GITHUB_SHA`; no unversioned
or user-asserted code version is accepted. A filesystem-backed append-only
store creates the immutable record with exclusive creation. Repeating the
same payload returns the original publication time. Tampering and partial
records fail verification.

The operator can publish an already-frozen snapshot using:

```bash
CAIFUBAO_BUILD_SHA="$(git rev-parse HEAD)" \
python -m app.jobs.strategy_artifact_publish \
  --snapshot frozen-snapshot.json \
  --output-dir /var/lib/caifubao/research-artifacts
```

This **does not** automatically change legacy JSONs or Mongo write paths and
does not grant FORWARD or TRADING_PAPER/LIVE evidence. Missing provenance,
unrepresentable units, invalid version or mutable historical numbers fail
closed. Full automatic producer-side integration, transactional Mongo
publication, independent migration tests and promotion review remain open
under #309; no existing artifact is silently relabelled compliant.
