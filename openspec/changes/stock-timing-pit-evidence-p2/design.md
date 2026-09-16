# Design: Stock Timing PIT Input Evidence P2a

## Boundary and sequencing

This slice captures immutable inputs only. It deliberately does not add
provenance to `StockScorePrediction`: doing that before a consumer recomputes
and verifies the complete artifact would recreate the cohort-only freshness
bug rejected by the Spec Gate. P2b will consume this artifact and is the first
slice allowed to write P1-compatible prediction provenance.

Capture is forward-only and two-phase. A universe artifact for China A-share
session D is captured at or after the previous authoritative session's 15:00
close and strictly before D's 09:30 open. The input artifact then requires that
exact universe artifact and is captured at or after D's 15:00 close and strictly
before the next session's 09:30 open. The previous, D, and next sessions must all
exist in the stored market calendar. A stale or calendar-edge session therefore
cannot be relabelled PIT by running either command today.

## Closed scoring read set

The command freezes the exact bounded superset used by batch ranked scoring for
the selected model and horizons:

- sorted `active_status=0` stock code/name membership and current industry
  classification captured by the pre-open artifact and consumed without
  rediscovery after close;
- D quote and factor rows for every member;
- D signals plus the maximum configured decay window;
- historical member quotes from the scoring window start through D, plus the
  scoring path's bounded per-code fallback for sparse/suspended histories;
- CSI300 quotes extended through the earliest captured fallback range;
- the pre-open industry classification and latest pre-D industry metrics for
  each requested horizon;
- the authoritative calendar segment needed for lookbacks/target dates; and
- exact model version, resolved ACTIVE ranked mode, model configuration hash,
  requested horizons, and build-injected source revision.

All Mongo reads are bounded at D or earlier. Query results are sorted by stable
business keys before hashing and publication. The command fails on an
unregistered/non-ACTIVE/non-ranked model, an empty universe, duplicate source
keys, missing build revision, failed read, or a source row dated after D. Quote
absence does not remove a member, preserving suspension evidence.

## Shared immutable artifact contract

Both artifact types use a top-level envelope containing `schema_version`, `artifact_id`,
`artifact_hash`, `as_of`, `generated_at`, `producer`, `grade`,
`input_snapshot`, `field_manifest`, and `payload`. The universe artifact's
`as_of` and `data_as_of` equal its actual pre-open capture instant; the input
artifact's values equal D close in UTC. `generated_at` is the actual publication
instant and is excluded from the content hash. `grade=RESEARCH` is inside the
hash.

The universe artifact identifies its build revision and calendar and contains
the normalized member and classification rows. The post-close input artifact identifies the build
revision, model config, exact universe artifact id/hash, every source collection
read set, and calendar by content hash; time-bounded market sources also repeat
D close. Its payload contains the normalized source rows, allowing a consumer
to recompute every source hash rather than trusting the envelope. The field
manifest classifies every emitted field and records the integer encoding for
numeric source fields.

Canonical serialization follows the shared contract: UTF-8 JSON, sorted keys,
no insignificant whitespace, unescaped Unicode, UTC `Z` instants, and no JSON
floating-point values in the hashed input. Stored integers remain integers;
floating source values are decimal-quantized with round-half-even to integers
in a fixed `1e-8 source unit` declared for every numeric payload path. A future
consumer must use these encoded values rather than re-reading or silently using
higher-precision mutable source rows. Booleans remain booleans and are not
treated as numbers. The field manifest declares every emitted payload path,
its kind and unit where numeric, and the producing build revision plus exact
input names on which the field depends.

`artifact_hash` covers exactly the seven shared fields; `artifact_id` is the
contract-defined `sha256:` identity over producer, canonical as-of, and hash.
Each command creates its output exclusively and refuses to overwrite any path.
Repeating a capture with an injected identical clock produces the same content
hash/id; a normal later capture cannot pass the forward window for D.

## Failure and compatibility

Validation and artifact construction complete in memory before each exclusive
file create. Any failure leaves no partial output. Existing scoring commands,
Mongo rows, and scheduling remain unchanged. Neither artifact is yet accepted
by P1 and neither can upgrade legacy predictions.
