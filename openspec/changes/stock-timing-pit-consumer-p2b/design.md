# Design: Stock Timing PIT Artifact Consumer P2b

## Trust boundary

P2b accepts exactly one `stock-pit-universe-v1` artifact and one
`stock-ranked-inputs-v1` artifact. It uses the P2a shared-contract validator to
recompute the closed artifact hash/id, every declared source hash, normalized
field coverage, numeric encoding, and temporal bounds. It additionally
requires equal session D, exact universe artifact id/hash binding, identical
universe-artifact `as_of`, byte-equivalent calendar rows and equal calendar
source hashes, a capture-time ranked model pin, requested horizons/config hash
matching the input artifact, and D-close `as_of`/`data_as_of`. Current ACTIVE
registry state is deliberately not part of artifact-only validation; it is
checked only by the explicit apply preflight.

The sole authoritative member set is the sorted
`universe.payload.sources.universe` list. The input artifact has no second
member list and binds that set through the universe id/hash/as-of. Every
member-scoped quote, fallback quote, factor, signal, and industry
classification row must name a frozen member; CSI300 `index_quotes` are the
explicit non-member exception. Sparse input coverage never determines or
shrinks the cohort.

The P2a input artifact hash proves the frozen-universe identity and complete
scoring read set, and remains separately stored as `input_artifact_hash`. It
does not by itself authenticate the derived score. P2b therefore builds one
Merkle tree per horizon over every frozen member's generated prediction. The
commitment protocol is `stock-prediction-merkle-v1`; unknown versions fail
closed.

For a stock-code-sorted horizon cohort, each leaf object is exactly:

```json
{"schema_version":"stock-prediction-merkle-leaf-v1","prediction":{"base_price":"<normalized>","date":"YYYY-MM-DD","explanation":"<normalized object>","horizon":"<integer>","input_snapshot":"<normalized snapshot without prediction_commitment>","model_version":"<text>","percentile":"<normalized or null>","rank":"<integer or null>","recommendation":"<text>","score":"<normalized>","stock_code":"<text>","stock_name":"<text or null>","target_date":"YYYY-MM-DD"}}
```

`<normalized>` uses the P2a normalizer recursively: every finite float becomes
round-half-even fixed `{"scaled_1e8": integer}`, integers/strings/null remain
their JSON types, booleans remain booleans, and unrepresentable precision or a
non-finite value fails. Canonical bytes are UTF-8 JSON with sorted keys, compact
separators, and unescaped Unicode. Top-level mutable `status`, `verification`,
`generated_at`, `updated_at`, and the database `stock` reference are not leaf
fields, so later outcome verification does not invalidate the original signal.

The leaf digest is
`SHA256(UTF8("caifubao:stock-prediction-merkle-leaf-v1\\0") || canonical_leaf)`.
The parent digest is
`SHA256(UTF8("caifubao:stock-prediction-merkle-node-v1\\0") || left_digest_bytes || right_digest_bytes)`.
In both formulas `\\0` is exactly one NUL byte (`0x00`), not the two printable
characters backslash and zero; digest bytes are the 32 raw bytes decoded from
lowercase hexadecimal.
Each odd-width level duplicates its final digest as both left and right input.
Proof elements are exactly `{"side":"left|right","sha256":"<lowercase hex>"}`
from leaf level upward. The verifier derives the required side at each level
from zero-based `leaf_index` and current width; an odd final node requires the
duplicated self hash on the right. Width advances as `(width + 1) // 2` until
one, fixing proof length. Each prediction stores one `prediction_commitment`
object containing exactly `schema_version`, `leaf_sha256`, `leaf_index`,
`leaf_count`, `proof`, and `root_sha256`.

For every horizon, `leaf_count == manifest.member_count == frozen universe
size`; index/code order and the complete horizon member set must reconcile. P1
recomputes the leaf and verifies the proof/count/root before accepting a signal.
The serialized P2b result records each horizon root but cannot contain its own
file hash. After exclusive canonical serialization, the CLI computes the exact
file-byte SHA-256 and emits the handoff record: `artifact_uri` points to that
file, `artifact_sha256` is its byte hash, and the separate
`prediction_root_sha256` is the verified horizon root. This avoids
self-reference and preserves the existing artifact URI/content-hash meaning.
Result serialization is sorted-key compact UTF-8 JSON with unescaped Unicode
followed by exactly one LF byte; `artifact_sha256` covers those exact bytes.

## Artifact-only scoring

The adapter decodes the fixed-unit P2a values and reconstructs an in-memory
prefetch interface for D quote/factor/signal rows, decay signals, bounded member
history and fallback rows, CSI300 rows, frozen industry classifications,
industry metrics, and the captured calendar. It invokes the existing scoring
components and ranked normalization semantics against that interface. The
calculation phase MUST NOT initialize or query MongoDB and MUST NOT discover a
current universe or model.

All frozen members remain in the cohort. A missing D quote yields a `BLOCKED`
prediction and does not remove the member; computable members are ranked by
descending final score with stock code as deterministic tie-breaker, and their
percentile/recommendation uses the existing ranked thresholds. Target dates
come only from the captured calendar. No value after D is available to this
phase.

Every prediction snapshot repeats `status`, `scoring_mode=ranked`, `freshness`,
`cohort_fingerprint`, `prediction_commitment`, `data_as_of`, universe/input
artifact ids and hashes, model/config pins, input producer build revision, and
consumer build revision. A computable row has top-level
`prediction.status=PENDING` and `input_snapshot.status=RANKED`; a missing-quote
row has top-level `prediction.status=BLOCKED`. `FRESH` means only that this prediction was recomputed
from a fully validated forward P2 artifact; it does not mean VERIFIED outcome
or profitable strategy evidence. `BLOCKED` rows retain the evidence binding but
remain unusable to P1.

## Result and publish sequence

The command calculates and validates the complete in-memory result first. It
creates a caller-selected JSON report exclusively; the report records the
deterministic prediction rows, summary, source artifact identities, consumer
revision, and per-horizon prediction roots. The deterministic report contains
no wall-clock publication field; Mongo insert timestamps are persistence
metadata and do not alter scoring values.

Dry-run is the default and never connects to MongoDB. With explicit `--apply`,
the command then initializes MongoDB and performs two read-only preflights:

1. the capture-time pinned model version still exists, is ACTIVE, resolves to ranked mode,
   and has the exact config hash; and
2. no `StockScorePrediction` exists for any generated
   `(stock_code, date, horizon, model_version)` key.

Any mismatch, duplicate, partial artifact, result-validation error, existing
output path, registry drift, or prediction collision fails before prediction
writes. Publishing is insert-only in one bulk operation; P2b never replaces or
upgrades legacy/live rows. This guarantees zero prediction writes for every
validation or preflight failure. A database failure during the bulk operation
is reported rather than claimed to be transactionally atomic.

## P1 contract strengthening and non-goals

The current scheduled scorer, ordinary backfill, and prediction verification
are unchanged. P1 is intentionally strengthened: each daily record requires
`prediction_root_sha256`, and each `FRESH` prediction requires a valid
`stock-prediction-merkle-v1` proof. Legacy/unproven rows fail closed. A caller
copies the post-serialization handoff record into a P1 manifest; P1 checks every
usable prediction against it. P2b does not backdate P2a evidence, capture inputs, schedule an
operator, verify future outcomes, select a profitable strategy, or place an
order.
