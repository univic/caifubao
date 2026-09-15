# Stock Timing PIT Artifact Consumer

## ADDED Requirements

### Requirement: P2b MUST fail closed on artifact-pair provenance

The consumer MUST fully revalidate one P2a universe artifact and one P2a input
artifact before scoring. It MUST require the expected schemas/producers/grade,
valid artifact and source hashes, complete field manifests, fixed-unit numeric
encoding, equal session D, exact input-to-universe id/hash binding, identical
universe-artifact `as_of`, byte-equivalent calendar rows and equal calendar
source hashes, D-close input cutoff, and exact capture-time ranked
model/config/horizon pins. A missing, partial, duplicate, tampered, cross-date,
cross-universe, future-dated, or inconsistent artifact MUST fail before MongoDB
initialization, output creation, or prediction writes.

The sorted `universe.payload.sources.universe` list MUST be the only
authoritative member set. Member-scoped rows in the universe/input pair MUST
refer only to those codes; CSI300 index quote rows are the explicit exception.
Input source coverage MUST NOT infer, replace, or shrink cohort membership.
Current registry ACTIVE state MUST NOT be queried during artifact-only
validation and MUST be checked only by apply preflight.

#### Scenario: Cross-universe substitution is rejected

- GIVEN two individually valid artifacts whose universe identities do not match
- WHEN P2b validates the pair
- THEN it fails before scoring or database access
- AND it creates no result and writes no prediction

#### Scenario: Tampered source evidence is rejected

- GIVEN a P2a input payload or source hash has changed after publication
- WHEN P2b recomputes the artifact contract
- THEN the former artifact identity is rejected
- AND `FRESH` is never emitted

### Requirement: Ranked calculation MUST use only frozen artifact data

The scoring phase MUST reconstruct D quote/factor/signals, decay rows, member
history and fallback rows, CSI300 rows, frozen industry classification/metrics,
calendar, universe, and config exclusively from the validated artifacts. It
MUST NOT initialize or query MongoDB, rediscover current membership, or read a
quote, factor, signal, industry, calendar, model, prediction, or stock-master
collection. It MUST apply the existing scoring components, directions,
penalties, cohort rank normalization, thresholds, target-session logic, and
deterministic stock-code tie-breaker. Missing D quotes MUST yield explicit
`BLOCKED` members rather than shrink the cohort.

#### Scenario: Mutable Mongo data cannot change a dry-run

- GIVEN a valid artifact pair and any different current database state
- WHEN the consumer calculates without apply
- THEN no database connection or query occurs
- AND the same artifact pair and consumer revision yield identical scoring rows

#### Scenario: Suspended member remains blocked in the cohort

- GIVEN a frozen member has no D quote in the complete input artifact
- WHEN ranked scores are calculated
- THEN that member is emitted as `BLOCKED`
- AND the cohort fingerprint and member count still include it

### Requirement: Usable predictions MUST carry P1-compatible FRESH provenance

Every computable ranked prediction MUST have top-level `status=PENDING` and
MUST store `freshness=FRESH`, `status=RANKED` inside `input_snapshot`, the
exact `scoring_mode=ranked`, frozen cohort fingerprint, P2a input artifact hash
as `input_artifact_hash`, D close as timezone-aware `data_as_of`, and exact
universe/input artifact ids and hashes, model/config pins, input producer build
revision, and consumer build revision.

For each horizon, P2b MUST apply the exact versioned
`stock-prediction-merkle-v1` byte protocol defined in the design to every
frozen-member prediction in stock-code order. Each row MUST store the exact
six-field `prediction_commitment` object containing the leaf hash, zero-based
index, complete member/leaf count, ordered sibling proof, and root. Unknown
versions, non-canonical normalized values, invalid lowercase SHA-256, wrong
side/index/width, wrong odd-node duplicate, wrong proof length, or unequal
`leaf_count`, manifest member count, and frozen universe size MUST fail closed.
The emitted P1 daily manifest record for that horizon MUST point to the P2b
result artifact URI and repeat the result file's byte SHA-256 as
`artifact_sha256`, the distinct Merkle root as `prediction_root_sha256`, the
fingerprint, complete member count, and D close. P1 MUST recompute the immutable
row leaf and verify its index/side/count proof to `prediction_root_sha256`.
The handoff MUST also carry the complete sorted member-code list so P1 can
reconcile the daily fingerprint/count and resolve the committed leaf index
before accepting the signal. Mutable future verification
status/outcome/timestamps MUST NOT enter the leaf. P1 MUST accept the row only
when its own frozen cohort/model checks, Merkle proof, and daily record all
match. `FRESH` MUST NOT imply a
VERIFIED future outcome, profitability, or live-trading suitability.

A member missing its D quote MUST instead have top-level `status=BLOCKED`;
its snapshot MUST retain the same artifact/cohort evidence binding but it MUST
remain unusable to P1.

#### Scenario: P1 accepts exact provenance

- GIVEN P2b predictions and a P1 manifest contain the same frozen model,
  cohort, artifact hash, member count, and D close
- WHEN P1 replays D
- THEN an otherwise usable prediction is accepted as `FRESH`

#### Scenario: One provenance field differs

- GIVEN the manifest artifact hash, cohort fingerprint, or data-as-of differs
  from the prediction snapshot
- WHEN P1 replays D
- THEN the prediction is rejected rather than silently downgraded or upgraded

#### Scenario: Stored score is changed after generation

- GIVEN a prediction score, rank, percentile, base price, target date,
  recommendation, explanation, model pin, or input provenance is changed
- WHEN P1 recomputes its Merkle leaf and proof
- THEN the row fails integrity validation against the manifest root
- AND it creates no timing signal

### Requirement: Publication MUST be explicit, collision-safe, and insert-only

Dry-run MUST be the default and MUST perform no MongoDB access or prediction
mutation. Apply MUST occur only after the entire artifact pair, calculated
prediction set, result report, and output path validate in memory. Before any
prediction write, apply MUST confirm that the pinned registry version is still
ACTIVE ranked with the exact config hash and that no generated natural key
already exists. Any validation, registry, output, or collision failure MUST
cause zero prediction writes. Publication MUST insert in one bulk operation and
MUST NOT replace or upgrade an existing prediction. Database failure during
that operation MUST be reported without claiming transaction atomicity.

#### Scenario: Existing live prediction blocks publication

- GIVEN any generated `(stock_code, date, horizon, model_version)` key already
  exists
- WHEN apply is requested
- THEN P2b fails the complete preflight
- AND it inserts, replaces, or updates zero predictions

#### Scenario: Dry-run emits replay handoff without database access

- GIVEN a valid artifact pair and a new output path
- WHEN the operator runs P2b without apply
- THEN the result is created exclusively with deterministic predictions and the
  exact P1 daily manifest record
- AND no MongoDB initialization, query, or write occurs
