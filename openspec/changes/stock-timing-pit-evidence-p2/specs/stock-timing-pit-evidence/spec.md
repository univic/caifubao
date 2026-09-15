# Stock Timing PIT Input Evidence

## ADDED Requirements

### Requirement: PIT input capture MUST be forward-only

The system MUST capture D's universe at or after the previous authoritative
session's Asia/Shanghai close and strictly before D opens. It MUST capture D's
scoring inputs only from that exact universe artifact, at or after D closes and
strictly before the next authoritative session opens. The previous, D, and next
sessions MUST exist in the stored China A-share trading calendar. A stale,
future, non-session, or calendar-edge date MUST fail before source data is read
or an output path is created.

#### Scenario: Two causal capture windows are accepted

- GIVEN D is a market session and its universe was frozen after the previous
  close but before D opened
- WHEN the operator captures D's inputs after D closes and before the next open
- THEN the read set uses exactly the pre-open universe and is bounded at D close

#### Scenario: Historical relabelling is rejected

- GIVEN either D has opened without a universe capture or the next session has
  opened without an input capture
- WHEN the operator attempts that missed phase
- THEN the phase fails before any source query
- AND no historical database state is relabelled as forward PIT evidence

### Requirement: The artifact MUST close the complete ranked-scoring read set

The system MUST freeze the selected ACTIVE ranked model version and config,
requested horizons, build-injected code revision, authoritative calendar,
pre-open captured member codes, names, and industry classifications,
evaluation-day quotes/factors/signals, signal-decay rows, bounded member quote
history, bounded CSI300 rows, and latest pre-D industry metrics used by those
horizons. Every database query MUST be bounded at D or earlier and sorted by a
stable business key. The system MUST fail on missing model pins/build revision,
an empty or duplicate universe, duplicate source keys, a read failure, or a
source business date after D. Missing D quote rows MUST NOT remove a member.

#### Scenario: Suspended name remains explicit

- GIVEN an active member has no quote on D because it is suspended
- WHEN D's read set is captured
- THEN the member remains in the frozen universe
- AND its absent quote is visible rather than filtered from membership

#### Scenario: Future or duplicate source row is rejected

- GIVEN a captured source row is dated after D or repeats its collection's
  business key
- WHEN the artifact is built
- THEN publication fails before any output is written

### Requirement: Published input evidence MUST conform to the shared artifact contract

The system MUST publish `schema_version`, `artifact_id`, `artifact_hash`,
`as_of`, `generated_at`, `producer`, `grade`, `input_snapshot`,
`field_manifest`, and `payload`. `grade=RESEARCH` and each artifact's own
contractual `as_of` MUST be in the hashed content; `generated_at` MUST record
publication time outside the hash. `input_snapshot` MUST identify the build revision, config, universe,
calendar, and every source collection read set with a content hash and
time-bounded source `as_of`. The payload MUST contain the normalized source rows
so every source hash is independently recomputable. Missing or partial input
identity MUST fail closed.

The universe artifact's `as_of` and `data_as_of` MUST equal its actual pre-open
capture instant; the input artifact's values MUST equal D close. Canonical
serialization MUST use sorted-key compact UTF-8 JSON with unescaped
Unicode and UTC `Z` instants. The hashed input MUST contain no JSON floating-
point value: integral sources remain integers and floating sources are
decimal-quantized with round-half-even to integers in a fixed declared
`1e-8 source unit`. `field_manifest` MUST classify every emitted payload path,
declare every numeric unit, and name the producing build revision and exact
input dependencies for each payload field. `artifact_hash` and `artifact_id`
MUST use the shared contract's exact closed hash and identity formulas.

#### Scenario: Input mutation changes identity or fails validation

- GIVEN a published artifact whose payload source row is altered
- WHEN its source hashes and artifact hash are recomputed
- THEN the mutation is detected
- AND the artifact is not accepted under its former identity

#### Scenario: Publication time does not enter content identity

- GIVEN identical normalized inputs and as-of with two publication timestamps
- WHEN artifact identity is computed
- THEN both have the same artifact hash and artifact id
- AND each still exposes its actual generated timestamp

### Requirement: P2a capture MUST NOT mutate scoring state

Each command MUST only read MongoDB and create one caller-selected JSON artifact
using exclusive creation. It MUST NOT overwrite an existing path, generate or
update `StockScorePrediction`, change a model/cohort record, or make the
artifact consumable by P1 without a follow-on consumer that revalidates it.
Existing scheduled scoring and backfill behavior MUST remain unchanged.

#### Scenario: Capture leaves predictions unchanged

- GIVEN valid pre-open or post-close inputs and a new output path
- WHEN that capture phase completes
- THEN exactly one artifact file is created for the phase
- AND no prediction/model/cohort database write occurs

#### Scenario: Existing output is protected

- GIVEN the selected output path already exists
- WHEN capture is requested
- THEN the command fails without replacing or truncating the file
