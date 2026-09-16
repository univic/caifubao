# Immutable Artifact Contract

## ADDED Requirements

### Requirement: Every immutable artifact MUST carry the shared identity fields

An artifact published as immutable evidence MUST carry `schema_version`,
`artifact_id`, `artifact_hash`, `as_of`, `generated_at`, `producer`, `grade`,
`input_snapshot`, `field_manifest`, and `payload`. Those names and their
semantics MUST be identical across producers in `datahub` and `backend`, and a
producer MUST NOT rename or reinterpret a field defined here; the only tolerated
second name is the pre-existing freshness field `data_as_of`, which this
contract does not rename and which MUST equal `as_of` where it is exposed.

#### Scenario: A conforming artifact carries all identity fields

- GIVEN an artifact published as immutable evidence
- WHEN a consumer inspects it
- THEN `schema_version`, `artifact_id`, `artifact_hash`, `as_of`, `generated_at`,
  `producer`, `grade`, `input_snapshot`, `field_manifest`, and `payload` are all
  present
- AND the field names match the shared contract exactly

#### Scenario: Two producers do not fork the contract

- GIVEN one artifact produced by `datahub` and one by `backend`
- WHEN their field names and semantics are compared
- THEN they are the same contract
- AND neither renames nor reinterprets a shared field

#### Scenario: The legacy freshness name is grandfathered, not duplicated

- GIVEN an artifact whose producer already exposes `data_as_of`
- WHEN the artifact is published
- THEN `data_as_of` is allowed to coexist with `as_of`
- AND it carries the same instant, so no third cut-off name is introduced

### Requirement: The hashed input set MUST be closed and canonically serialized

`artifact_hash` MUST be the lowercase hexadecimal SHA-256 of one canonical byte
string: a **single JSON object** keyed by exactly `schema_version`, `producer`,
`as_of`, `grade`, `input_snapshot`, `field_manifest`, and `payload` — nothing else
contributes. Those seven keys MUST be top-level in that object and MUST NOT be
wrapped under another key, though the values of `input_snapshot`, `field_manifest`,
and `payload` are themselves structured.
Canonical serialization MUST be UTF-8 JSON with object keys sorted
lexicographically, no insignificant whitespace, and non-ASCII characters left
unescaped. Instant-valued fields (`as_of`, `generated_at`, and every instant
inside `input_snapshot`) MUST be ISO-8601 in UTC with a `Z` suffix and at least
second precision, and MUST NOT carry a numeric offset, so one instant has
exactly one lexical form.

#### Scenario: The hash input set is closed

- GIVEN a conforming artifact
- WHEN its `artifact_hash` is recomputed
- THEN the input set is exactly schema version, producer, `as_of`, `grade`, input
  snapshot, field manifest, and payload
- AND `generated_at` and any declared lifecycle field contribute nothing to it

#### Scenario: Instants have one lexical form

- GIVEN the same instant expressed with different offsets or precisions
- WHEN each is serialized for hashing
- THEN both serialize to the same UTC `Z` form
- AND a value carrying a numeric offset is normalized or rejected, never hashed
  as-is

### Requirement: Every hashed number MUST have exactly one representation

Every numeric value inside the hashed input set MUST be serialized as a JSON
integer. A quantity that is not integral MUST be scaled to an integer by a unit
the field manifest fixes for that field (for example cents for money or basis
points for rates), and that unit MUST be recorded. JSON floating-point numbers
and decimal strings MUST NOT appear anywhere in the hashed input set.

#### Scenario: One logical value has one encoding

- GIVEN the same logical quantity produced by two independent implementations of
  the same producer
- WHEN each serializes it for hashing
- THEN both emit the identical JSON integer under the manifest-declared unit
- AND neither can emit a differently scaled integer or a decimal string

#### Scenario: Non-integral quantities are scaled, not floated

- GIVEN a quantity such as a price or a weight that is not integral
- WHEN it enters the hashed input set
- THEN it is scaled to an integer by its declared unit
- AND a floating-point value is rejected rather than hashed

### Requirement: The field manifest MUST be hashed and MUST classify every field

`field_manifest` MUST declare, for every field a producer emits, its kind —
asserted result (`payload`), narrative, or mutable lifecycle state — a `derived`
attribute marking it as a derived annotation (the kind remains one of those
three), and, for **every numeric field in the hashed input set**, its unit. It
MUST also record the artifact type's asserted field set, and per payload field
the producing code version and the input names that field derives from, so a
consumer can audit a value from the artifact alone. The code version recorded for
a payload field MUST equal the producing code version recorded in
`input_snapshot`. `field_manifest` MUST be inside the hashed input set. Every
field in the declared asserted field set MUST be declared `payload`;
classifying such a field as lifecycle or narrative in order to keep it out of the
hash is not conforming.

#### Scenario: The manifest is part of the hashed set

- GIVEN a conforming artifact
- WHEN its `field_manifest` is altered without re-publication
- THEN the recomputed hash does not match the recorded `artifact_hash`
- AND the alteration is detectable

#### Scenario: Asserted results cannot be hidden from the hash

- GIVEN a producer that emits a numeric result
- WHEN it builds the artifact
- THEN that field is declared `payload` and contributes to the hash
- AND declaring it lifecycle or narrative while still asserting it as a result is
  rejected

#### Scenario: A numeric field is auditable from the artifact alone

- GIVEN a numeric field in the payload
- WHEN a consumer audits it
- THEN the manifest names the code version that produced it
- AND the manifest names the inputs it derives from

### Requirement: `grade` MUST be part of the hashed payload

The research/observation `grade` MUST be included in the hashed input set. A
change of `grade` MUST therefore change `artifact_hash` and MUST be published as
a new artifact; it MUST NOT be edited in place.

#### Scenario: A grade change is detectable

- GIVEN a published artifact whose `grade` is altered without re-publication
- WHEN a consumer recomputes the hash
- THEN the recomputed hash does not match the recorded `artifact_hash`
- AND the change is treated as tampering rather than as an update

#### Scenario: Re-grading publishes a new artifact

- GIVEN an artifact that is re-graded
- WHEN the new artifact is produced
- THEN it carries a different `artifact_hash` and `artifact_id`
- AND the previously published artifact is left unchanged

### Requirement: Artifact identity MUST be deterministic and idempotent

`artifact_id` MUST be the lowercase hexadecimal SHA-256 of `producer`,
the canonical `as_of`, and `artifact_hash` joined by newline characters, prefixed
with `sha256:` — a single pinned form, so two producers cannot derive different
ids for the same artifact. Reproducing byte-identical hashed content MUST yield
the same `artifact_id` and MUST NOT append a second record; identity comes from
content, not from publication order. Any change to the hashed input set MUST
yield a new `artifact_id` and MUST be appended.

#### Scenario: The same inputs yield the same hash and id

- GIVEN the same producer, `as_of`, input snapshot, manifest, `grade`, and payload
- WHEN the artifact is produced twice
- THEN both runs produce the same `artifact_hash`
- AND both runs produce the same `artifact_id`

#### Scenario: Production time does not change identity

- GIVEN two runs of the same inputs at different times
- WHEN their hashes and ids are compared
- THEN the hashes and ids are identical
- AND `generated_at` remains the publication instant of the existing record and
  is not rewritten by the later run
- AND the later run's own timestamp is not written into the artifact

#### Scenario: Identical re-publication is idempotent, not a duplicate

- GIVEN an artifact that is produced again unchanged
- WHEN it is published
- THEN the existing artifact is recognised by its `artifact_id`
- AND no second record is appended for it

### Requirement: `as_of` MUST be distinct from `generated_at` and MUST NOT fork the freshness name

`as_of` MUST record the data cut-off the artifact reflects, and `generated_at`
MUST record when the artifact was first published; one MUST NOT be substituted
for the other and wall-clock time MUST NOT be used as `as_of`. Where a producer
already exposes the freshness field `data_as_of`, its value MUST equal the
artifact's `as_of`; a producer MUST NOT publish two conflicting cut-off values.

#### Scenario: A backfill keeps its data cut-off

- GIVEN an artifact produced today for a data cut-off in the past
- WHEN it is inspected
- THEN `as_of` names the past cut-off
- AND `generated_at` names today

#### Scenario: The freshness field and as_of agree

- GIVEN a producer whose response also exposes `data_as_of`
- WHEN the artifact is published
- THEN `as_of` and `data_as_of` carry the same instant
- AND neither is silently derived from the other's absence

#### Scenario: A stale cut-off is visible rather than hidden

- GIVEN an artifact whose `as_of` lags the latest available data
- WHEN a consumer evaluates freshness
- THEN the lag is derivable from the artifact's own fields
- AND the producer does not silently present it as current

### Requirement: `input_snapshot` MUST identify the artifact's provenance

`input_snapshot` MUST identify every input that determined the payload by name,
with that input's version or content hash and, where the input is time-bounded,
its own `as_of`. It MUST also record the producing code version, defined as an
immutable identifier injected by the build or CI — the source revision or package
version resolved at build time — not a hand-maintained constant. This requirement
identifies provenance; it is not a claim of bit-identical replay, and such a claim
MUST NOT be made for an input the snapshot cannot identify. An artifact whose
inputs cannot be resolved MUST fail closed rather than be published with an empty
or partial snapshot.

#### Scenario: Inputs are individually identified

- GIVEN an artifact derived from several inputs
- WHEN its `input_snapshot` is inspected
- THEN each input is named
- AND each carries a version or content hash
- AND each time-bounded input carries its own `as_of`
- AND the producing code version is recorded

#### Scenario: Timeless inputs are not forced to invent a cut-off

- GIVEN an input with no data cut-off, such as a configuration or a code revision
- WHEN it is recorded
- THEN it carries its version or content hash and no fabricated `as_of`
- AND the snapshot still identifies it unambiguously

#### Scenario: The code version is build-injected, not asserted

- GIVEN a producer publishing from a built image
- WHEN its code version is recorded
- THEN the value comes from the build or CI, such as the source revision
- AND a hand-maintained constant is not accepted as the code version

#### Scenario: Unresolvable provenance fails closed

- GIVEN a producer that cannot resolve the identity or version of one of its inputs
- WHEN it is asked to publish an immutable artifact
- THEN the publication fails closed with an explicit error
- AND no artifact is emitted with an empty or partial input snapshot

### Requirement: Hashed content MUST be immutable and mutable lifecycle fields MUST be declared

All hashed content MUST be immutable once published: superseding it MUST create a
new artifact. A derived annotation MUST be **pre-declared** in the
publication-time manifest — `kind` narrative with the `derived` attribute set —
and only its value may be populated later, mirroring the lifecycle pattern, so
that adding the annotation never changes the hashed manifest or
`artifact_hash`. Only fields the manifest declares as mutable lifecycle state or
as pre-declared derived annotations MAY be updated in place, and such an update
MUST NOT change `artifact_hash`.

#### Scenario: Declared lifecycle fields are excluded from the hash

- GIVEN an artifact whose manifest declares lifecycle fields — values not known at
  publication time, such as a verification status or a profit and loss realised
  later
- WHEN one of those fields is updated
- THEN `artifact_hash` is unchanged
- AND the update is visibly outside the hashed content

#### Scenario: Populating a pre-declared derived annotation does not change the hash

- GIVEN an artifact whose publication-time manifest pre-declares a derived
  annotation
- WHEN that annotation's value is populated after publication
- THEN `artifact_hash` is unchanged
- AND the annotation is still declared in the same hashed manifest
- AND no new manifest entry was added after publication

#### Scenario: A mutation inside the hashed content is detectable

- GIVEN an artifact whose hashed content was altered after publication
- WHEN a consumer recomputes the hash
- THEN the recomputed hash does not match the recorded `artifact_hash`
- AND the mismatch is treated as corruption, not as a new version

#### Scenario: Superseding appends a new artifact

- GIVEN an existing artifact and new inputs
- WHEN the producer regenerates the artifact
- THEN a new artifact with a new hash and id is created
- AND the original artifact's hashed content is unchanged

### Requirement: Artifacts MUST stay research-grade until the promotion gate passes

Every artifact MUST carry a research/observation `grade` and the
research/demo/MVP non-advice framing while the producing model version has not
passed the roadmap promotion gate. The `grade` MUST NOT be inferred from `as_of`
recency, evidence-kind proxies, session counts, or job status.

#### Scenario: An unpromoted artifact is labelled research-grade

- GIVEN an artifact from a model version that has not been promoted
- WHEN it is published
- THEN its `grade` is research/observation
- AND it is not described as tradable, actionable, or validated

#### Scenario: No proxy upgrades the grade

- GIVEN an artifact with a very recent `as_of` and a long run history
- WHEN its grade is assigned
- THEN none of those proxies change the grade
- AND only the explicit promotion record could

### Requirement: The hashed payload MUST contain only deterministic values

Every field in the hashed payload MUST be computed by deterministic backend code
from the inputs recorded in `input_snapshot`, and its manifest entry MUST name the
producing code version and those inputs. A value produced by a language model
inside a conversation MUST NOT appear in the hashed payload, and any narrative
explanation MUST be declared narrative in the manifest and live outside `payload`.

#### Scenario: Every hashed field traces to code and inputs

- GIVEN an artifact's hashed payload
- WHEN a consumer audits one of its numeric fields
- THEN the manifest attributes it to a recorded code version
- AND to inputs named in the snapshot

#### Scenario: Explanation is separable from numbers

- GIVEN an artifact with an accompanying human-readable explanation
- WHEN the artifact is hashed
- THEN the explanation is declared narrative and is not part of the hashed payload
- AND the numbers remain reproducible without it
