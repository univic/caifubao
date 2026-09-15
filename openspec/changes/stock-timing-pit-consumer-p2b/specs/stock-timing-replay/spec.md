# Stock Timing Replay Adapter P1 Integrity Strengthening

## MODIFIED Requirements

### Requirement: Every ranked signal binds to a causal daily cohort

The manifest SHALL provide one immutable prediction-cohort provenance record
for every signal date used by replay. Each record SHALL include the signal
date, artifact URI, lowercase SHA-256 of the exact artifact bytes,
`prediction_root_sha256`, daily ranked-cohort fingerprint, sorted unique daily
ranked-cohort member codes, member count, and timezone-aware
`data_as_of` no later than the authoritative signal-session close. Every stored
prediction SHALL have `prediction.date` equal to that signal date and SHALL
repeat exact `freshness=FRESH`, `scoring_mode=ranked`, ranked snapshot status,
cohort fingerprint, and `data_as_of` in its input snapshot.

Every `FRESH` prediction SHALL also contain a
`stock-prediction-merkle-v1` commitment. P1 SHALL recompute the versioned
canonical leaf from the prediction's immutable generated fields after removing
the commitment object from the snapshot. It SHALL verify lowercase hashes,
zero-based daily-ranked-cohort stock-code-order index,
`leaf_count == prediction_cohorts[D].member_count == len(member_codes)`, exact
proof length and side sequence derived from the daily member-code index/current width, odd-width
self-duplication, leaf hash, and root equal to the manifest's
`prediction_root_sha256`. Top-level verification status, verification outcome,
database reference, and persistence timestamps SHALL remain outside the leaf
so later outcome verification does not invalidate a genuine original signal.
P1 SHALL recompute the daily cohort fingerprint from `member_codes` and SHALL
derive the proof index from that list. The replay/evaluation cohort is
independently frozen and MAY be a selected subset of the daily ranked cohort;
P1 MUST NOT require equal sizes or derive the proof index from the selected
replay cohort.

Missing or mismatched daily provenance, an unknown/malformed/invalid
commitment, a future cutoff, a mutated immutable prediction field, or a
prediction derived from a current-active-only cohort SHALL be unusable and
SHALL create no signal. Canonical SHA-256 hashes of the complete daily
prediction-cohort map and the authoritative trading calendar SHALL be included
in the fixed P0 configuration and therefore in P0's report identity.

#### Scenario: Ranked prediction matches its frozen daily universe and root

- GIVEN a D prediction whose input snapshot matches D's manifest cohort and
  cutoff and whose immutable leaf has a valid proof to D's prediction root
- WHEN replay evaluates D
- THEN D may form a timing intent for a later session open

#### Scenario: Selected replay pool uses a larger daily ranking cohort

- GIVEN the replay cohort contains one selected stock and D's ranked artifact
  contains that stock plus additional point-in-time ranking members
- WHEN the selected stock's daily member index and committed leaf prove
  membership in D's root
- THEN P1 accepts the signal without equating the two cohort sizes

#### Scenario: Mutable future verification does not alter the signal proof

- GIVEN a valid generated prediction is later updated only in top-level
  verification status, verification outcome, or persistence timestamps
- WHEN replay verifies its original leaf proof
- THEN the proof remains valid

#### Scenario: Legacy, stale, mutated, or malformed prediction fails closed

- GIVEN a prediction lacks exact freshness/ranked provenance, lacks the
  versioned commitment, changes a leaf field, has a mismatched root/count/path,
  or has `data_as_of` after D close
- WHEN replay evaluates D
- THEN it creates no timing signal and the missing/mismatched integrity reason
  is retained in replay diagnostics
