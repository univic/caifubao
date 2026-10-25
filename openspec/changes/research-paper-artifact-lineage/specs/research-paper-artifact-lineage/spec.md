## ADDED Requirements

### Requirement: Research simulated execution trace SHALL require unambiguous immutable provenance

The read-only Research paper API SHALL validate an artifact's canonical
content hash, strategy identity, hash kind, decision date, execution session,
and ledger event identities before attributing simulated fills or rejections.
The API MUST return a visible missing, ambiguous or invalid-provenance
status instead of choosing a latest derivation with incompatible evidence.

#### Scenario: Two overlapping Research simulations

- **GIVEN** two separate derivation artifacts overlap the same strategy
  and execution date
- **WHEN** a user asks for the Research plan's execution trace
- **THEN** the UI refuses to select one arbitrarily or display its fills

#### Scenario: Tampered artifact payload

- **GIVEN** an artifact event was edited after its content SHA-256 was stored
- **WHEN** the read model validates the artifact
- **THEN** no simulation fills are attributed to the plan

### Requirement: Research execution event display SHALL distinguish evidence from inference

The Research UI SHALL show only dated, hash-verified, decision-tagged simulated
fills and rejected orders for the exact signal and execution session. It MUST
keep undated corporate actions unlinked and label all results as Research
simulation rather than broker or Trading Paper execution. It SHALL NOT
promote REPLAY to FORWARD or assert that current NAV is reconciled.

#### Scenario: Old fill lacks a decision date

- **GIVEN** a Research FILL event with an execution date but no linked
  decision_date
- **WHEN** a user opens a saved plan's decision details
- **THEN** the event is not presented as a fill of that decision

#### Scenario: Valid rejected order

- **GIVEN** a dated and decision-linked Research ledger REJECTED event
- **WHEN** its verified artifact is rendered
- **THEN** the rejection reason and order identity are visible without
  creating any trading order state
