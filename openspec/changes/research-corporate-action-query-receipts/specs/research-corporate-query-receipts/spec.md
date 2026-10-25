## ADDED Requirements

### Requirement: Offline corporate-action replay publication SHALL have a query success receipt for every requested code

The retrospective research collector SHALL call the vendor for every
requested instrument, write a success receipt even for a confirmed empty
response, and SHALL refuse to publish an apparently complete event inventory
if any code fails, returns null, produces invalid data or has conflicting
same-day event rows. Content and per-code receipts MUST be independently
fingerprinted.

#### Scenario: A required stock's dividend query times out

- **GIVEN** a two-stock Research universe where the second provider call fails
- **WHEN** the collector attempts the offline archive
- **THEN** no complete corporate-action manifest or event list is published

#### Scenario: A stock returns a confirmed empty response

- **GIVEN** a successful provider response with zero rows for a stock
- **WHEN** the replay archive is produced
- **THEN** that stock appears in covered_codes and its receipt reports zero
  included events, without implying historical PIT completeness

### Requirement: Retrospective action inventories SHALL never claim FORWARD input provenance

The publisher SHALL label its resulting source inventory REPLAY, never
FORWARD_INPUT, and SHALL fail on noncausal record/ex/pay dates, unsupported
missing cash/ratio fields, duplicate implemented ex-dates and inconsistent
bonus/capitalization components.

#### Scenario: A later announcement is discovered in a historical source response

- **GIVEN** a vendor row announced after the requested historical as_of date
- **WHEN** it is evaluated for the archive
- **THEN** the collector refuses its publication and does not label it as
  a historically available event
