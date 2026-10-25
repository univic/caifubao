## Why

Research paper UI (#322) exposes daily target plans, but target intents are
not simulated trades. Existing immutable ResearchSimulationArtifact derivations
contain FILL/REJECTED/MARK/dividend events. Without verifying each artifact
hash and its exact execution-date/decision-date mapping, a UI could attach
someone else's event to a FORWARD plan. See #325.

## What Changes

Add a pure verifier and a read-only decision-detail trace against the exact
strategy name, model cohort hash kind, execution interval and content-addressed
artifact. Reject ambiguous derivations, wrong strategy hash, invalid event
identities or cross-domain execution. Show confirmed Research simulated FILL
and REJECTED events and dated session ledger activity, never broker executions.

## Capabilities

### New Capabilities

- research-paper-artifact-lineage: verified, bounded Research simulated event
  attribution to saved Research strategy plan decisions.

## Impact

Backend read API and validation helper, frontend Research page, unit and
authenticated-route tests. No writer, promotion, Research deploy or Trading
order path.

## Non-goals

Artifacts remain provisional NAV, not certified returns. Missing or undated
legacy corporate events stay unlinked. No execution event retroactively
turns a historical REPLAY into a FORWARD observation.
