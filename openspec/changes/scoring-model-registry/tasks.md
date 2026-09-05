# Scoring Model-Version Registry Tasks

## 1. Registry model + config binding

- [x] 1.1 ScoreModelVersion document: unique model_version, per-horizon config,
  config_hash, scoring_mode, ACTIVE/RETIRED lifecycle.
- [x] 1.2 ScoringService config precedence: explicit scoring_config > ACTIVE
  registered config > built-in SCORING_CONFIG; unregistered versions unchanged.
- [x] 1.3 Registry lookup filters model_version + status='ACTIVE' at query level.
- [x] 1.4 config.model_config_hash (order-insensitive canonical sha256).

## 2. Registration CLI + validation

- [x] 2.1 jobs/model_registry_runner: register / list / retire.
- [x] 2.2 Registration-time validation: horizon keys in {5,20,60}; direction
  keys/values validated through the resolution path (bad config fails at
  register, not at scoring).

## 3. Tests + gates

- [x] 3.1 Config precedence tests (registered loaded / explicit wins /
  unregistered fallback / db-error fallback).
- [x] 3.2 ACTIVE filter at query level asserted; hash order-insensitivity.
- [x] 3.3 Registration validation tests.
- [ ] 3.4 spec-guardian / qa-reviewer on the diff.
- [ ] 3.5 branch-conflict check against develop; merge.
