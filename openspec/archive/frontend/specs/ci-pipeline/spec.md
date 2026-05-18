> **⚠️ ARCHIVED — HISTORICAL REFERENCE ONLY**
> This file describes an abandoned architecture (Django, Celery, AkQuant).
> The current system uses Flask + datahub + Vue 3. Do NOT use as a current spec.
> See `openspec/changes/mvp-quant-demo/` for the active specification.

# CI Pipeline Specification

## Overview

This specification defines the requirements for the automated testing and continuous integration pipeline for the caifubao project.

## Requirements

### Requirement: Backend tests must pass

- **WHEN** PR is created or updated against develop/master
- **THEN** All pytest tests must pass
- **AND** Coverage must be above 70%

### Requirement: Frontend tests must pass

- **WHEN** PR is created or updated against develop/master
- **AND** All Vitest tests must pass
- **AND** Frontend build must succeed

### Requirement: Code quality checks must pass

- **WHEN** PR is created or updated
- **THEN** Python lint (Ruff) must pass
- **AND** JavaScript/TypeScript lint (ESLint) must pass

### Requirement: Build must succeed

- **WHEN** PR is created or updated
- **THEN** Frontend build must succeed
- **AND** Backend Docker build must succeed
- **AND** Datahub Docker build must succeed

### Requirement: Branch protection

- **WHEN** Merging to develop or master
- **THEN** All CI checks must pass
- **AND** At least 1 reviewer approval required
