# Datahub Runners

## Overview

Datahub runners are the operational entry points used to backfill or refresh quote and factor data for the MVP.

## Rules

- Quote runner updates index and stock quotes.
- Factor runner updates FQ and MA factors from the latest available quote data.
- Runner execution must copy the live deployment runtime shape when launched manually.
- Runner commands should support dry-run mode for operational verification.
- Scheduled quote updates should run as observable Kubernetes Jobs/CronJobs rather than relying only on an in-process scheduler.
- Quote job runs should record status, timing, phase stats, and error details for downstream freshness diagnosis.

## Responsibilities

- `datahub` produces and stores market data.
- `backend` does not run data refresh jobs.
- `frontend` does not invoke Mongo directly.

## Acceptance Criteria

- Manual execution can reuse the live `caifubao-datahub` deployment config.
- Freshness metadata is updated after successful quote/factor refresh.
- Missed quote updates can be detected and compensated before downstream analysis proceeds.
- The operational workflow is documented in OpenSpec.
