# Data Quality

## Overview

The data quality experience verifies whether the market data pipeline is healthy enough for the MVP demo.

## Rules

- Summary covers supported active A-share stocks only.
- BSE stocks and unsupported symbols are excluded from the denominator.
- Quote freshness is required for a stock to be considered healthy.
- FQ and MA freshness are evaluated against the latest quote date.
- If quote data lags the expected latest trading day, FQ and MA should be treated as blocked by quote freshness rather than independent factor failures.
- MA windows that are not yet applicable to a newly listed stock must not be treated as abnormal.

## Outcomes

- Show overall status, latest quote date, generated time, and coverage cards.
- Show a freshness detail table with quote, FQ, and MA freshness.
- Distinguish `missing`, `stale`, `ahead`, and `not applicable` cases in the UI.
- Distinguish factor states that are blocked by stale quote data so downstream systems can pause analysis for the correct reason.

## Backend Boundary

- `backend` reads Mongo and computes the summary.
- `datahub` owns the quote/factor/freshness records.
- `frontend` only renders the API response.

## Acceptance Criteria

- New listings with insufficient history do not fail MA coverage.
- BSE symbols are excluded from the data quality denominator.
- Generated time is displayed in the application timezone.
- FQ and MA coverage do not report independent abnormal coverage when quote data is stale.
