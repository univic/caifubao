# OpenClaw Data Access

## Overview

OpenClaw is a downstream consumer of caifubao data. The next phase should make caifubao a reliable data provider for OpenClaw investment analysis by exposing stable, documented, read-only APIs for quotes, factors, signals, and data freshness.

## Role Boundary

- `datahub` produces and refreshes quotes, factors, signals, freshness, and quality metadata.
- `backend` exposes stable API contracts and lightweight aggregation for downstream consumers.
- `frontend` remains a human-facing UI and is not the integration surface for OpenClaw.
- OpenClaw consumes caifubao APIs and performs its own investment analysis downstream.

## Required Data Domains

- Stock master data: code, name, exchange/market, active status, supported data capabilities, listing metadata when available.
- Quotes: daily OHLCV, volume, turnover-related fields when available, and adjusted prices.
- Factors: FQ factor, HFQ/QFQ prices, MA factors, and future MVP-safe derived factors.
- Signals: dated signal list, signal name/type/direction/strength, reason, factor snapshot, source freshness.
- Data quality: expected trading date, latest available quote date, freshness status, and blocked-by-quote factor state.

## API Expectations

- APIs must be read-only for OpenClaw in the MVP phase.
- Prefer a dedicated integration namespace such as `/api/integrations/openclaw/*` so service-to-service contracts do not drift with human-facing UI APIs.
- Responses must use stable field names and avoid leaking raw Mongo collection structure.
- List endpoints should support date ranges, symbol filters, pagination, and predictable ordering.
- Responses that depend on market data must include data freshness or generated time.
- Missing, stale, not applicable, and blocked-by-quote states must be explicit rather than inferred from null values.

## Operational Expectations

- Quote and factor refresh jobs must be observable through job run records.
- Downstream consumers must be able to tell whether data is current enough before analysis.
- Backfills should be triggered operationally by caifubao maintainers, not by OpenClaw directly.
- API failures should return actionable error messages and stable status codes.

## Security And Access

- OpenClaw access should use authenticated backend APIs, not direct Mongo credentials.
- OpenClaw should authenticate with a dedicated service token sent as `Authorization: Bearer <token>`.
- The token must represent a service identity, not a normal interactive user session.
- MVP should start with one read-only scope, `openclaw:data-read`.
- The scope model should leave room for narrower future scopes such as `stocks:read`, `quotes:read`, `factors:read`, `signals:read`, and `quality:read`.
- Tokens must be stored hashed, not in plaintext.
- A token record should include at least `id`, `name`, `token_hash`, `scopes`, `status`, `expires_at`, `created_at`, `last_used_at`, and `last_used_ip`.
- Revoked, expired, disabled, or scope-mismatched tokens must be rejected with stable 401 or 403 responses.
- The first integration should be read-only and should not expose admin, collection-write, backfill, data mutation, or scheduler-trigger endpoints.
- Backend should log each accepted OpenClaw request with token id, request id, endpoint, status code, remote address, and data date or data-as-of when available.
- Responses should include a request id, and data responses should include data-as-of or generated-at metadata so OpenClaw can audit analysis inputs.
- Rate limits should be applied per service token once usage volume becomes material; MVP can define the contract before enforcing strict limits.

## Authentication Non-goals

- Do not reuse normal user JWTs for OpenClaw service-to-service access.
- Do not give OpenClaw direct Mongo credentials.
- Do not require full OAuth/OIDC client-credential flow in the MVP unless a later security review explicitly asks for it.
- Do not allow OpenClaw tokens to trigger quote updates, factor recomputation, backfills, or administrative operations.

## Non-goals

- Do not embed OpenClaw investment analysis logic inside caifubao.
- Do not expose Mongo collections directly as the public contract.
- Do not introduce a new microservice solely for OpenClaw in the MVP phase.
- Do not add real-time streaming or minute-level data unless explicitly scoped later.

## Acceptance Criteria

- OpenClaw can fetch supported stocks, daily quotes, latest factors, signals, and freshness from backend APIs.
- OpenClaw can determine whether analysis should proceed or pause due to stale quote/factor data.
- API responses include enough metadata to reproduce which data date and generated time were used.
- The integration contract is documented in OpenSpec before implementation work begins.
