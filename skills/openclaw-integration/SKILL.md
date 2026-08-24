---
name: openclaw-integration
description: OpenClaw 集成契约——service token 鉴权、scope、只读数据 API、freshness/审计语义与硬边界
license: MIT
compatibility: opencode, dsh
metadata:
  audience: contributors
  project: caifubao
---

## What this skill covers

The integration contract between caifubao and OpenClaw, a downstream **read-only**
consumer. Caifubao provides data, contracts, freshness metadata, authentication,
and auditability; OpenClaw performs investment analysis. OpenClaw analysis logic
must NEVER enter caifubao (`RULES.md` → OpenClaw-Specific Rules).

Source of truth: `docs/integrations/openclaw.md`, `docs/operations/service-tokens.md`,
`backend/app/api/v1/integrations/openclaw/*`, `backend/app/lib/auth_decorators.py`,
`backend/app/utilities/auth_util.py`, `backend/app/model/service_token.py`.

## 1. Authentication (service tokens)

- Header: `Authorization: Bearer st_...` (plain tokens are `st_` + 32-byte urlsafe
  random, base64url-encoded to 43 chars; `generate_service_token` in `auth_util.py`).
- Managed via `backend/app/scripts/manage_service_tokens.py`:
  - `create --name <n> [--scopes openclaw:data-read] [--expires 365]` — plain token printed once, never again.
  - `list` — name / status / expires / scopes.
  - `revoke --name <n>` — sets status `revoked` immediately.
- Tokens are stored **hashed** (SHA-256, `hash_token`); only the hash lives in the
  `service_tokens` collection. Plain text is never stored or returned.
- Scopes (model-enforced choices): `openclaw:data-read` (broad) and
  `openclaw:score-read` (narrow). `data-read` satisfies every endpoint including
  score endpoints; the reverse is not true.
- Lifecycle: `status` ∈ active/revoked/expired; `expires_at` (create with
  `--expires 0` for never); `ServiceToken.is_valid()` rejects non-active, expired,
  or scope-missing tokens. Lookup is `token_hash` + `status=active`.
- Audit: each successful request updates `last_used_at` + `last_used_ip` on the
  token doc; `wrap_response` success bodies carry a per-request `request_id` for
  tracing. An `EventLog` model exists (`backend/app/model/event_log.py`) but is
  NOT wired into the service-token auth path (no separate audit collection for
  OpenClaw requests — 已核实).

## 2. Endpoint map (all GET, base `/api/v1/integrations/openclaw`)

| Endpoint | Scope | Key params | Notes |
|---|---|---|---|
| `/` | data-read | — | health/info; `version: v1-mvp`, `service_identity` = token name |
| `/stocks` | data-read | page(1), per_page(100), active_status(0/1/2), exchange, market, keyword | stock metadata + data_capabilities |
| `/quotes/daily` | data-read | symbols (docs say required; code does NOT enforce — absent ⇒ full-table scan), start_date, end_date, page, per_page(100) | OHLCV + trade_amount, turnover_rate, trade_status, is_st |
| `/factors/daily` | data-read | symbols, start_date, end_date, page, per_page(50) | fq_factor, *_hfq, open/close, ma_10..ma_120 |
| `/signals` | data-read | date, signal_name, direction, page, per_page(100) | includes factor/price snapshot, generated_at |
| `/quality` | data-read | symbol, asset_type | freshness/coverage; items hard-capped at 500 |
| `/scores` | score-read **or** data-read | date, horizon(5/20/60), stock_code, model_version, status, page(1), per_page(100, max 500) | ordered -date,-score; explanation / input_snapshot / verification |
| `/recommendations/daily` | **data-read only** | date, horizon(5/20/60, default 5), min_score(60.0), limit(20), model_version | score >= min_score; 400 on invalid horizon |
| `/recommendations/performance` | **data-read only** | horizon(default 5), model_version | total_verified, effective_predictions, accuracy_rate, top_recommendations_count, avg_max_profit_top |

> Code-vs-docs discrepancy (verified): `/scores` accepts `openclaw:score-read`, but
> `/recommendations/*` require `openclaw:data-read` (`recommendations.py` decorators).
> `openspec/.../openclaw-data-access/spec.md` claims score-read grants recommendation
> access — **code is authoritative**; reconcile docs/spec via spec gate before relying on it.

## 3. Response envelope & errors

- `wrap_response` (utils.py) success bodies: `success`, `message`, `request_id`
  (uuid4), `generated_at` (ISO-8601), `data`; optional `data_as_of` (ISO-8601).
  Do not return raw Mongo shapes (P2: API response is the contract). Note: the
  `/` health endpoint and 401/403 error bodies are NOT wrapped — they return
  only `success`/`message` (+`version`/`service_identity`/`timestamp` on `/`,
  +`error_code` on auth failures), with no `request_id`/`generated_at`/`data`.
- Errors: missing/bad Authorization header → 401 `AUTH_HEADER_MISSING`;
  invalid/inactive token → 401 `AUTH_FAILED`; valid token missing scope → 403
  `AUTH_FAILED`; service token on a compute endpoint → 403 `SERVICE_TOKEN_BLOCKED`;
  400 for invalid horizon on recommendations. 429 is documented as contract-based
  rate limiting — no rate-limit implementation exists in the backend (grep for
  429/rate-limit/flask-limiter: zero matches; may live at gateway/ingress).

## 4. Freshness semantics

- `data_as_of` = latest available data date for the queried model, with date-range
  filters stripped (`_get_latest_date` in `utils.py`), so it reflects the true
  freshness boundary, not the caller's query window.
- `/scores` items carry `input_snapshot` (freshness of quote/factor/signal inputs)
  and `verification` (hit_target_close, hit_target_intra, return_at_target,
  max_return, min_return, max_drawdown, days_to_max_return, quote_count).
- `/quality` exposes per-asset `status`, `coverage_rate`, `first_data_date` /
  `latest_data_date`, `last_calculated_at`. Consumers must check freshness before
  analysis; stale or quote-blocked state must be explicit (spec requirement), never
  silently assumed fresh.

## 5. Hard boundaries (never violate)

- Service tokens are rejected with 403 on these blueprints (`block_service_tokens`
  matches `Authorization: Bearer st_`): `/api/backtest/*`, `/api/tasks`,
  `/api/score-experiments`, `/api/score-strategies`, `/api/portfolios/*`,
  `/api/decisions/*`, `/api/datahub/*`, `/api/factor-eval/*`.
- Never hand OpenClaw Mongo credentials; never let it trigger scheduling, backfill,
  mutation, or admin actions; never let OpenClaw analysis logic enter caifubao
  (P2 module boundary + OpenClaw-Specific Rules).
- OpenClaw endpoints stay under `/api/v1/integrations/openclaw` — do not add
  OpenClaw access elsewhere.

## 6. Changing the contract (flow)

- Any change to endpoints, response fields, auth/scope/token lifecycle, freshness
  semantics, data ownership, or public docs triggers the **P3 Spec Gate** (`RULES.md`).
- `contract-reviewer` is mandatory whenever the OpenClaw integration is touched;
  `qa-reviewer` for all non-trivial changes; `spec-guardian` when endpoints, auth,
  or boundaries shift.
- Minimize scope: grant least-privilege scopes per token, keep response shapes
  stable, and update docs + openspec in the same change.

## 7. Self-check list

- [ ] Header is `Bearer st_...` (checked case-insensitively)?
- [ ] Scope correct for endpoint — `openclaw:score-read` alone is NOT enough for `/recommendations/*`?
- [ ] Response includes success/message/request_id/generated_at, and data_as_of on data-dependent endpoints?
- [ ] No compute/mutation path reachable with a service token (403 `SERVICE_TOKEN_BLOCKED`)?
- [ ] Plain tokens never logged, committed, or returned after creation; only SHA-256 hash stored?
- [ ] Freshness conveyed via data_as_of / input_snapshot / quality status rather than assumed?
- [ ] Contract change passed P3 Spec Gate + contract-reviewer?
- [ ] Docs (`docs/integrations/openclaw.md`, `docs/operations/service-tokens.md`) and openspec stay in sync with code?
