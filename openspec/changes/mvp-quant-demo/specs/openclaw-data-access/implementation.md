# OpenClaw Data Integration Guide (MVP)

This document provides technical details for integrating OpenClaw with the Caifubao backend.

## Authentication

All requests to the OpenClaw integration APIs must include a Service Token in the `Authorization` header.

**Header Format:**
```http
Authorization: Bearer <your-service-token>
```

**Token Scopes:**
- `openclaw:data-read`: Required for all read-only data access.

## API Endpoints

The base URL for all OpenClaw integration endpoints is `/api/v1/integrations/openclaw`.

### 1. Stock Master Data
`GET /stocks`

Returns a list of stocks currently tracked by Caifubao.

**Query Parameters:**
- `page` (int, default: 1): Page number.
- `per_page` (int, default: 100): Number of items per page.
- `active_status` (int): 0 for normal, 1 for inactive, 2 for delisted.
- `exchange` (string): Exchange code (e.g., `sh`, `sz`, `bj`).
- `keyword` (string): Search by stock code or name.

---

### 2. Daily Quotes
`GET /quotes/daily`

Returns daily OHLCV (Open, High, Low, Close, Volume) data.

**Query Parameters:**
- `symbols` (string, required): Comma-separated list of stock codes (e.g., `sh600519,sz000001`).
- `start_date` (string): Start date in `YYYY-MM-DD` format.
- `end_date` (string): End date in `YYYY-MM-DD` format.
- `page` / `per_page`: Pagination parameters.

---

### 3. Daily Factors & Adjusted Prices
`GET /factors/daily`

Returns adjusted prices (HFQ) and technical factors (MA).

**Query Parameters:**
- `symbols` (string, required): Comma-separated list of stock codes.
- `start_date` / `end_date`: Date range filters.

**Data Included:**
- `fq_factor`: Restatement factor.
- `open_hfq`, `close_hfq`, `high_hfq`, `low_hfq`: Backward-adjusted prices.
- `ma_10` to `ma_120`: Moving average values.

---

### 4. Daily Signals
`GET /signals`

Returns quantitative signals triggered on a specific date.

**Query Parameters:**
- `date` (string): Specific date (`YYYY-MM-DD`).
- `signal_name` (string): e.g., `MA10_CROSS_MA20`.
- `direction` (string): `BULLISH` or `BEARISH`.

---

### 5. Data Quality & Freshness
`GET /quality`

Provides information on the freshness and completeness of the data.

**Query Parameters:**
- `symbol` (string): Filter by a specific stock code.
- `asset_type` (string): e.g., `quote`.

## Response Format

All responses follow a standard structure:

```json
{
  "success": true,
  "message": "Success",
  "request_id": "uuid-string",
  "generated_at": "ISO-8601-timestamp",
  "data": { ... }
}
```

## Error Handling

- **401 Unauthorized**: Missing or invalid `Authorization` header or token.
- **403 Forbidden**: Token exists but lacks the required scope (e.g., `openclaw:data-read`).
- **404 Not Found**: Resource not found.

## Operational: Managing Service Tokens

To manage service tokens for OpenClaw, use the provided Python script:

```bash
# Create a new token for OpenClaw
python3 backend/app/scripts/manage_service_tokens.py create --name openclaw-prod --scopes openclaw:data-read --expires 365

# List all tokens
python3 backend/app/scripts/manage_service_tokens.py list

# Revoke a token
python3 backend/app/scripts/manage_service_tokens.py revoke --name openclaw-prod
```
