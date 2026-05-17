# OpenClaw Integration Guide

This guide explains how to consume Caifubao market data, factors, and signals via the OpenClaw Integration API.

## 1. Authentication

The OpenClaw Integration API uses **Service Tokens** for authentication. These tokens are tied to a specific service identity and have defined scopes.

### Header Requirement
All requests must include the token in the `Authorization` header using the `Bearer` scheme.

```http
Authorization: Bearer st_your_service_token_here
```

### Security Notes
- Tokens should be treated as secrets.
- If a token is compromised, contact the Caifubao administrator immediately to have it revoked.
- Tokens are restricted to the `openclaw:data-read` scope, which only allows read-only access to specific endpoints.

## 2. API Reference

**Base URL:** `/api/v1/integrations/openclaw`

### 2.1 Stock Metadata
`GET /stocks`

Fetch the list of stocks and indices supported by Caifubao.

**Parameters:**
- `page`: Page number (default: 1)
- `per_page`: Items per page (default: 100)
- `keyword`: Search by code or name
- `active_status`: Filter by status (0: Normal, 1: Inactive, 2: Delisted)

### 2.2 Daily Quotes
`GET /quotes/daily`

Fetch historical or latest daily OHLCV data.

**Parameters:**
- `symbols`: **Required**. Comma-separated list of symbols (e.g., `sh600519,sz000001`)
- `start_date`: `YYYY-MM-DD`
- `end_date`: `YYYY-MM-DD`
- `page` / `per_page`: Pagination parameters

### 2.3 Daily Factors & Adjusted Prices
`GET /factors/daily`

Fetch backward-adjusted prices (HFQ) and technical factors (MA).

**Parameters:**
- `symbols`: **Required**. Comma-separated list of symbols.
- `start_date` / `end_date`: Date range.

**Returned Factors:**
- `fq_factor`: The adjustment factor.
- `open_hfq`, `close_hfq`, `high_hfq`, `low_hfq`: Adjusted prices.
- `ma_10`, `ma_20`, `ma_30`, `ma_60`, `ma_120`: Moving averages.

### 2.4 Daily Signals
`GET /signals`

Fetch signals triggered for a specific date or strategy.

**Parameters:**
- `date`: `YYYY-MM-DD`
- `signal_name`: Specific signal identifier (e.g., `MA10_CROSS_MA20`)
- `direction`: `BULLISH` or `BEARISH`

### 2.5 Data Quality
`GET /quality`

Check the freshness and coverage of data assets.

**Parameters:**
- `symbol`: Filter by a specific stock.
- `asset_type`: e.g., `quote`.

### 2.6 Score Predictions
`GET /scores`

Fetch multi-horizon score predictions (Score5, Score20, Score60) with per-component explanations, input-snapshot freshness, and verification metrics.

Requires the `openclaw:score-read` scope (or the broader `openclaw:data-read` scope).

**Parameters:**
- `date`: `YYYY-MM-DD` — filter by evaluation date
- `horizon`: `5`, `20`, or `60`
- `stock_code`: filter by stock code (e.g., `sh600519`)
- `model_version`: filter by model version (e.g., `score_v2_202604`)
- `status`: filter by verification status (`PENDING`, `TRACKING`, `VERIFIED`, `INSUFFICIENT_DATA`, `BLOCKED`, `FAILED`)
- `page` / `per_page`: Pagination parameters (default 100, max 500)

**Response Fields (per item):**

| Field | Type | Description |
| :--- | :--- | :--- |
| `stock_code` | string | Stock code (e.g., `sh600519`) |
| `stock_name` | string | Stock name |
| `date` | string | Evaluation date (ISO-8601) |
| `horizon` | int | Prediction horizon (5, 20, 60 trading days) |
| `score` | float | Score value (higher = stronger opportunity) |
| `rank` | int | Rank within date/horizon cohort |
| `percentile` | float | Percentile within cohort |
| `recommendation` | string | `BUY`, `WATCH`, `AVOID`, or `NONE` |
| `status` | string | Verification status |
| `base_price` | float | Close price on evaluation date |
| `target_date` | string | Target date for verification (ISO-8601) |
| `model_version` | string | Scoring model version |
| `explanation` | object | Per-component scores, penalties, thresholds, evidence |
| `input_snapshot` | object | Input data freshness for quote, factor, signal inputs |
| `verification` | object/null | Outcomes: `hit_target_close`, `hit_target_intra`, `return_at_target`, `max_return`, `min_return`, `max_drawdown`, `days_to_max_return`, `quote_count` |

**Example Request:**
```http
GET /api/v1/integrations/openclaw/scores?horizon=20&date=2026-04-15&per_page=10
Authorization: Bearer st_your_service_token_here
```

**Example Response:**
```json
{
  "success": true,
  "message": "Success",
  "request_id": "a1b2c3d4-...",
  "generated_at": "2026-05-17T12:00:00",
  "data": {
    "items": [
      {
        "stock_code": "sh600519",
        "stock_name": "贵州茅台",
        "date": "2026-04-15",
        "horizon": 20,
        "score": 82.5,
        "rank": 1,
        "percentile": 0.99,
        "recommendation": "BUY",
        "status": "VERIFIED",
        "base_price": 1650.0,
        "target_date": "2026-05-13",
        "model_version": "score_v2_202604",
        "explanation": { ... },
        "input_snapshot": { ... },
        "verification": {
          "hit_target_close": true,
          "hit_target_intra": true,
          "return_at_target": 0.034,
          "max_return": 0.052,
          "min_return": -0.012,
          "max_drawdown": -0.018,
          "days_to_max_return": 8,
          "quote_count": 20
        }
      }
    ],
    "total": 1250,
    "page": 1,
    "per_page": 10
  }
}
```

## 3. Response Format

All responses follow this standard structure:

| Field | Type | Description |
| :--- | :--- | :--- |
| `success` | boolean | Indicates if the request was successful |
| `message` | string | Human-readable status message |
| `request_id` | string | Unique ID for tracing and auditing |
| `generated_at` | string | ISO-8601 timestamp of when the response was generated |
| `data` | object | The actual payload (items, total, etc.) |
| `data_as_of` | string | (Optional) The date the data was last updated |

## 4. Error Codes

| Status | Message | Description |
| :--- | :--- | :--- |
| 401 | Missing or invalid Authorization header | Token is missing or incorrectly formatted. |
| 401 | Invalid or inactive token | The token is wrong, revoked, or expired. |
| 403 | Token missing required scope | The token is valid but doesn't have permission for this API. |
| 429 | Too Many Requests | Rate limit exceeded (contract-based). |
