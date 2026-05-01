# Signals Implementation Status (MVP)

This document tracks the implementation progress of the Signals module against the `signals-mvp/spec.md`.

## 1. Data Model

### Modern Model: `StockSignalDaily`
Located in `backend/app/model/signal.py`.
- **Status**: Implemented and used by APIs.
- **Key Fields**: `stock_code`, `date`, `signal_name`, `direction`, `signal_type`, `strength`, `reason`, `factor_snapshot`.

### Legacy Model: `SignalData`
- **Status**: Still used by `SignalMan` processors.
- **Gap**: Need to migrate processors to use `StockSignalDaily`.

## 2. Signal Generation (SignalMan)

### Infrastructure
- Located in `backend/app/lib/signal_man`.
- **Status**: Basic framework exists but currently uses legacy models and lacks full MVP integration.

### Implemented Signals (Processors)
- **MA Crossover**: `MACrossSignalProcessor` in `moving_average.py`.
    - Supports `MA10_CROSS_MA20` style signals.
- **Price-MA Relation**: `PriceMARelationProcessor` in `moving_average.py`.
    - **Status**: Skeleton only, logic not implemented.

### Missing MVP Signals
- [ ] Close price above MA 60.
- [ ] MA 20 above MA 60 trend state.

## 3. APIs

### User API (`/api/signals`)
- **Status**: Implemented in `backend/app/api/v1/signals.py`.
- **Features**: Supports latest date auto-detection, signal filtering, and pagination.

### OpenClaw Integration API (`/api/v1/integrations/openclaw/signals`)
- **Status**: Implemented in PR 127.
- **Features**: Stable contract for downstream consumers.

## 4. Gaps and Tasks

1.  **Refactor `SignalMan` Processors**:
    *   Update `MACrossSignalProcessor` to save to `StockSignalDaily`.
    *   Implement `PriceMARelationProcessor` logic.
2.  **Standardize Signal Names**:
    *   `MA10_CROSS_MA20` (Bullish/Bearish)
    *   `PRICE_ABOVE_MA60` (Bullish)
    *   `MA20_ABOVE_MA60` (Bullish)
3.  **Datahub Integration**:
    *   Ensure signal generation is triggered as part of the daily `datahub` run after factors are updated.
