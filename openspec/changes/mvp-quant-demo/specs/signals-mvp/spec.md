# Signals MVP

## Overview

The MVP signal layer turns existing factor data into a small set of readable trading signals. 

## Architectural Direction

- **Data Model**: All new signals must use `StockSignalDaily` (defined in `backend/app/model/signal.py`). Legacy `SignalData` is deprecated.
- **Computation**: Signal generation is the responsibility of `datahub`. `backend` provides read-only access.
- **Standardization**: Signals must include `direction` (BULLISH/BEARISH), `signal_type`, and a `factor_snapshot` for explainability.

## Initial Signal Set

- `MA10_CROSS_MA20` (Bullish): MA 10 crossing above MA 20.
- `PRICE_ABOVE_MA60` (Bullish): Close price above MA 60.
- `MA20_ABOVE_MA60` (Bullish): MA 20 above MA 60 trend state.

## Current Implementation Status

- [x] **Core Model**: `StockSignalDaily` implemented.
- [x] **Infrastructure**: `MovingAverageSignalService` in `datahub` for bulk processing.
- [x] **Signal 1**: `MA10_CROSS_MA20` implemented in `datahub`.
- [x] **APIs**: User API (`/api/signals`) and OpenClaw API (`/api/v1/integrations/openclaw/signals`) implemented.

## Gaps and Missing Features

- [ ] **Signal 2**: Implement `PRICE_ABOVE_MA60` logic in `datahub`.
- [ ] **Signal 3**: Implement `MA20_ABOVE_MA60` logic in `datahub`.
- [ ] **Processor Migration**: Refactor or deprecate `backend/app/lib/signal_man` to avoid logic duplication.
- [ ] **Data Pipeline**: Ensure `datahub` runs signal updates automatically after daily factor calculation.

## Acceptance Criteria

- Signals can be computed from current factor data in `datahub`.
- The signal page shows a dated list with direction and reason.
- The MVP signal set is small, stable, and explainable via snapshots.
