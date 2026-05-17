# Technical Factors Reference

8 technical factors computed from existing OHLCV data in `StockDailyQuote`.
All functions prefer HFQ-adjusted prices (`close_hfq`, `open_hfq`) with raw price fallback.

## Factor Index

| # | Factor | Function | Description | Typical Range |
|---|--------|----------|-------------|---------------|
| 1 | volume_ratio | `volume / MA20(volume)` | Trading interest intensity. >1 = above-average volume | 0 — 5 |
| 2 | bb_position | `(close - BB_lower) / (BB_upper - BB_lower)` | Bollinger Band(20,2) position. 0=lower band, 0.5=middle, 1=upper | 0 — 1+ |
| 3 | atr_ratio | `ATR(14) / close` | Normalized volatility (Wilder's smoothing). Distinct from raw std-dev | 0 — 0.15 |
| 4 | consecutive_up | `count of consecutive days close > open` | Trend persistence. Positive = uptrend streak length | 0 — 20+ |
| 5 | turnover_accel | `turnover_rate / MA5(turnover_rate)` | Volume acceleration. >1 = increasing interest | 0 — 5 |
| 6 | gap_ratio | `(open - prev_close) / prev_close` | Overnight gap strength. Positive=gap up, negative=gap down | -0.1 — 0.1 |
| 7 | yearly_position | `(close - 52w_low) / (52w_high - 52w_low)` | 52-week range position. 0=low, 1=high | 0 — 1 |
| 8 | rsi_14 | Standard RSI(14) with Wilder's smoothing | Momentum oscillator. >70 overbought, <30 oversold | 0 — 100 |

## Formula Details

### volume_ratio
```
MA20_volume = mean(volume[t-19:t+1])
ratio = volume[t] / MA20_volume
```
Indicates whether today's volume is above or below the 20-day average. Spikes often accompany breakouts or news events.

### bb_position
```
MA = mean(close[t-19:t+1])
STD = pstdev(close[t-19:t+1])
BB_upper = MA + 2*STD
BB_lower = MA - 2*STD
position = (close - BB_lower) / (BB_upper - BB_lower)
```
Values >1 indicate a breakout above the upper band. Values <0 indicate a breakdown below the lower band.

### atr_ratio
Wilder's smoothing:
```
TR[t] = max(high-low, abs(high-prev_close), abs(low-prev_close))
ATR[14] = mean(TR[1:15])  # first value
ATR[t] = (ATR[t-1] * 13 + TR[t]) / 14  # subsequent
ratio = ATR / close
```
Normalizes volatility by price level, making it comparable across stocks.

### rsi_14
Wilder's smoothing:
```
change[t] = close[t] - close[t-1]
gain[t] = max(change[t], 0)
loss[t] = abs(min(change[t], 0))
avg_gain[14] = mean(gain[1:15])
avg_loss[14] = mean(loss[1:15])
RS = avg_gain / avg_loss
RSI = 100 - 100 / (1 + RS)
```

## Evaluation Pipeline

Each factor should be evaluated before integration into the scoring engine:

1. **Compute**: `python -m app.jobs.tech_factor_runner compute <code> <start> <end> --factors <name>`
2. **Evaluate**: `python -m app.jobs.tech_factor_runner evaluate <name> <start> <end> --horizon 20 --save`
3. **Review**: Check IC (> 0.03), ICIR (> 0.5), quintile monotonicity, correlation (< 0.7 with existing components)
4. **Integrate**: Add as a new component in `components.py`, re-run grid search for optimal weight
5. **Validate**: Rolling cross-validation to confirm out-of-sample stability

## Usage in OpenCode

```bash
# List available factors
python -m app.jobs.tech_factor_runner list

# Compute RSI for a single stock
python -m app.jobs.tech_factor_runner compute sh600519 2024-01-01 2024-12-31 --factors rsi_14

# Evaluate RSI predictive power over 1 year
python -m app.jobs.tech_factor_runner evaluate rsi_14 2024-01-01 2024-12-31 --horizon 20 --save

# Compute all 8 factors and save to JSON
python -m app.jobs.tech_factor_runner compute sh600519 2024-01-01 2024-12-31 --all --output factors.json
```
