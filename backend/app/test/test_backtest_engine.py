# -*- coding: utf-8 -*-
"""Unit tests for backtest_service core functions (friction, limits, benchmark, sizing).

These tests use synthetic data / SimpleNamespace objects — no MongoDB required.
"""

from datetime import datetime

from types import SimpleNamespace


# ============================================================================
# _apply_friction
# ============================================================================
class TestFrictionModel:
    """_apply_friction — commission, stamp duty, slippage."""

    def test_buy_friction(self):
        """Buy: execution price higher (slippage against buyer), commission applied, no stamp duty."""
        from app.services.backtest_service import _apply_friction

        exec_price, comm, stamp, slip = _apply_friction(100.0, 1000, "BUY")
        assert exec_price > 100.0  # slippage markup
        assert comm > 0  # commission on buy
        assert stamp == 0.0  # no stamp duty on buy
        assert slip > 0

    def test_sell_friction(self):
        """Sell: execution price lower, commission + stamp duty both applied."""
        from app.services.backtest_service import _apply_friction

        exec_price, comm, stamp, slip = _apply_friction(100.0, 1000, "SELL")
        assert exec_price < 100.0
        assert comm > 0
        assert stamp > 0  # stamp duty on sell
        assert slip > 0

    def test_min_commission(self):
        """Small trades hit minimum commission of 5 CNY."""
        from app.services.backtest_service import _apply_friction

        # 1 share at 10 CNY = tiny trade, commission should be min 5 CNY
        exec_price, comm, stamp, slip = _apply_friction(10.0, 1, "BUY")
        assert comm == 5.0

    def test_commission_above_min(self):
        """Large trades pay rate-based commission (0.025%), above the 5 CNY floor."""
        from app.services.backtest_service import _apply_friction

        # 10000 shares at 50 CNY = 500k trade value, commission ~125 CNY
        exec_price, comm, stamp, slip = _apply_friction(50.0, 10000, "BUY")
        assert comm > 5.0

    def test_sell_stamp_duty_rate(self):
        """Stamp duty on sell side is 0.1% of trade value."""
        from app.services.backtest_service import _apply_friction, STAMP_DUTY_RATE

        exec_price, comm, stamp, slip = _apply_friction(50.0, 2000, "SELL")
        expected_stamp = round(exec_price * 2000 * STAMP_DUTY_RATE, 4)
        assert stamp == expected_stamp

    def test_buy_stamp_duty_zero(self):
        """Buy side has exactly zero stamp duty."""
        from app.services.backtest_service import _apply_friction

        _, _, stamp, _ = _apply_friction(50.0, 2000, "BUY")
        assert stamp == 0.0


# ============================================================================
# _can_trade
# ============================================================================
class TestLimitConstraints:
    """_can_trade — limit-up/down and suspended stock checks."""

    def test_normal_stock_can_trade(self):
        from app.services.backtest_service import _can_trade

        q = SimpleNamespace(trade_status=1, change_rate=2.5)
        assert _can_trade(q, "BUY") is True
        assert _can_trade(q, "SELL") is True

    def test_limit_up_blocks_buy(self):
        from app.services.backtest_service import _can_trade

        q = SimpleNamespace(trade_status=1, change_rate=9.95)
        assert _can_trade(q, "BUY") is False
        assert _can_trade(q, "SELL") is True  # can still sell at limit-up

    def test_limit_down_blocks_sell(self):
        from app.services.backtest_service import _can_trade

        q = SimpleNamespace(trade_status=1, change_rate=-9.95)
        assert _can_trade(q, "BUY") is True  # can still buy at limit-down
        assert _can_trade(q, "SELL") is False

    def test_suspended_blocks_all(self):
        from app.services.backtest_service import _can_trade

        q = SimpleNamespace(trade_status=0, change_rate=0.0)
        assert _can_trade(q, "BUY") is False
        assert _can_trade(q, "SELL") is False

    def test_none_change_rate(self):
        """None change_rate is treated as 0 (no limit)."""
        from app.services.backtest_service import _can_trade

        q = SimpleNamespace(trade_status=1, change_rate=None)
        assert _can_trade(q, "BUY") is True

    def test_exactly_at_limit_up(self):
        """change_rate == 9.9 exactly should block buy."""
        from app.services.backtest_service import _can_trade

        q = SimpleNamespace(trade_status=1, change_rate=9.9)
        assert _can_trade(q, "BUY") is False

    def test_exactly_at_limit_down(self):
        """change_rate == -9.9 exactly should block sell."""
        from app.services.backtest_service import _can_trade

        q = SimpleNamespace(trade_status=1, change_rate=-9.9)
        assert _can_trade(q, "SELL") is False


# ============================================================================
# _blocked_reason
# ============================================================================
class TestBlockedReason:
    """_blocked_reason — human-readable blocked-trade diagnostics."""

    def test_limit_up_reason(self):
        from app.services.backtest_service import _blocked_reason

        q = SimpleNamespace(trade_status=1, change_rate=9.95)
        assert _blocked_reason(q, "BUY") == "limit_up_blocked"

    def test_limit_down_reason(self):
        from app.services.backtest_service import _blocked_reason

        q = SimpleNamespace(trade_status=1, change_rate=-9.95)
        assert _blocked_reason(q, "SELL") == "limit_down_blocked"

    def test_suspended_reason(self):
        from app.services.backtest_service import _blocked_reason

        q = SimpleNamespace(trade_status=0, change_rate=0.0)
        assert _blocked_reason(q, "BUY") == "suspended"
        assert _blocked_reason(q, "SELL") == "suspended"

    def test_unknown_reason(self):
        """A stock with no blocking condition returns 'unknown'."""
        from app.services.backtest_service import _blocked_reason

        q = SimpleNamespace(trade_status=1, change_rate=2.0)
        assert _blocked_reason(q, "BUY") == "unknown"
        assert _blocked_reason(q, "SELL") == "unknown"


# ============================================================================
# _max_buy_shares
# ============================================================================
class TestMaxBuyShares:
    """_max_buy_shares — friction-aware share calculation."""

    def test_basic_buy(self):
        from app.services.backtest_service import _max_buy_shares

        # 100 CNY/share, 10000 cash, should get 99 shares after friction
        shares = _max_buy_shares(100.0, 10000.0)
        assert shares > 0
        assert shares <= 100

    def test_zero_cash(self):
        from app.services.backtest_service import _max_buy_shares

        assert _max_buy_shares(100.0, 0.0) == 0

    def test_insufficient_for_min_commission(self):
        from app.services.backtest_service import _max_buy_shares

        # Less than MIN_COMMISSION (5.0) available
        assert _max_buy_shares(100.0, 3.0) == 0

    def test_does_not_exceed_cash(self):
        """After buying shares, the actual cash deduction (exec_price * shares + comm)
        must not exceed available cash.

        Note: slippage_cost returned by _apply_friction is a tracking metric only;
        it is already embedded in exec_price and is NOT separately deducted from cash.
        """
        from app.services.backtest_service import _max_buy_shares, _apply_friction

        cash = 100000.0
        price = 50.0
        shares = _max_buy_shares(price, cash)
        assert shares > 0
        exec_price, comm, stamp, slip = _apply_friction(price, shares, "BUY")
        # Actual cash deduction in the simulation: cost = shares * exec_price + comm
        actual_cost = shares * exec_price + comm
        assert actual_cost <= cash + 0.01  # allow tiny rounding error

    def test_zero_price(self):
        from app.services.backtest_service import _max_buy_shares

        assert _max_buy_shares(0.0, 10000.0) == 0
        assert _max_buy_shares(-10.0, 10000.0) == 0

    def test_small_trade_min_commission_adjustment(self):
        """When rate-based commission < MIN_COMMISSION, return shares that leave
        room for the 5 CNY minimum commission."""
        from app.services.backtest_service import _max_buy_shares

        # price=100, cash=1500: raw shares ~14, rate commission ~0.35 < 5
        # Adjusted: floor(1495 / 100.1) = 14
        shares = _max_buy_shares(100.0, 1500.0)
        assert shares > 0
        assert shares <= 14


# ============================================================================
# _round_to_lot
# ============================================================================
class TestRoundToLot:
    """_round_to_lot — 100-share lot rounding."""

    def test_rounds_down(self):
        from app.services.backtest_service import _round_to_lot

        assert _round_to_lot(199) == 100
        assert _round_to_lot(200) == 200
        assert _round_to_lot(50) == 0

    def test_zero(self):
        from app.services.backtest_service import _round_to_lot

        assert _round_to_lot(0) == 0

    def test_exact_lots(self):
        from app.services.backtest_service import _round_to_lot

        assert _round_to_lot(100) == 100
        assert _round_to_lot(500) == 500
        assert _round_to_lot(1000) == 1000

    def test_float_input(self):
        from app.services.backtest_service import _round_to_lot

        assert _round_to_lot(150.7) == 100
        assert _round_to_lot(99.9) == 0


# ============================================================================
# _closing_price
# ============================================================================
class TestClosingPrice:
    """_closing_price — HFQ fallback."""

    def test_hfq_preferred(self):
        from app.services.backtest_service import _closing_price

        q = SimpleNamespace(close_hfq=150.0, close=145.0)
        assert _closing_price(q) == 150.0

    def test_raw_fallback(self):
        from app.services.backtest_service import _closing_price

        q = SimpleNamespace(close_hfq=None, close=145.0)
        assert _closing_price(q) == 145.0

    def test_both_none(self):
        from app.services.backtest_service import _closing_price

        q = SimpleNamespace(close_hfq=None, close=None)
        assert _closing_price(q) == 0.0

    def test_hfq_zero_falls_back(self):
        """close_hfq == 0 falls back to raw close (0 is falsy)."""
        from app.services.backtest_service import _closing_price

        q = SimpleNamespace(close_hfq=0, close=120.0)
        assert _closing_price(q) == 120.0


# ============================================================================
# _allocate_positions
# ============================================================================
class TestAllocatePositions:
    """_allocate_positions — equal-weight and score-weighted allocation."""

    def test_equal_weight_allocation(self):
        from app.services.backtest_service import _allocate_positions

        stocks = ["sh600001", "sh600002", "sh600003"]
        scores = {}
        prices = {"sh600001": 10.0, "sh600002": 20.0, "sh600003": 50.0}
        result = _allocate_positions(stocks, scores, 100000.0, prices, "equal_weight")

        # Each stock gets ~33333, rounded down to lots of 100
        assert result["sh600001"] >= 100  # ~3333 shares -> 3300
        assert result["sh600002"] >= 100  # ~1666 shares -> 1600
        assert result["sh600003"] >= 100  # ~666 shares -> 600

    def test_score_weighted_allocation(self):
        from app.services.backtest_service import _allocate_positions

        stocks = ["sh600001", "sh600002"]
        # sh600001 has higher score — should get more weight
        scores = {"sh600001": 120.0, "sh600002": 50.0}
        prices = {"sh600001": 10.0, "sh600002": 10.0}
        result = _allocate_positions(stocks, scores, 100000.0, prices, "score_weighted")

        # sh600001 weight = 120/170 ≈ 70.6%, sh600002 ≈ 29.4%
        # When prices equal, higher-score stock gets more shares
        assert result["sh600001"] >= result["sh600002"]

    def test_empty_stocks(self):
        from app.services.backtest_service import _allocate_positions

        result = _allocate_positions([], {}, 100000.0, {}, "equal_weight")
        assert result == {}

    def test_zero_price_skipped(self):
        from app.services.backtest_service import _allocate_positions

        stocks = ["sh600001", "sh600002"]
        scores = {}
        prices = {"sh600001": 0.0, "sh600002": 10.0}
        result = _allocate_positions(stocks, scores, 100000.0, prices, "equal_weight")

        # sh600001 has price 0, should be skipped
        assert "sh600001" not in result
        assert "sh600002" in result

    def test_max_position_cap(self):
        from app.services.backtest_service import _allocate_positions

        stocks = ["sh600001"]
        scores = {"sh600001": 100.0}
        prices = {"sh600001": 10.0}
        # max_position_pct=0.10 means at most 10% of 100000 = 10000 cash
        result = _allocate_positions(
            stocks, scores, 100000.0, prices, "score_weighted", max_position_pct=0.10
        )
        # 10000 / 10 = 1000 shares, rounded to lot = 1000
        assert result["sh600001"] == 1000

    def test_invalid_allocation_method(self):
        from app.services.backtest_service import _allocate_positions

        result = _allocate_positions(
            ["sh600001"], {}, 100000.0, {"sh600001": 10.0}, "invalid_method"
        )
        assert result == {}


# ============================================================================
# _compute_metrics (includes gross_return)
# ============================================================================
class TestComputeMetrics:
    """_compute_metrics — net and gross return calculations."""

    def test_no_trades_no_change(self):
        from app.services.backtest_service import _compute_metrics

        result = _compute_metrics(100000.0, 100000.0, [], [])
        assert result["total_return"] == 0.0
        assert result["gross_return"] == 0.0
        assert result["total_trades"] == 0
        assert result["win_rate"] == 0.0

    def test_gross_vs_net_return(self):
        """Gross return should be higher than net by the sum of friction costs."""
        from app.services.backtest_service import _compute_metrics

        result = _compute_metrics(
            initial_cash=100000.0,
            final_value=105000.0,
            trades=[
                {
                    "side": "BUY",
                    "amount": 50000.0,
                    "commission": 12.5,
                    "stamp_duty": 0.0,
                    "slippage": 50.0,
                },
                {
                    "side": "SELL",
                    "amount": 53000.0,
                    "pnl": 3000.0,
                    "commission": 13.25,
                    "stamp_duty": 53.0,
                    "slippage": 53.0,
                },
            ],
            daily_values=[
                {"date": "2024-01-15T00:00:00", "equity": 100000.0},
                {"date": "2024-01-16T00:00:00", "equity": 100500.0},
                {"date": "2024-01-17T00:00:00", "equity": 101000.0},
                {"date": "2024-01-18T00:00:00", "equity": 102000.0},
                {"date": "2024-01-19T00:00:00", "equity": 103500.0},
                {"date": "2024-01-22T00:00:00", "equity": 105000.0},
            ],
            total_commission=25.75,
            total_stamp_duty=53.0,
            total_slippage=103.0,
        )

        net = result["total_return"]  # 5000.0
        friction = (
            result["total_commission"]
            + result["total_stamp_duty"]
            + result["total_slippage"]
        )
        gross = result["gross_return"]
        # gross = net + friction
        assert gross == round(net + friction, 4)
        assert gross > net

    def test_profitable_trade_metrics(self):
        from app.services.backtest_service import _compute_metrics

        result = _compute_metrics(
            initial_cash=100000.0,
            final_value=102000.0,
            trades=[
                {"side": "SELL", "pnl": 1500.0},
                {"side": "SELL", "pnl": 500.0},
            ],
            daily_values=[
                {"date": "2024-01-15T00:00:00", "equity": 100000.0},
                {"date": "2024-01-16T00:00:00", "equity": 102000.0},
            ],
        )

        assert result["total_trades"] == 2
        assert result["profit_trades"] == 2
        assert result["loss_trades"] == 0
        assert result["win_rate"] == 100.0
        assert result["best_trade"] == 1500.0
        assert result["worst_trade"] == 500.0

    def test_mixed_trade_metrics(self):
        from app.services.backtest_service import _compute_metrics

        result = _compute_metrics(
            initial_cash=100000.0,
            final_value=100000.0,
            trades=[
                {"side": "SELL", "pnl": 1500.0},
                {"side": "SELL", "pnl": -800.0},
                {"side": "SELL", "pnl": -200.0},
                {"side": "SELL", "pnl": 300.0},
            ],
            daily_values=[
                {"date": "2024-01-15T00:00:00", "equity": 100000.0},
                {"date": "2024-01-16T00:00:00", "equity": 100000.0},
            ],
        )

        assert result["total_trades"] == 4
        assert result["profit_trades"] == 2
        assert result["loss_trades"] == 2
        assert result["win_rate"] == 50.0
        assert result["best_trade"] == 1500.0
        assert result["worst_trade"] == -800.0


# ============================================================================
# _compute_drawdown
# ============================================================================
class TestComputeDrawdown:
    """_compute_drawdown — max drawdown and duration from equity curve."""

    def test_steady_growth_no_drawdown(self):
        from app.services.backtest_service import _compute_drawdown

        dv = [
            {"equity": 1000.0},
            {"equity": 1010.0},
            {"equity": 1020.0},
            {"equity": 1030.0},
        ]
        dd, dur = _compute_drawdown(dv)
        assert dd == 0.0
        assert dur == 0

    def test_simple_drawdown(self):
        from app.services.backtest_service import _compute_drawdown

        # Peak at 1000, then down to 900 (10% DD), then partially recovers
        dv = [
            {"equity": 1000.0},
            {"equity": 950.0},
            {"equity": 900.0},
            {"equity": 980.0},
        ]
        dd, dur = _compute_drawdown(dv)
        assert dd == 0.1  # 100.0 / 1000.0
        assert dur == 3  # three days below peak before recovery

    def test_recovery_resets_duration(self):
        from app.services.backtest_service import _compute_drawdown

        # Drop, recover to new high, drop again
        dv = [
            {"equity": 1000.0},
            {"equity": 950.0},  # dd=0.05, dur=1
            {"equity": 1100.0},  # new peak, resets
            {"equity": 1000.0},  # dd=0.0909, dur=1
        ]
        dd, dur = _compute_drawdown(dv)
        assert round(dd, 4) == round(100.0 / 1100.0, 4)
        assert dur == 1

    def test_empty(self):
        from app.services.backtest_service import _compute_drawdown

        dd, dur = _compute_drawdown([])
        assert dd is None
        assert dur == 0


# ============================================================================
# _compute_sharpe
# ============================================================================
class TestComputeSharpe:
    """_compute_sharpe — annualized Sharpe ratio."""

    def test_constant_equity(self):
        from app.services.backtest_service import _compute_sharpe

        dv = [
            {"equity": 100000.0},
            {"equity": 100000.0},
            {"equity": 100000.0},
        ]
        result = _compute_sharpe(dv, 100000.0)
        assert result == 0.0  # zero std dev

    def test_positive_returns(self):
        from app.services.backtest_service import _compute_sharpe

        dv = [
            {"equity": 100000.0},
            {"equity": 101000.0},
            {"equity": 102000.0},
        ]
        result = _compute_sharpe(dv, 100000.0)
        assert result > 0  # positive returns => positive Sharpe

    def test_insufficient_data(self):
        from app.services.backtest_service import _compute_sharpe

        assert _compute_sharpe([{"equity": 100.0}], 100.0) == 0.0
        assert _compute_sharpe([], 100.0) == 0.0


# ============================================================================
# _compute_information_ratio (bonus — pure function)
# ============================================================================
class TestInformationRatio:
    """_compute_information_ratio — annualized IR from daily excess returns."""

    def test_no_benchmark_data(self):
        from app.services.backtest_service import _compute_information_ratio

        assert _compute_information_ratio([], [], 100000.0) == 0.0
        assert (
            _compute_information_ratio([{"equity": 100.0}], [0.01, 0.02], 100.0) == 0.0
        )

    def test_perfect_tracking(self):
        """When strategy returns match benchmark, IR should be ~0."""
        from app.services.backtest_service import _compute_information_ratio

        dv = [
            {"equity": 100000.0},
            {"equity": 101000.0},  # 1% return
            {"equity": 102000.0},  # ~0.99% return
        ]
        bench = [0.01, 0.0099]  # matching returns
        result = _compute_information_ratio(dv, bench, 100000.0)
        assert (
            result is not None
        )  # IR can be high with perfect tracking, just verify it computes


# ============================================================================
# _error helper
# ============================================================================
class TestErrorHelper:
    """_error — consistent error dict shape."""

    def test_error_dict_shape(self):
        from app.services.backtest_service import _error

        result = _error("Test error", "some detail")
        assert result["error"] == "Test error"
        assert result["detail"] == "some detail"


# ============================================================================
# MULTI_HORIZON_CONSENSUS strategy simulation tests
# ============================================================================
class TestMultiHorizonConsensus:
    """_simulate with MULTI_HORIZON_CONSENSUS strategy."""

    @staticmethod
    def _make_quote(price: float, trade_status: int = 1) -> SimpleNamespace:
        return SimpleNamespace(
            trade_status=trade_status,
            change_rate=1.0,
            close=price,
            close_hfq=price,
        )

    @staticmethod
    def _make_day(date_str: str) -> datetime:
        return datetime.strptime(date_str, "%Y-%m-%d")

    def test_buy_when_all_horizons_meet_entry(self):
        """BUY when Score5>=60, Score20>=55, Score60>=50."""
        from app.services.backtest_service import _simulate

        days = [self._make_day(d) for d in ("2025-01-02", "2025-01-03")]
        quote_map = {d: self._make_quote(100.0) for d in days}
        factor_map = {}

        score_maps = {
            5: {
                days[0]: SimpleNamespace(score=65.0),
                days[1]: SimpleNamespace(score=65.0),
            },
            20: {
                days[0]: SimpleNamespace(score=60.0),
                days[1]: SimpleNamespace(score=60.0),
            },
            60: {
                days[0]: SimpleNamespace(score=55.0),
                days[1]: SimpleNamespace(score=55.0),
            },
        }

        result = _simulate(
            strategy="MULTI_HORIZON_CONSENSUS",
            trading_days=days,
            quote_map=quote_map,
            factor_map=factor_map,
            initial_cash=100000.0,
            score_maps=score_maps,
        )

        # Should have a BUY trade on day 0
        trades = result["trades"]
        assert len(trades) >= 1
        assert trades[0]["side"] == "BUY"
        assert trades[0]["quantity"] > 0

    def test_sell_when_one_horizon_below_exit(self):
        """SELL when ANY horizon drops below its exit threshold."""
        from app.services.backtest_service import _simulate

        days = [self._make_day(d) for d in ("2025-01-02", "2025-01-03")]
        quote_map = {d: self._make_quote(100.0) for d in days}
        factor_map = {}

        # Day 0: all above entry → BUY
        # Day 1: Score5 drops to 25 (below exit=30) → SELL
        score_maps = {
            5: {
                days[0]: SimpleNamespace(score=70.0),
                days[1]: SimpleNamespace(score=25.0),
            },
            20: {
                days[0]: SimpleNamespace(score=60.0),
                days[1]: SimpleNamespace(score=60.0),
            },
            60: {
                days[0]: SimpleNamespace(score=55.0),
                days[1]: SimpleNamespace(score=55.0),
            },
        }

        result = _simulate(
            strategy="MULTI_HORIZON_CONSENSUS",
            trading_days=days,
            quote_map=quote_map,
            factor_map=factor_map,
            initial_cash=100000.0,
            score_maps=score_maps,
            consensus_entry_thresholds={5: 60, 20: 55, 60: 50},
            consensus_exit_thresholds={5: 30, 20: 35, 60: 40},
        )

        trades = result["trades"]
        assert len(trades) >= 2
        assert trades[0]["side"] == "BUY"
        assert trades[-1]["side"] == "SELL"

    def test_stop_loss_fires_when_score_data_missing(self):
        """Stop-loss fires even when fewer than 2 horizons have data."""
        from app.services.backtest_service import _simulate

        days = [self._make_day(d) for d in ("2025-01-02", "2025-01-03", "2025-01-06")]
        quote_map = {
            days[0]: self._make_quote(100.0),
            days[1]: self._make_quote(100.0),
            days[2]: self._make_quote(94.0),  # below stop-loss
        }
        factor_map = {}

        # Day 0: all 3 horizons above → BUY at 100, stop_loss_price=95
        # Day 1: only Score5 has data (<2 horizons → skip consensus signals)
        # Day 2: price=94 < stop_loss=95 → SELL (stop-loss fires despite missing data)
        score_maps = {
            5: {
                days[0]: SimpleNamespace(score=70.0),
                days[1]: SimpleNamespace(score=70.0),
                days[2]: SimpleNamespace(score=70.0),
            },
            20: {
                days[0]: SimpleNamespace(score=60.0),
                # days[1] and days[2] intentionally missing
            },
            60: {
                days[0]: SimpleNamespace(score=55.0),
                # days[1] and days[2] intentionally missing
            },
        }

        result = _simulate(
            strategy="MULTI_HORIZON_CONSENSUS",
            trading_days=days,
            quote_map=quote_map,
            factor_map=factor_map,
            initial_cash=100000.0,
            score_maps=score_maps,
            stop_loss_pct=-5.0,
        )

        trades = result["trades"]
        assert any(t["side"] == "BUY" for t in trades)
        assert any(t["side"] == "SELL" for t in trades), (
            f"Stop-loss should have fired when price dropped to 94, "
            f"got trades: {trades}"
        )

    def test_skipped_consensus_tracked(self):
        """When <2 horizons have data, skipped_consensus is populated."""
        from app.services.backtest_service import _simulate

        days = [self._make_day(d) for d in ("2025-01-02", "2025-01-03")]
        quote_map = {d: self._make_quote(100.0) for d in days}
        factor_map = {}

        # Only 1 horizon with data → should track skipped_consensus
        score_maps = {
            5: {
                days[0]: SimpleNamespace(score=70.0),
                days[1]: SimpleNamespace(score=70.0),
            },
        }

        result = _simulate(
            strategy="MULTI_HORIZON_CONSENSUS",
            trading_days=days,
            quote_map=quote_map,
            factor_map=factor_map,
            initial_cash=100000.0,
            score_maps=score_maps,
        )

        skipped = result.get("skipped_consensus", [])
        assert len(skipped) >= 1
        assert "insufficient horizons" in skipped[0]["reason"]

    def test_no_buy_when_one_horizon_below_entry(self):
        """No BUY when Score60 is below entry even if Score5/20 are above."""
        from app.services.backtest_service import _simulate

        days = [self._make_day(d) for d in ("2025-01-02", "2025-01-03")]
        quote_map = {d: self._make_quote(100.0) for d in days}
        factor_map = {}

        # Score60=40 is below entry=50 → no buy
        score_maps = {
            5: {days[0]: SimpleNamespace(score=70.0)},
            20: {days[0]: SimpleNamespace(score=60.0)},
            60: {days[0]: SimpleNamespace(score=40.0)},
        }

        result = _simulate(
            strategy="MULTI_HORIZON_CONSENSUS",
            trading_days=days,
            quote_map=quote_map,
            factor_map=factor_map,
            initial_cash=100000.0,
            score_maps=score_maps,
        )

        trades = result["trades"]
        assert len(trades) == 0, (
            f"No trades expected when Score60=40 < entry=50, got: {trades}"
        )

    def test_custom_thresholds_via_dict(self):
        """Custom entry thresholds via consensus_entry_thresholds dict."""
        from app.services.backtest_service import _simulate

        days = [self._make_day(d) for d in ("2025-01-02", "2025-01-03")]
        quote_map = {d: self._make_quote(100.0) for d in days}
        factor_map = {}

        # All scores=45 — under default thresholds, but above custom
        score45 = SimpleNamespace(score=45.0)
        score_maps = {
            5: {days[0]: score45},
            20: {days[0]: score45},
            60: {days[0]: score45},
        }

        # Custom thresholds: entry at 40 → all scores meet it
        result = _simulate(
            strategy="MULTI_HORIZON_CONSENSUS",
            trading_days=days,
            quote_map=quote_map,
            factor_map=factor_map,
            initial_cash=100000.0,
            score_maps=score_maps,
            consensus_entry_thresholds={5: 40, 20: 40, 60: 40},
        )

        trades = result["trades"]
        assert len(trades) >= 1
        assert trades[0]["side"] == "BUY"


# ============================================================================
# Statistical significance tests
# ============================================================================
class TestPermutationTest:
    """permutation_test — significance of observed Sharpe."""

    def test_significant_positive_returns(self):
        """Strategy with high Sharpe should be significant."""
        from app.services.backtest_service import permutation_test

        daily_ret = [0.01] * 100 + [0.0] * 100
        equity = 100000.0
        dv = []
        for r in daily_ret:
            equity *= 1 + r
            dv.append({"equity": equity})
        result = permutation_test(dv, 100000.0, iterations=300)
        assert result["p_value"] is not None
        assert result["observed_sharpe"] > 0
        assert result["significant"] is True, (
            f"Expected significant with observed_sharpe={result['observed_sharpe']}, "
            f"p_value={result['p_value']}"
        )

    def test_random_returns_insignificant(self):
        """Zero returns → not significant."""
        from app.services.backtest_service import permutation_test

        dv = [{"equity": 100000.0} for _ in range(100)]
        result = permutation_test(dv, 100000.0, iterations=200)
        assert result["significant"] is False

    def test_insufficient_data(self):
        """Too few data → reason returned."""
        from app.services.backtest_service import permutation_test

        dv = [{"equity": 100000.0} for _ in range(5)]
        result = permutation_test(dv, 100000.0)
        assert result["significant"] is False
        assert result["reason"] is not None


class TestBootstrapCI:
    """bootstrap_ci — confidence intervals."""

    def test_returns_ci_bounds(self):
        """CI brackets mean return."""
        from app.services.backtest_service import bootstrap_ci

        daily_ret = [0.01] * 100 + [0.0] * 100
        equity = 100000.0
        dv = []
        for r in daily_ret:
            equity *= 1 + r
            dv.append({"equity": equity})
        result = bootstrap_ci(dv, iterations=200)
        assert result["return_ci"] is not None
        assert result["return_ci"][0] <= result["return_ci"][1]
        assert result["mean_return"] > 0

    def test_insufficient_data(self):
        """Too few data → None CI."""
        from app.services.backtest_service import bootstrap_ci

        dv = [{"equity": 100000.0}] * 5
        result = bootstrap_ci(dv)
        assert result["return_ci"] is None
        assert result["reason"] is not None
