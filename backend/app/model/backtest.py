import datetime

from mongoengine import (
    Document,
    StringField,
    DateTimeField,
    FloatField,
    IntField,
    ListField,
    DictField,
)


class BackTest(Document):
    """Legacy backtest record used by the old BasicBackTester engine.

    Preserved for backward compatibility. The MVP uses BacktestResult instead.
    """

    name = StringField(unique=True)
    strategy = StringField()
    start_date = DateTimeField()
    end_date = DateTimeField()
    created_at = DateTimeField(default=datetime.datetime.utcnow)
    started_at = DateTimeField()
    completed_at = DateTimeField()
    status = StringField(default="CRTD")
    exec_result = StringField()
    exec_msg = StringField()
    earning_rate = FloatField()


class BacktestResult(Document):
    """MVP backtest result record.

    One record per backtest run. Stores the full trade list and daily equity
    curve so that the API can return complete results without recomputation.
    """

    name = StringField(required=True, unique=True)
    stock_code = StringField(required=True)
    stock_name = StringField()
    strategy = StringField(required=True)
    start_date = DateTimeField(required=True)
    end_date = DateTimeField(required=True)
    initial_cash = FloatField(default=100000.0)
    final_value = FloatField()
    total_return = FloatField()
    total_return_pct = FloatField()
    annualized_return = FloatField()
    max_drawdown = FloatField()
    max_drawdown_duration = IntField()
    sharpe_ratio = FloatField()
    win_rate = FloatField()
    total_trades = IntField(default=0)
    profit_trades = IntField(default=0)
    loss_trades = IntField(default=0)
    best_trade = FloatField()
    worst_trade = FloatField()
    status = StringField(default="PENDING")  # PENDING, RUNNING, COMPLETED, FAILED
    error_message = StringField()
    trades = ListField(DictField())  # list of trade records
    daily_values = ListField(DictField())  # list of daily equity curve

    # Friction costs
    total_commission = FloatField(default=0.0)
    total_stamp_duty = FloatField(default=0.0)
    total_slippage = FloatField(default=0.0)
    gross_return = FloatField()  # return before friction
    gross_return_pct = FloatField()  # return pct before friction

    # Benchmark comparison
    benchmark_code = StringField(default="sh000300")  # CSI 300
    benchmark_return = FloatField()  # benchmark absolute return
    benchmark_return_pct = FloatField()  # benchmark return pct
    benchmark_annualized_return = FloatField()
    excess_return = FloatField()  # strategy - benchmark
    excess_return_pct = FloatField()
    information_ratio = FloatField()

    # Strategy config (for score-driven strategies)
    score_config = DictField()  # scoring config snapshot
    horizon = IntField()  # scoring horizon used

    # Multi-stock / portfolio fields
    per_stock_contributions = ListField(
        DictField()
    )  # per-stock realized PnL, trade count, max DD
    top_n = IntField()  # TOP_N_ROTATION param
    rebalance_interval = IntField()  # rebalance frequency
    allocation = StringField()  # position sizing method

    created_at = DateTimeField(default=datetime.datetime.utcnow)
    completed_at = DateTimeField()

    meta = {
        "collection": "backtest_results",
        "indexes": [
            "stock_code",
            "strategy",
            "-created_at",
            ("stock_code", "-created_at"),
        ],
    }
