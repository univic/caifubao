from mongoengine import DateTimeField, Document, FloatField, StringField


class StockDailyBasic(Document):
    """
    Daily valuation snapshot per stock per trade date, sourced from tushare
    pro.daily_basic (one full-market row per stock per trade date).

    Values are computed by tushare at that trade date's close from the latest
    published financials — point-in-time by trade_date (no look-ahead), so the
    collection is safe to join into research snapshots keyed by trade date.
    Suspended stocks are absent for that date (tushare omits them).

    Units follow tushare: total_mv/circ_mv are in 万元; dv_ttm and
    turnover_rate are percentages; pe_ttm/pb/ps_ttm are plain ratios.
    """

    meta = {
        "collection": "stock_daily_basic",
        "indexes": [
            {"fields": ["code", "date"], "unique": True},
            ("code", "-date"),
            "-date",
        ],
    }
    code = StringField(required=True)
    date = DateTimeField(required=True)
    pe_ttm = FloatField()
    pb = FloatField()
    ps_ttm = FloatField()
    dv_ttm = FloatField()  # % 股息率TTM
    total_mv = FloatField()  # 万元 总市值
    circ_mv = FloatField()  # 万元 流通市值
    turnover_rate = FloatField()  # % 换手率
