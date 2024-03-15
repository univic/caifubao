
from app.model.data_freshness import DataFreshnessMeta


def read_freshness_meta(stock_code, meta_type, meta_name, backtest_name=None):
    res = None
    entry = DataFreshnessMeta.objects(stock_code=stock_code, meta_type=meta_type, meta_name=meta_name,
                                      backtest_name=backtest_name).first()
    if entry:
        res = entry.date
    return res


def upsert_freshness_meta(stock_code, meta_type, meta_name, dt, backtest_name=None):
    # TODO: this seems has no effect
    query = DataFreshnessMeta.objects(stock_code=stock_code, meta_type=meta_type,
                                      meta_name=meta_name, backtest_name=backtest_name)
    query.upsert_one(set__freshness_datetime=dt)
