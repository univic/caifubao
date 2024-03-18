
from app.model.data_freshness import DataFreshnessMeta


def read_freshness_meta(code, object_type, meta_type, meta_name, backtest_name=None):
    res = None
    entry = DataFreshnessMeta.objects(code=code, object_type=object_type, meta_type=meta_type, meta_name=meta_name,
                                      backtest_name=backtest_name).first()
    if entry:
        res = entry.freshness_datetime
    return res


def upsert_freshness_meta(code, object_type, meta_type, meta_name, dt, backtest_name=None):
    query = DataFreshnessMeta.objects(code=code, object_type=object_type, meta_type=meta_type,
                                      meta_name=meta_name, backtest_name=backtest_name)
    query.upsert_one(set__freshness_datetime=dt)
