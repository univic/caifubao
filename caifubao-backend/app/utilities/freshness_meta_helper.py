
from app.model.data_freshness import DataFreshnessMeta


def read_freshness_meta(stock_code, meta_type, name):
    res = None
    entry = DataFreshnessMeta.objects(stock_code=stock_code, meta_type=meta_type, name=name).first()
    if entry:
        res = entry.date
    return res


def upsert_freshness_meta(stock_code, meta_type, name, dt):
    query = DataFreshnessMeta.objects(stock_code=stock_code, meta_type=meta_type, name=name)
    query.upsert_one(set__freshness_datetime=dt)
