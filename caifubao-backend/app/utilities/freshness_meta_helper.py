
from app.model.data_freshness import DataFreshnessMeta


def read_freshness_meta(stock_obj, name):
    res = None
    code = stock_obj.code
    entry = DataFreshnessMeta.objects(code=code, name=name).first()
    if entry:
        res = entry.date
    return res


def upsert_freshness_meta(stock_obj, name, meta_type, date):
    query = DataFreshnessMeta.objects(code=stock_obj.code, name=name, type=meta_type)
    query.upsert_one(set__date=date)
