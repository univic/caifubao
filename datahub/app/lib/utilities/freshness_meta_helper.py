import logging
import datetime
from mongoengine import NotUniqueError
from app.model.data_freshness import DataFreshnessMeta


logger = logging.getLogger(__name__)


def read_meta_obj(code, meta_type, meta_name, backtest_name=None):
    entry = DataFreshnessMeta.objects(
        code=code, meta_type=meta_type, meta_name=meta_name, backtest_name=backtest_name
    ).first()
    return entry


def read_freshness_meta(code, object_type, meta_type, meta_name, backtest_name=None):
    res = None
    entry = read_meta_obj(
        code=code, meta_type=meta_type, meta_name=meta_name, backtest_name=backtest_name
    )
    # entry = DataFreshnessMeta.objects(code=code, object_type=object_type, meta_type=meta_type, meta_name=meta_name,
    #                                   backtest_name=backtest_name).first()
    if entry:
        res = entry.freshness_datetime
    return res


def upsert_freshness_meta(
    code, object_type, meta_type, meta_name, dt, backtest_name=None
):
    query = DataFreshnessMeta.objects(
        code=code,
        object_type=object_type,
        meta_type=meta_type,
        meta_name=meta_name,
        backtest_name=backtest_name,
    )
    try:
        query.upsert_one(
            set__freshness_datetime=dt, set__calculated_at=datetime.datetime.now()
        )
    except NotUniqueError:
        logger.warning(
            f"{object_type}-{code}-{meta_type}-{meta_name}-{dt} encountered NotUniqueError when trying to upsert freshness meta"
        )


def check_single_factor_freshness(stock_obj, meta_name, scenario):
    delisted = False
    prev_complete_trading_day = scenario.current_datetime_prev_complete_trading_day
    result_code = ""  # OK, UPD, SKIP

    freshness_meta = DataFreshnessMeta.objects(
        code=stock_obj.code, meta_type="factor", meta_name=meta_name
    ).first()
    if freshness_meta:
        freshness_date = freshness_meta.freshness_datetime
        calculated_at = freshness_meta.calculated_at
        if freshness_date and freshness_date == prev_complete_trading_day:
            result_code = "OK"
        elif delisted and calculated_at and calculated_at > freshness_date:
            result_code = "SKIP"
        else:
            result_code = "UPD"
    else:
        result_code = "UPD"
    return result_code


def check_factor_freshness(stock_obj, meta_obj, scenario):
    if stock_obj.active_status != 0 or meta_obj.status == "NO_UPD":
        inactive = True
    else:
        inactive = False
    prev_complete_trading_day = scenario.current_datetime_prev_complete_trading_day
    result_code = "UPD"  # OK, UPD, SKIP

    if meta_obj:
        freshness_date = getattr(meta_obj, "freshness_datetime")
        calculated_at = getattr(meta_obj, "calculated_at")
        if freshness_date and freshness_date == prev_complete_trading_day:
            result_code = "OK"
        elif inactive and calculated_at and calculated_at > freshness_date:
            result_code = "SKIP"

    return result_code


def check_meta_status(scenario, quote_df, meta_obj):
    """
    check meta data is align with underlying data
    """
    result_code = "GO"
    prev_complete_trading_day = scenario.current_datetime_prev_complete_trading_day
    # determine whether its no longer need to update

    if not quote_df.empty:
        latest_quote_date = quote_df.index[-1]
        if (
            latest_quote_date == meta_obj.freshness_datetime
            and latest_quote_date < prev_complete_trading_day
        ):
            set_meta_status(meta_obj, "NO_UPD")
            result_code = "SKIP"

    return result_code


def remove_meta(code, meta_type, meta_name):
    meta_obj = read_meta_obj(code=code, meta_type=meta_type, meta_name=meta_name)
    meta_obj.delete()


def set_meta_status(meta, status):
    meta.status = status
    meta.save()
