import datetime
from app.model.stock import DataFreshnessMeta


def determine_closest_trading_date(trade_calendar, given_time=datetime.datetime.now()):

    closest_avail_trading_day = min(trade_calendar, key=lambda x: (x > given_time, abs(x - given_time)))
    return closest_avail_trading_day


def determine_latest_quote_date(quote_list, date_attribute):
    latest_quote_date = max(quote_list, key=lambda x: x[date_attribute])
    return latest_quote_date[date_attribute]


def determine_date_diff_with_latest_quote(trade_calendar_list, stock_index_obj):
    closest_avail_trading_day = determine_closest_trading_date(trade_calendar_list)
    latest_quote_date = stock_index_obj.data_freshness_meta.daily_quote
    trade_day_list = sorted(trade_calendar_list, reverse=True)
    latest_quote_date_index = trade_day_list.index(latest_quote_date)
    closest_avail_trading_day_index = trade_day_list.index(closest_avail_trading_day)
    date_diff = abs(closest_avail_trading_day_index - latest_quote_date_index)
    return date_diff


def determine_trading_date_diff(trade_calendar_list, trading_day_a, trading_day_b):
    """
    put the earlier date in day_a, and the latter date in day_b
    trade_day_list is descend sorted, the earlier date will have bigger index number than latter
    """
    trade_day_list = sorted(trade_calendar_list, reverse=True)
    trading_day_a_index = trade_day_list.index(trading_day_a)
    trading_day_b_index = trade_day_list.index(trading_day_b)
    date_diff = trading_day_a_index - trading_day_b_index
    return date_diff


def determine_pervious_trading_day(trade_calendar_list, given_date):
    trade_day_list = sorted(trade_calendar_list, reverse=True)
    given_date_index = trade_day_list.index(given_date)
    return trade_day_list[given_date_index + 1]


def determine_next_trading_day(trade_calendar_list, given_date):
    trade_day_list = sorted(trade_calendar_list, reverse=True)
    given_date_index = trade_day_list.index(given_date)
    return trade_day_list[given_date_index - 1]


def update_freshness_meta(obj, freshness_field, freshness_value):
    if obj.data_freshness_meta:
        data_freshness_meta = obj.data_freshness_meta
    else:
        data_freshness_meta = DataFreshnessMeta()
    data_freshness_meta[freshness_field] = freshness_value
    obj.data_freshness_meta = data_freshness_meta


def read_freshness_meta(obj, freshness_field):
    freshness_value = None
    if obj.data_freshness_meta:
        freshness_value = obj.data_freshness_meta[freshness_field]
    return freshness_value
