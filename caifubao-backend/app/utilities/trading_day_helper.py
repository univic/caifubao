import re
import logging
import datetime
from app.model.stock import FinanceMarket, StockDailyQuote


logger = logging.getLogger(__name__)


def determine_closest_trading_date(trade_calendar, given_time=datetime.datetime.now()):
    divide_hour = 3
    if given_time.hour < divide_hour:
        given_time = given_time - datetime.timedelta(days=1)
    closest_avail_trading_day = min(trade_calendar, key=lambda x: (x > given_time, abs(x - given_time)))
    return closest_avail_trading_day


def determine_most_recent_previous_complete_trading_day(trade_calendar, given_time):
    # rewrite on 20240323
    if trade_calendar:
        """
        the lambda func return a tuple that make two comparison, the first comparison determines if the given time is 
        less than the item being iterated and gives a boolean value. 
        the second comparison determines the absolute difference of the time.
        the min func then compare the key value that provided by the lambda func, in this case, False is prior to True.
        """
        divide_hour = 3
        if given_time.hour < divide_hour:
            given_time = given_time - datetime.timedelta(days=1)
        closest_avail_trading_day = min(trade_calendar, key=lambda x: (x > given_time, abs(x - given_time)))
    else:
        closest_avail_trading_day = None
    return closest_avail_trading_day


def determine_most_recent_next_trading_dt(trade_calendar, given_time):
    # rewrite on 20240323
    if trade_calendar:
        """
        the lambda func return a tuple that make two comparison, the first comparison determines if the given time is 
        less than the item being iterated and gives a boolean value. 
        the second comparison determines the absolute difference of the time.
        the min func then compare the key value that provided by the lambda func, in this case, False is prior to True.
        """
        closest_avail_trading_day = min(trade_calendar, key=lambda x: (x < given_time, abs(x - given_time)))
    else:
        closest_avail_trading_day = None
    return closest_avail_trading_day


def determine_latest_quote_date(stock_obj, date_attribute='date'):
    res = None
    latest_quote_obj = StockDailyQuote.objects(code=stock_obj.code).order_by(f'-{date_attribute}').first()
    if latest_quote_obj:
        res = latest_quote_obj[date_attribute]
    return res


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


def next_trading_day(trade_calendar, given_time=datetime.datetime.now()):
    t_day = min(trade_calendar, key=lambda x: (x <= given_time, abs(x - given_time)))
    return t_day


# def update_freshness_meta(obj, freshness_field, freshness_value):
#     if obj.data_freshness_meta:
#         data_freshness_meta = obj.data_freshness_meta
#     else:
#         data_freshness_meta = DataFreshnessMeta()
#     data_freshness_meta[freshness_field] = freshness_value
#     obj.data_freshness_meta = data_freshness_meta


# def read_freshness_meta(obj, freshness_field):
#     freshness_value = None
#     if obj.data_freshness_meta:
#         freshness_value = obj.data_freshness_meta[freshness_field]
#     return freshness_value


def is_trading_day(trade_calendar, given_time=datetime.datetime.now()):
    given_time = given_time.replace(hour=0, minute=0, second=0, microsecond=0)
    if given_time in trade_calendar:
        response = True
    else:
        response = False
    return response


def get_a_stock_market_trade_calendar():
    market = FinanceMarket.objects(name="ChinaAStock").first()
    trade_calendar = market.trade_calendar
    return trade_calendar


def convert_date_to_datetime(date):
    """
    the akshare trade calendar interface returns date object, but mongodb store as Datetime
    """
    dt = datetime.datetime.combine(date, datetime.time())
    return dt


def update_title_date_str(title_str, date):
    pattern = r"[0-9]{8}"
    date_str = date.strftime('%Y%m%d')
    res = re.search(pattern, title_str)
    if res:
        new_str = re.sub(pattern, date_str, title_str)
        return new_str
    else:
        new_str = title_str + ' ' + date_str
        return new_str

