import datetime


def determine_closest_trading_date(trade_calendar, given_time=datetime.datetime.now()):

    closest_avail_trading_day = min(trade_calendar, key=lambda x: (x > given_time, abs(x - given_time)))
    return closest_avail_trading_day


def determine_latest_quote_date(quote_list, date_attribute):
    latest_quote_date = max(quote_list, key=lambda x: x[date_attribute])
    return latest_quote_date


def determine_date_diff_with_latest_quote(trade_calendar_list, stock_index_obj):
    closest_avail_trading_day = determine_closest_trading_date(trade_calendar_list)
    latest_quote_date = stock_index_obj.data_freshness_meta.daily_quote
    trade_day_list = sorted(trade_calendar_list, reverse=True)
    latest_quote_date_index = trade_day_list.index(latest_quote_date)
    closest_avail_trading_day_index = trade_day_list.index(closest_avail_trading_day)
    date_diff = abs(closest_avail_trading_day_index - latest_quote_date_index)
    return date_diff


def determine_pervious_trading_day(trade_calendar_list, given_date):
    trade_day_list = sorted(trade_calendar_list, reverse=True)
    given_date_index = trade_day_list.index(given_date)
    return trade_day_list[given_date_index + 1]
