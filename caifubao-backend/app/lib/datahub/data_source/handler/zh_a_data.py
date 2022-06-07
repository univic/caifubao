import datetime
import logging


from app.lib.datahub.data_source import interface
from app.utilities import trading_day_helper, performance_helper, stock_code_helper


logger = logging.getLogger()


def get_a_stock_trade_date_hist():
    remote_data = interface.akshare.get_trade_date_hist()
    # convert to datetime
    r = remote_data['trade_date'].map(trading_day_helper.convert_date_to_datetime)
    return list(r)


@performance_helper.func_performance_timer
def get_zh_a_stock_index_spot():
    name_mapping = {
        '名称': 'name',
        '代码': 'code',
        '今开': 'open',
        '最新价': 'close',
        '最高': 'high',
        '最低': 'low',
        '成交量': 'volume'
    }
    raw_df = interface.akshare.zh_stock_index_spot()
    raw_df.rename(name_mapping, axis=1, inplace=True)
    raw_df.fillna(0, inplace=True)
    df = raw_df.loc[raw_df['name'] != '']
    return df


@performance_helper.func_performance_timer
def get_zh_a_stock_spot():
    name_mapping = {
        '名称': 'name',
        # '代码': 'code',        # will carry out code convert later
        '今开': 'open',
        '昨收': 'previous_close',
        '最新价': 'close',
        '涨跌幅': 'change_rate',
        '涨跌额': 'change_amount',
        '最高': 'high',
        '最低': 'low',
        '成交量': 'volume',
        '成交额': 'trade_amount',
        '振幅': 'amplitude',
        '换手率': 'turnover_rate',
        '市盈率-动态': 'peTTM',
        '市净率': 'pbMRQ'
    }
    raw_df = interface.akshare.stock_zh_a_spot_em()
    df = raw_df[raw_df['名称'] != '']
    df.fillna(0, inplace=True)
    df.rename(name_mapping, axis=1, inplace=True)        # rename column
    df['code'] = df['代码'].apply(stock_code_helper.add_market_prefix)
    return df


def get_zh_a_index_hist_daily_quote(code, start_date=None, incremental=True):
    raw_df = interface.akshare.stock_zh_index_daily(code)
    raw_df.fillna(0, inplace=True)
    if incremental and start_date:
        start_date = datetime.datetime.strptime(start_date, '%Y-%m-%d').date()
        df = raw_df[raw_df.date > start_date].sort_index(axis=1, ascending=False)
    else:
        df = raw_df
    return df


def get_zh_a_stock_hist_daily_quote(code, start_date=None):
    name_mapping = {
        'preclose': 'previous_close',
        'pcgChg': 'change_rate',
        'amount': 'trade_amount',
        'turn': 'turnover_rate',
        'tradestatus': 'trade_status',
    }
    float_columns = ["open", "high", "low", "close", "preclose", "volume", "amount", "turn", "pctChg", "peTTM", "pbMRQ",
                     "psTTM", "pcfNcfTTM"]
    int_columns = ["adjustflag", "tradestatus", "isST"]
    raw_df = interface.baostock.get_zh_a_stock_hist_k_data(code, start_date)
    raw_df.replace('', 0, inplace=True)  # replace empty cells
    raw_df.fillna(0, inplace=True)
    # perform type convert
    raw_df[float_columns] = raw_df[float_columns].astype('float')
    raw_df[int_columns] = raw_df[int_columns].astype('int')
    raw_df.rename(name_mapping, axis=1, inplace=True)  # rename column
    raw_df['code'] = raw_df['code'].apply(lambda x: x.replace(".", ""))           # replace the dot in the stock code
    return raw_df

# def get_zh_a_stock_quote_daily(code, incremental="false"):
#     """
#     INOP: preclose data not available, use baostock data instead
#     :param code:
#     :param incremental:
#     :return:
#     """
#     status_code = "GOOD"
#     status_msg = None
#     try:
#         stock_obj = IndividualStock.objects(code=code).first()
#         most_recent_quote_date = trading_day_helper.read_freshness_meta(stock_obj, 'daily_quote').date()
#         if stock_obj:
#             if incremental == "true" and most_recent_quote_date:
#                 # prepare the df for incremental update
#                 quote_df = interface.akshare.stock_zh_a_hist(code)
#             else:
#                 quote_df = interface.akshare.stock_zh_a_hist(code)
#             if not quote_df.empty:
#                 for i, row in quote_df.iterrows():
#                     daily_quote = StockDailyQuote()
#                     daily_quote.code = stock_obj.code
#                     daily_quote.stock = stock_obj
#                     daily_quote.date = row['日期']
#                     daily_quote.open = row['开盘']
#                     daily_quote.close = row['收盘']
#                     daily_quote.high = row['最高']
#                     daily_quote.low = row['最低']
#                     daily_quote.volume = row['成交量']
#                     daily_quote.trade_amount = row['成交额']
#                     daily_quote.amplitude = row['振幅']
#                     daily_quote.change_rate = row['涨跌幅']
#                     daily_quote.change_amount = row['涨跌额']
#                     daily_quote.turnover_rate = row['换手率']
#                     daily_quote.save()
#
#                 # update data freshness meta data
#                 date_of_quote = quote_df['date'].max()
#                 trading_day_helper.update_freshness_meta(stock_obj, 'daily_quote', date_of_quote)
#                 stock_obj.save()
#             else:
#                 status_code = 'FAIL'
#                 status_msg = 'No available data for update'
#         else:
#             status_code = 'FAIL'
#             status_msg = 'INDEX CODE CAN NOT BE FOUND IN LOCAL DB'
#         # time.sleep(0.5)    # reduce the query frequency
#     except KeyError:
#         status_code = 'FAIL'
#         status_msg = 'the interface did not return valid dataframe, possibly due to no quote data'
#     except Exception as e:
#         status_code = 'FAIL'
#         status_msg = ';'.join(traceback.format_exception(e))
#     status = {
#         'code': status_code,
#         'message': status_msg,
#     }
#     return status


if __name__ == '__main__':
    interface.baostock.establish_baostock_conn()
    o = get_zh_a_stock_hist_daily_quote(code="sh600284")
    print(o)
