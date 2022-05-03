import time
import datetime
import logging
from app.model.stock import StockIndex, DataFreshnessMeta
from app.model.stock import DailyQuote
from app.utilities import trading_day_helper
import app.lib.datahub.remote_data.akshare.interface as interface


logger = logging.getLogger()


def get_a_stock_trade_date_hist():
    remote_data = interface.get_trade_date_hist()
    return remote_data


def get_zh_stock_index_list():
    raw_df = interface.zh_stock_index_spot()
    index_list = raw_df[['代码', '名称']][raw_df['名称'] != '']
    return index_list


def get_zh_stock_index_daily_spot():
    """
    根据每日盘后信息更新当日日线数据
    """
    status = {
        'code': 'GOOD',
        'message': None,
    }
    remote_data_df = interface.zh_stock_index_spot()
    stock_index_list = StockIndex.objects()
    if stock_index_list:
        for i, row in remote_data_df.iterrows():
            local_stock_index_object = StockIndex.objects(code=row['代码']).first()
            market = local_stock_index_object.market
            new_daily_quote_data = DailyQuote()
            new_daily_quote_data.date = trading_day_helper.determine_closest_trading_date(
                trade_calendar=market.trade_calendar,
                ignore_trading_hour=True)

    else:
        status = {
            'code': 'FAIL',
            'message': 'Local stock index data not found',
        }
    return status


def get_zh_individual_stock_list():
    df = interface.stock_zh_a_spot_em()
    stock_list = None


def get_zh_a_stock_index_quote_daily(code, incremental=False):
    status = {
        'code': 'GOOD',
        'message': None,
    }
    stock_index = StockIndex.objects(code=code).first()
    res_df = interface.stock_zh_index_daily(code)
    local_daily_quote_list = []
    if stock_index:
        if incremental and stock_index.daily_quote:
            # prepare the df for incremental update
            local_daily_quote_list = stock_index.daily_quote
            most_recent_quote = max(local_daily_quote_list, key=lambda x: x.date)
            quote_df = res_df[res_df.date > most_recent_quote.date].sort_index(axis=1, ascending=False)
        else:
            quote_df = res_df

        for i, row in quote_df.iterrows():
            daily_quote = DailyQuote()
            daily_quote.date = row['date']
            daily_quote.open = row['open']
            daily_quote.close = row['close']
            daily_quote.high = row['high']
            daily_quote.low = row['low']
            daily_quote.volume = row['volume']
            local_daily_quote_list.append(daily_quote)
        stock_index.daily_quote = local_daily_quote_list

        # update data freshness meta data
        if stock_index.data_freshness_meta:
            data_freshness_meta = stock_index.data_freshness_meta
        else:
            data_freshness_meta = DataFreshnessMeta()
        data_freshness_meta.daily_quote = quote_df['date'].max()
        stock_index.data_freshness_meta = data_freshness_meta
        stock_index.save()
    else:
        status = {
            'code': 'FAIL',
            'message': 'INDEX CODE CAN NOT BE FOUND IN LOCAL DB',
        }
    # time.sleep(0.5)    # reduce the query frequency
    return status

