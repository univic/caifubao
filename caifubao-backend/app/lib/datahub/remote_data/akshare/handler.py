import time
import traceback
import logging
from app.model.stock import StockIndex
from app.model.stock import DailyQuote
from app.utilities import trading_day_helper, performance_helper
import app.lib.datahub.remote_data.akshare.interface as interface


logger = logging.getLogger()


def get_a_stock_trade_date_hist():
    remote_data = interface.get_trade_date_hist()
    return remote_data


@performance_helper.func_performance_timer
def get_zh_stock_index_list():
    raw_df = interface.zh_stock_index_spot()
    index_list = raw_df[['代码', '名称']][raw_df['名称'] != '']
    return index_list


@performance_helper.func_performance_timer
def update_zh_stock_index_daily_spot():
    """
    根据每日盘后信息更新当日日线数据
    """
    status_code = "GOOD"
    status_msg = ""
    counter_dict = {
        "NO_UPD": 0,
        "UPD": 0,
        "OUT_DATED": 0,
        "NO_DATA": 0
    }
    remote_data_df = interface.zh_stock_index_spot()
    stock_index_list = StockIndex.objects()
    if stock_index_list:
        market = StockIndex.objects().first().market
        closest_avail_trading_day = trading_day_helper.determine_closest_trading_date(market.trade_calendar)
        for i, row in remote_data_df.iterrows():
            local_index_obj = StockIndex.objects(code=row['代码']).first()
            if local_index_obj:
                latest_quote_date = trading_day_helper.read_freshness_meta(local_index_obj, 'daily_quote')
                date_diff = trading_day_helper.determine_trading_date_diff(
                    trade_calendar_list=market.trade_calendar,
                    trading_day_a=latest_quote_date,
                    trading_day_b=closest_avail_trading_day
                )
                if date_diff == 1:
                    new_daily_quote_data = DailyQuote()
                    new_daily_quote_data.date = closest_avail_trading_day
                    new_daily_quote_data.open = row['今开']
                    new_daily_quote_data.close = row['最新价']
                    new_daily_quote_data.high = row['最高']
                    new_daily_quote_data.low = row['最低']
                    new_daily_quote_data.volume = row['成交量']
                    trading_day_helper.update_freshness_meta(local_index_obj, 'daily_quote', closest_avail_trading_day)
                    counter_dict["UPD"] += 1
                elif date_diff == 0:
                    status_msg += f'{row["名称"]} - {row["代码"]} is up to date. '
                    counter_dict["NO_UPD"] += 1
                else:
                    status_code = "WARN"
                    status_msg += f'{row["名称"]} - {row["代码"]} local data was outdated. '
                    counter_dict["OUT_DATED"] += 1
            else:
                status_code = "WARN"
                status_msg += f'{row["名称"]} - {row["代码"]} local data not found. '
                counter_dict["NO_DATA"] += 1

    else:
        status_code = 'FAIL'
        status_msg = 'Local stock index data not found.'
    if status_code != 'FAIL':
        status_brief = f"Index spot info update result: " \
                     f"{counter_dict['UPD']} updated, " \
                     f"{counter_dict['NO_UPD']} was up to date already," \
                     f"{counter_dict['OUT_DATED']} outdated (did not update), " \
                     f"{counter_dict['NO_DATA']} no local data "
        status_msg = status_brief + status_msg
    status = {
        'code': status_code,
        'message': status_msg,
    }
    return status


def get_zh_individual_stock_list():
    start = time.process_time()
    raw_df = interface.stock_zh_a_spot()
    stock_list = raw_df[['代码', '名称']][raw_df['名称'] != '']
    end = time.process_time()
    t = end - start
    logger.info(f'Elapsed time during the handler run: {t:.2f} seconds')
    return stock_list


def get_zh_a_stock_index_quote_daily(code, incremental="false"):
    status_code = "GOOD"
    status_msg = None
    try:
        res_df = interface.stock_zh_index_daily(code)
        stock_index = StockIndex.objects(code=code).first()
        local_daily_quote_list = []
        if stock_index:
            if incremental == "true" and stock_index.daily_quote:
                # prepare the df for incremental update
                local_daily_quote_list = stock_index.daily_quote
                most_recent_quote = trading_day_helper.read_freshness_meta(stock_index, 'daily_quote')
                most_recent_quote_date = most_recent_quote.date.date()
                quote_df = res_df[res_df.date > most_recent_quote_date].sort_index(axis=1, ascending=False)
            else:
                quote_df = res_df
            if not quote_df.empty:
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
                date_of_quote = quote_df['date'].max()
                trading_day_helper.update_freshness_meta(stock_index, 'daily_quote', date_of_quote)
                stock_index.save()
            else:
                status_code = 'FAIL'
                status_msg = 'No available data for update'
        else:
            status_code = 'FAIL'
            status_msg = 'INDEX CODE CAN NOT BE FOUND IN LOCAL DB'
        # time.sleep(0.5)    # reduce the query frequency
    except KeyError:
        status_code = 'FAIL'
        status_msg = 'the interface did not return valid dataframe, possibly due to no quote data'
    except Exception as e:
        status_code = 'FAIL'
        status_msg = ';'.join(traceback.format_exception(e))
    status = {
        'code': status_code,
        'message': status_msg,
    }
    return status
