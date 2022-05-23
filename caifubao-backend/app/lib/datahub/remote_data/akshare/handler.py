import datetime
import traceback
import logging
from mongoengine.errors import ValidationError
from app.model.stock import StockIndex, IndividualStock
from app.model.stock import DailyQuote
from app.utilities.progress_bar import progress_bar
from app.utilities import trading_day_helper, performance_helper, stock_code_helper
import app.lib.datahub.remote_data.akshare.interface as interface


logger = logging.getLogger()


def get_a_stock_trade_date_hist():
    remote_data = interface.get_trade_date_hist()
    # convert to datetime
    r = remote_data['trade_date'].map(trading_day_helper.convert_date_to_datetime)
    # r = remote_data['trade_date'].map(lambda x: datetime.datetime.strptime(x, '%Y-%m-%d'))
    return list(r)


@performance_helper.func_performance_timer
def get_zh_stock_index_list():
    raw_df = interface.zh_stock_index_spot()
    index_list = raw_df[['代码', '名称']][raw_df['名称'] != '']
    return index_list


@performance_helper.func_performance_timer
def update_zh_stock_index_daily_spot():
    """
    根据每日盘后信息更新当日指数日线数据
    """
    status_code = "GOOD"
    status_msg = ""
    counter_dict = {
        "NO_UPD": 0,
        "UPD": 0,
        "OUT_DATED": 0,
        "NO_DATA": 0
    }

    market = StockIndex.objects().first().market
    logger.info(f'Stock Market {market.name} - Updating index daily quote with spot data')
    prog_bar = progress_bar()
    remote_data_df = interface.zh_stock_index_spot()
    remote_index_num = len(remote_data_df)
    stock_index_list = StockIndex.objects()
    if stock_index_list:

        closest_trading_day = trading_day_helper.determine_closest_trading_date(market.trade_calendar)
        for i, row in remote_data_df.iterrows():
            local_index_obj = StockIndex.objects(code=row['代码']).first()
            # if the local index data exists
            if local_index_obj:
                latest_quote_date = trading_day_helper.read_freshness_meta(local_index_obj, 'daily_quote')
                # if the freshness data exists
                if latest_quote_date:
                    date_diff = trading_day_helper.determine_trading_date_diff(
                        trade_calendar_list=market.trade_calendar,
                        trading_day_a=latest_quote_date,
                        trading_day_b=closest_trading_day
                    )
                    if date_diff == 1:
                        quote_data_list = local_index_obj.daily_quote
                        new_daily_quote_data = DailyQuote()
                        new_daily_quote_data.date = closest_trading_day
                        new_daily_quote_data.open = row['今开']
                        new_daily_quote_data.close = row['最新价']
                        new_daily_quote_data.high = row['最高']
                        new_daily_quote_data.low = row['最低']
                        new_daily_quote_data.volume = row['成交量']
                        trading_day_helper.update_freshness_meta(local_index_obj, 'daily_quote', closest_trading_day)
                        quote_data_list.append(new_daily_quote_data)
                        local_index_obj.daily_quote = quote_data_list
                        local_index_obj.save()
                        counter_dict["UPD"] += 1
                    elif date_diff == 0:
                        status_msg += f'{row["名称"]} - {row["代码"]} is up to date. '
                        counter_dict["NO_UPD"] += 1
                    else:
                        # status_code = "WARN"
                        status_msg += f'{row["名称"]} - {row["代码"]} local data was outdated. '
                        counter_dict["OUT_DATED"] += 1
                else:
                    # status_code = "WARN"
                    status_msg += f'{row["名称"]} - {row["代码"]} quote freshness data not found. '
                    counter_dict["NO_DATA"] += 1
            else:
                # status_code = "WARN"
                status_msg += f'{row["名称"]} - {row["代码"]} local data not found. '
                counter_dict["NO_DATA"] += 1
            prog_bar(i, remote_index_num)

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
        logger.info(f'Stock Market {market.name} - {status_brief}')
    else:
        logger.info(f'Stock Market {market.name} - Index daily quote update failed, {status_msg}')

    status = {
        'code': status_code,
        'message': status_msg,
    }
    return status


def get_zh_a_stock_index_quote_daily(code, incremental="false", start_date=None):
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
                most_recent_quote_date = trading_day_helper.read_freshness_meta(stock_index, 'daily_quote').date()
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


@performance_helper.func_performance_timer
def get_zh_individual_stock_list():
    raw_df = interface.stock_zh_a_spot()
    stock_list = raw_df[['代码', '名称']][raw_df['名称'] != '']
    return stock_list


def get_zh_a_stock_quote_daily(code, incremental="false"):
    status_code = "GOOD"
    status_msg = None
    try:
        stock_obj = IndividualStock.objects(code=code).first()
        local_daily_quote_list = []
        if stock_obj:
            if incremental == "true" and stock_obj.daily_quote:
                # prepare the df for incremental update
                local_daily_quote_list = stock_obj.daily_quote
                most_recent_quote_date = trading_day_helper.read_freshness_meta(stock_obj, 'daily_quote').date()
                quote_df = interface.stock_zh_a_hist(code)
            else:
                quote_df = interface.stock_zh_a_hist(code)
            if not quote_df.empty:
                for i, row in quote_df.iterrows():
                    daily_quote = DailyQuote()
                    daily_quote.date = row['日期']
                    daily_quote.open = row['开盘']
                    daily_quote.close = row['收盘']
                    daily_quote.high = row['最高']
                    daily_quote.low = row['最低']
                    daily_quote.volume = row['成交量']
                    daily_quote.trade_amount = row['成交额']
                    daily_quote.amplitude = row['振幅']
                    daily_quote.change_rate = row['涨跌幅']
                    daily_quote.change_amount = row['涨跌额']
                    daily_quote.turnover_rate = row['换手率']
                    local_daily_quote_list.append(daily_quote)
                stock_obj.daily_quote = local_daily_quote_list

                # update data freshness meta data
                date_of_quote = quote_df['date'].max()
                trading_day_helper.update_freshness_meta(stock_obj, 'daily_quote', date_of_quote)
                stock_obj.save()
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


@performance_helper.func_performance_timer
def update_zh_stock_spot(detail_msg=False):
    """
    根据每日盘后信息更新当日个股日线数据
    """
    # TODO: UPDATE DAILY QUOTE ACCORDING TO SPOT DATA
    status_code = "GOOD"
    status_msg = ""
    counter_dict = {
        "NO_UPD": 0,
        "UPD": 0,
        "OUT_DATED": 0,
        "NO_DATA": 0,
        "ERR": 0
    }
    market = IndividualStock.objects().first().market
    logger.info(f'Stock Market {market.name} - Updating stock daily quote with spot data')
    prog_bar = progress_bar()
    # Use eastmoney interface here
    raw_remote_data_df = interface.stock_zh_a_spot_em()
    remote_data_df = raw_remote_data_df.fillna(0)
    remote_index_num = len(remote_data_df)
    stock_list = IndividualStock.objects()
    if stock_list:
        closest_trading_day = trading_day_helper.determine_closest_trading_date(market.trade_calendar)
        for i, row in remote_data_df.iterrows():
            stock_code = stock_code_helper.add_market_prefix(row['代码'])
            local_stock_obj = IndividualStock.objects(code=stock_code).first()
            # if the local index data exists
            if local_stock_obj:
                latest_quote_date = trading_day_helper.read_freshness_meta(local_stock_obj, 'daily_quote')
                # if the freshness data exists
                if latest_quote_date:
                    date_diff = trading_day_helper.determine_trading_date_diff(
                        trade_calendar_list=market.trade_calendar,
                        trading_day_a=latest_quote_date,
                        trading_day_b=closest_trading_day
                    )
                    if date_diff == 1:
                        new_daily_quote_data = DailyQuote()
                        new_daily_quote_data.date = closest_trading_day
                        new_daily_quote_data.open = row['今开']
                        new_daily_quote_data.close = row['最新价']
                        new_daily_quote_data.high = row['最高']
                        new_daily_quote_data.low = row['最低']
                        new_daily_quote_data.change_rate = row['涨跌幅']
                        new_daily_quote_data.change_amount = row['涨跌额']
                        new_daily_quote_data.low = row['最低']
                        new_daily_quote_data.volume = row['成交量']
                        new_daily_quote_data.trade_amount = row['成交额']
                        new_daily_quote_data.previous_close = row['昨收']
                        new_daily_quote_data.amplitude = row['振幅']
                        new_daily_quote_data.turnover_rate = row['换手率']
                        new_daily_quote_data.peTTM = row['市盈率-动态']
                        new_daily_quote_data.psTTM = row['市净率']
                        trading_day_helper.update_freshness_meta(local_stock_obj, 'daily_quote', closest_trading_day)
                        quote_data_list = local_stock_obj.daily_quote
                        quote_data_list.append(new_daily_quote_data)
                        local_stock_obj.daily_quote = quote_data_list
                        try:
                            local_stock_obj.save()
                            counter_dict["UPD"] += 1
                        except ValidationError as e:
                            counter_dict["ERR"] += 1
                            status_msg += f'{row["名称"]} - {row["代码"]} encountered error'
                            print(f"validation error on {local_stock_obj.code}, {e}")

                    elif date_diff == 0:
                        status_msg += f'{row["名称"]} - {row["代码"]} is up to date. '
                        counter_dict["NO_UPD"] += 1
                    else:
                        status_code = "WARN"
                        status_msg += f'{row["名称"]} - {row["代码"]} local data was outdated. '
                        counter_dict["OUT_DATED"] += 1
                else:
                    status_code = "WARN"
                    status_msg += f'{row["名称"]} - {row["代码"]} quote freshness data not found. '
                    counter_dict["NO_DATA"] += 1
            else:
                status_code = "WARN"
                status_msg += f'{row["名称"]} - {row["代码"]} local data not found. '
                counter_dict["NO_DATA"] += 1
            prog_bar(i, remote_index_num)

    else:
        status_code = 'FAIL'
        status_msg = 'Local stock index data not found.'
    if status_code != 'FAIL':
        status_brief = f"Stock quote data update result: " \
                     f"{counter_dict['UPD']} updated, " \
                     f"{counter_dict['NO_UPD']} was up to date already," \
                     f"{counter_dict['OUT_DATED']} outdated (did not update), " \
                     f"{counter_dict['NO_DATA']} no local data, " \
                     f"{counter_dict['ERR']} encountered error "
        #
        if detail_msg:
            status_msg = status_brief + status_msg
        else:
            status_msg = status_brief
        logger.info(f'Stock Market {market.name} - {status_msg}')
    else:
        logger.info(f'Stock Market {market.name} - Index daily quote update failed, {status_msg}')

    status = {
        'code': status_code,
        'message': status_msg,
    }
    return status
