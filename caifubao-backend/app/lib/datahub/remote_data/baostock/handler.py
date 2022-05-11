import traceback
from app.model.stock import IndividualStock, DailyQuote
from app.utilities import performance_helper, trading_day_helper
from app.lib.datahub.remote_data.baostock import interface


def get_zh_a_stock_k_data_daily(code, start_date=None, end_date=None):
    status_code = "GOOD"
    status_msg = None
    try:
        stock_obj = IndividualStock.objects(code=code).first()
        local_daily_quote_list = []
        if stock_obj:
            if start_date and stock_obj.daily_quote:
                # prepare the df for incremental update
                local_daily_quote_list = stock_obj.daily_quote
                quote_df = interface.query_history_k_data(code, start_date=start_date, end_date=end_date)
            else:
                quote_df = interface.query_history_k_data(code)

            if not quote_df.empty:
                for i, row in quote_df.iterrows():
                    daily_quote = DailyQuote()
                    daily_quote.date = row['date']
                    daily_quote.open = row['open']
                    daily_quote.close = row['close']
                    daily_quote.high = row['high']
                    daily_quote.low = row['low']
                    daily_quote.volume = row['volume']
                    daily_quote.trade_amount = row['amount']
                    daily_quote.amplitude = row['high'] - row['low']
                    daily_quote.change_rate = row['pctChg']
                    DailyQuote.change_amount = row['close'] - row['preclose']
                    daily_quote.turnover_rate = row['turn']

                    daily_quote.peTTM = row['peTTM']
                    daily_quote.pbMRQ = row['pbMRQ']
                    daily_quote.psTTM = row['psTTM']
                    daily_quote.pcfNcfTTM = row['pcfNcfTTM']

                    daily_quote.trade_status = row['tradestatus']
                    daily_quote.isST = row['isST']
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
            status_msg = 'STOCK CODE CAN NOT BE FOUND IN LOCAL DB'
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
