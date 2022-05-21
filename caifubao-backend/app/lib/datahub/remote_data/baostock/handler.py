import time
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
                for i, raw_row in quote_df.iterrows():
                    row = raw_row.replace('', 0)
                    daily_quote = DailyQuote()
                    daily_quote.date = row['date']
                    daily_quote.open = float(row['open'])
                    daily_quote.close = float(row['close'])
                    daily_quote.previous_close = float(row['preclose'])
                    daily_quote.high = float(row['high'])
                    daily_quote.low = float(row['low'])
                    daily_quote.volume = int(row['volume'])
                    daily_quote.trade_amount = float(row['amount'])
                    daily_quote.amplitude = daily_quote.high - daily_quote.low
                    daily_quote.change_rate = float(row['pctChg'])
                    DailyQuote.change_amount = daily_quote.close - daily_quote.previous_close
                    daily_quote.turnover_rate = float(row['turn'])

                    daily_quote.peTTM = float(row['peTTM'])
                    daily_quote.pbMRQ = float(row['pbMRQ'])
                    daily_quote.psTTM = float(row['psTTM'])
                    daily_quote.pcfNcfTTM = float(row['pcfNcfTTM'])

                    daily_quote.trade_status = int(row['tradestatus'])
                    daily_quote.isST = int(row['isST'])
                    local_daily_quote_list.append(daily_quote)
                stock_obj.daily_quote = local_daily_quote_list

                # update data freshness meta data
                date_of_quote = quote_df['date'].max()
                trading_day_helper.update_freshness_meta(stock_obj, 'daily_quote', date_of_quote)
                stock_obj.save()
            else:
                status_code = 'FAIL'
                status_msg = 'No available data for update'
                time.sleep(0.5)  # reduce the query frequency
        else:
            status_code = 'FAIL'
            status_msg = 'STOCK CODE CAN NOT BE FOUND IN LOCAL DB'
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
