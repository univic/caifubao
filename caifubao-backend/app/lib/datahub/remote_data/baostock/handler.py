import time
import traceback
from app.model.stock import IndividualStock, StockDailyQuote
from app.utilities import trading_day_helper


def get_zh_a_stock_k_data_daily(code, start_date=None, end_date=None):
    status_code = "GOOD"
    status_msg = None
    try:
        stock_obj = IndividualStock.objects(code=code).only('code', 'data_freshness_meta').first()
        most_recent_quote_date = trading_day_helper.read_freshness_meta(stock_obj, 'daily_quote')
        if stock_obj:
            if start_date and most_recent_quote_date:
                # prepare the df for incremental update
                quote_df = interface.get_zh_a_stock_hist_k_data(code, start_date=start_date, end_date=end_date)
            else:
                quote_df = interface.get_zh_a_stock_hist_k_data(code)

            if not quote_df.empty:
                for i, raw_row in quote_df.iterrows():
                    # replace empty cells
                    row = raw_row.replace('', 0)

                    daily_quote = StockDailyQuote()
                    daily_quote.code = stock_obj.code
                    daily_quote.stock = stock_obj
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
                    daily_quote.change_amount = daily_quote.close - daily_quote.previous_close
                    daily_quote.turnover_rate = float(row['turn'])

                    daily_quote.peTTM = float(row['peTTM'])
                    daily_quote.pbMRQ = float(row['pbMRQ'])
                    daily_quote.psTTM = float(row['psTTM'])
                    daily_quote.pcfNcfTTM = float(row['pcfNcfTTM'])

                    daily_quote.trade_status = int(row['tradestatus'])
                    daily_quote.isST = int(row['isST'])
                    daily_quote.save()

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
