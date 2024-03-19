from app.lib.db_watcher.mongoengine_tool import db_watcher
from app.utilities.progress_bar import progress_bar
from app.model.stock import StockIndex, BasicStock, StockDailyQuote
from app.utilities import trading_day_helper, freshness_meta_helper


def calibrate_daily_quote_meta():
    counter_dict = {
        'GOOD': 0,
        "DISAGREE": 0,
        "INVALID": 0,
        "NO_DATA": 0,
    }
    # Establish DB Connection
    db_watcher.initialize()
    db_watcher.connect_to_db()

    # calibrate stock data freshness meta
    stock_list = BasicStock.objects()
    prog_bar = progress_bar()
    list_len = stock_list.count()
    print("calibrating index data freshness meta")
    for i, stock_item in enumerate(stock_list):
        latest_quote = StockDailyQuote.objects(code=stock_item.code).order_by('-date').first()
        # curr_daily_quote_meta = trading_day_helper.read_freshness_meta(stock_item, 'daily_quote')
        curr_daily_quote_meta = freshness_meta_helper.read_freshness_meta(code=stock_item.code,
                                                                          object_type=stock_item.object_type,
                                                                          meta_type='quote',
                                                                          meta_name='daily_quote')
        prog_bar_msg = ""

        if latest_quote:
            latest_quote_date = latest_quote.date

            if latest_quote_date != curr_daily_quote_meta:
                counter_flag = "DISAGREE"
                prog_bar_msg = f"Updating {stock_item.code} - {stock_item.name} freshness meta to {latest_quote_date}"
                freshness_meta_helper.upsert_freshness_meta(code=stock_item.code,
                                                            object_type=stock_item.object_type,
                                                            meta_type='quote',
                                                            meta_name='daily_quote',
                                                            dt=latest_quote_date)
            else:
                counter_flag = "GOOD"
                prog_bar_msg = f"{stock_item.code} - {stock_item.name} freshness meta check OK"
        else:
            if curr_daily_quote_meta:
                counter_flag = "INVALID"
                prog_bar_msg = f"Removing invalid data freshness meta for {stock_item.code} - {stock_item.name}"
                freshness_meta_helper.upsert_freshness_meta(code=stock_item.code,
                                                            object_type=stock_item.object_type,
                                                            meta_type='quote',
                                                            meta_name='daily_quote',
                                                            dt=None)
            else:
                counter_flag = "NO_DATA"
                prog_bar_msg = f"No quote data or freshness meta found for {stock_item.code} - {stock_item.name}"
        counter_dict[counter_flag] += 1
        prog_bar(i, list_len, prog_bar_msg)
    print("Daily quote freshness meta calibration completed, the result is as follows:")
    print(counter_dict)


def del_daily_quote():
    """
    a temporary method to delete quote data that inside stock document
    :return:
    """
    print("removing quote data from stock object")
    obj_list = [StockIndex, IndividualStock]
    for obj in obj_list:
        item_list = obj.objects()
        list_len = item_list.count()
        prog_bar = progress_bar()
        for i, item in enumerate(item_list):
            item.daily_quote = None
            item.save()
            prog_bar(i, list_len)
    mongoengine_tool.disconnect_from_db()


if __name__ == '__main__':
    calibrate_daily_quote_meta()
