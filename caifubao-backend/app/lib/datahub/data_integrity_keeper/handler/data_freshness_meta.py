from app.utilities.progress_bar import progress_bar
from app.model.stock import StockIndex, IndividualStock, StockDailyQuote
from app.utilities import trading_day_helper, freshness_meta_helper


def calibrate_daily_quote_meta():
    mongoengine_tool.connect_to_db()

    # # calibrate stock index data freshness meta
    # stock_list = StockIndex.objects().exclude('daily_quote')
    # prog_bar = progress_bar()
    # list_len = stock_list.count()
    # print("calibrating index data freshness meta")
    # for i, stock_item in enumerate(stock_list):
    #     latest_quote = StockDailyQuote.objects(code=stock_item.code).order_by('-date').first()
    #     latest_quote_date = latest_quote.date
    #     curr_daily_quote_meta = trading_day_helper.read_freshness_meta(stock_item, 'daily_quote')
    #     if latest_quote_date != curr_daily_quote_meta:
    #         print(f"Updating {stock_item.code}")
    #         trading_day_helper.update_freshness_meta(stock_item, 'daily_quote', latest_quote_date)
    #         stock_item.daily_quote = None
    #         stock_item.save()
    #     else:
    #         print(f"Skipping {stock_item.code}")
    #
    #     prog_bar(i, list_len)

    # calibrate stock data freshness meta
    stock_list = IndividualStock.objects().exclude('daily_quote')
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

        if latest_quote:
            pass
            # latest_quote_date = latest_quote.date
            #
            # if latest_quote_date != curr_daily_quote_meta:
            #     print(f"Updating {stock_item.code}")
            #     trading_day_helper.update_freshness_meta(stock_item, 'daily_quote', latest_quote_date)
            #     stock_item.daily_quote = None
            #     stock_item.save()
            # else:
            #     print(f"Skipping {stock_item.code}")
        else:
            if curr_daily_quote_meta:
                print(f"Removing invalid data freshness meta for {stock_item.code} - {stock_item.name}")
                # trading_day_helper.update_freshness_meta(stock_item, 'daily_quote', None)
                freshness_meta_helper.upsert_freshness_meta(code=stock_item.code,
                                                            object_type=stock_item.object_type,
                                                            meta_type='quote',
                                                            meta_name='daily_quote',
                                                            dt=None)
                stock_item.save()
            else:
                print(f"Skipping {stock_item.code} - {stock_item.name}")
        prog_bar(i, list_len)


def del_daily_quote():
    """
    a temporary method to delete quote data that inside stock document
    :return:
    """
    mongoengine_tool.connect_to_db()
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
