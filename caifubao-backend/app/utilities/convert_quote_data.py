from mongoengine.errors import NotUniqueError
from app.lib.db_tool import mongoengine_tool
from app.utilities.progress_bar import progress_bar
from app.model.stock import StockIndex, IndividualStock, StockDailyQuote


def go_convert():
    mongoengine_tool.connect_to_db()
    # convert stock
    print("converting stock quote data")
    stock_list = IndividualStock.objects()
    prog_bar = progress_bar()
    list_len = stock_list.count()
    for i, stock_item in enumerate(stock_list):

        source_quote_list_len = stock_item.daily_quote.count()
        dest_quote_list_len = StockDailyQuote.objects(code=stock_item.code).count()
        if source_quote_list_len > 0 and source_quote_list_len != dest_quote_list_len:
            quote_list = stock_item.daily_quote
            for quote_item in quote_list:
                new_quote = StockDailyQuote()
                new_quote.stock = stock_item
                new_quote.code = stock_item.code
                new_quote.date = quote_item.date
                new_quote.open = quote_item.open
                new_quote.close = quote_item.close
                new_quote.previous_close = quote_item.previous_close
                new_quote.high = quote_item.high
                new_quote.low = quote_item.low
                new_quote.amplitude = quote_item.amplitude
                new_quote.change_rate = quote_item.change_rate
                new_quote.change_amount = quote_item.change_amount
                new_quote.turnover_rate = quote_item.turnover_rate
                # new_quote.fq_factor = quote_item.fq_factor
                new_quote.volume = quote_item.volume
                new_quote.trade_amount = quote_item.trade_amount
                new_quote.trade_status = quote_item.trade_status  # 1 - 正常交易  0 - 停牌
                new_quote.peTTM = quote_item.peTTM  # 滚动市盈率
                new_quote.pbMRQ = quote_item.pbMRQ  # 市净率
                new_quote.psTTM = quote_item.psTTM  # 滚动市销率
                new_quote.pcfNcfTTM = quote_item.pcfNcfTTM  # 滚动市现率
                new_quote.isST = quote_item.isST  # 1 - 被ST  0 - 否
                try:
                    new_quote.save()
                except NotUniqueError as e:
                    print(e)
        prog_bar(i, list_len)

    # convert index
    # stock_list = StockIndex.objects()
    # prog_bar = progress_bar()
    # list_len = stock_list.count()
    # print("converting index quote data")
    # for i, stock_item in enumerate(stock_list):
    #     quote_qs = stock_item.daily_quote
    #     if quote_qs:
    #         for quote_item in quote_qs:
    #             new_quote = StockDailyQuote()
    #             new_quote.stock = stock_item
    #             new_quote.code = stock_item.code
    #             new_quote.date = quote_item.date
    #             new_quote.open = quote_item.open
    #             new_quote.close = quote_item.close
    #             # new_quote.previous_close = quote_item.previous_close
    #             new_quote.high = quote_item.high
    #             new_quote.low = quote_item.low
    #             new_quote.amplitude = quote_item.amplitude
    #             new_quote.change_rate = quote_item.change_rate
    #             new_quote.change_amount = quote_item.change_amount
    #             new_quote.turnover_rate = quote_item.turnover_rate
    #             # new_quote.fq_factor = quote_item.fq_factor
    #             new_quote.volume = quote_item.volume
    #             new_quote.trade_amount = quote_item.trade_amount
    #             # new_quote.trade_status = quote_item.trade_status  # 1 - 正常交易  0 - 停牌
    #             # new_quote.peTTM = quote_item.peTTM  # 滚动市盈率
    #             # new_quote.pbMRQ = quote_item.pbMRQ  # 市净率
    #             # new_quote.psTTM = quote_item.psTTM  # 滚动市销率
    #             # new_quote.pcfNcfTTM = quote_item.pcfNcfTTM  # 滚动市现率
    #             # new_quote.isST = quote_item.isST  # 1 - 被ST  0 - 否
    #             new_quote.save()
    #     prog_bar(i, list_len)


if __name__ == "__main__":
    go_convert()
