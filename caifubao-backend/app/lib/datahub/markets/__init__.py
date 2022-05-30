from app.lib.datahub.markets.china_a_stock import ChinaAStock

zh_a_stock_market = ChinaAStock()


def initialize_markets():

    zh_a_stock_market.initialize()
