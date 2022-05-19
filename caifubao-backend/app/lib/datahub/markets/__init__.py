from app.lib.datahub.markets.china_a_stock import ChinaAStock


def initialize_markets():
    a_stock_market_obj = ChinaAStock()
    a_stock_market_obj.initialize()

