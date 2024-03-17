
class BasicStrategy(object):
    code = None
    market_list = ["ChinaAStock"]
    tick_freq = ""                    # daily  | 60m  | 30m |  15m  |  5m
    stock_scope = "single"            # single | list | pattern | full
    stock_code_list = []
    stock_select_pattern = ""
    factor_list = []
    signal_list = []
    opportunity_scanner_list = []


class Strategy01(BasicStrategy):
    stock_scope = "single"            # single | list | pattern | full
    stock_code_list = ['sz000977']
    factor_list = [
        "FQ_FACTOR",
        "MA_10",
        "MA_20",
        "MA_120",
    ]
    factor_rule_list = ['*']               # * - calculate every factor for every stock
    signal_list = [
        "MA_10_UPCROSS_20",
        "HFQ_PRICE_ABOVE_MA_120",
    ]
    opportunity_scanner_list = []
