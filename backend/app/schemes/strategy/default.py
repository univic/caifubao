class BasicStrategy(object):
    code = None
    market = "ChinaAStock"
    tick_freq = ""  # daily  | 60m  | 30m |  15m  |  5m
    stock_scope = "single"  # single | list | pattern | full
    stock_code_list = []
    stock_select_pattern = ""
    factor_list = []
    signal_list = []
    opportunity_scanner_list = []


class Strategy01(BasicStrategy):
    # stock_scope = "single"            # single | list | pattern | full
    # stock_code_list = ['sz300082']
    stock_scope = "full"  # single | list | pattern | full
    factor_list = [
        "FQ_FACTOR",
        "MA_10",
        "MA_20",
        "MA_30",
        "MA_60",
        "MA_120",
    ]
    # factor_rule_list = ['*']               # * - calculate every factor for every stock
    signal_list = [
        "HFQ_PRICE_UPCROSS_MA_60",
    ]
    opportunity_scanner_list = []
