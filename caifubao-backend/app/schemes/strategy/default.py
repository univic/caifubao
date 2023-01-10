
class BasicStrategy(object):
    code = None
    market_name = "A股"
    stock_scope = "single"            # single | list | pattern | full
    stock_code_list = []
    stock_select_pattern = ""
    stock_factor_list = []
    signal_list = []
    opportunity_scanner_list = []


class Strategy01(BasicStrategy):
    stock_scope = "single"            # single | list | pattern | full
    stock_code_list = ['sz000977']
    stock_factor_list = [
        "FQ_FACTOR",
        "MA_10",
        "MA_20",
        "MA_120",
    ]
    signal_list = [
        "MA_10_UPCROSS_20",
        "HFQ_PRICE_ABOVE_MA_120",
    ]
    opportunity_scanner_list = []
