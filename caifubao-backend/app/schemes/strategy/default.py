
class BasicStrategy(object):
    code = None
    market_name = "A股"
    stock_scope = "single"            # single | list | pattern | full
    stock_code_list = []
    stock_select_pattern = ""
    stock_factor_list = []
    signal_list = []
    opportunity_seeker_list_long = []
    opportunity_seeker_list_short = []


class Strategy01(BasicStrategy):
    stock_scope = "single"            # single | list | pattern | full
    stock_code_list = ['sz000977']
    stock_factor_list = [
        "FQ_FACTOR"
        "MA_10",
    ]
    signal_list = []
    opportunity_seeker_list_long = []
    opportunity_seeker_list_short = []
