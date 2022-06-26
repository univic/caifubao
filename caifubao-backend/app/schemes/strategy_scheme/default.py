
class BasicStrategyScheme(object):

    def __init__(self):
        self.name = None
        self.code = None
        self.market_name = "A股"
        stock_scope = "single"            # single | list | pattern | full
        stock_list = []
        stock_select_pattern = ""
        factor_list = [
            "FQ_FACTOR"
            "MA_10",
        ]
        signal_list = []
        opportunity_seeker_list_long = []
        opportunity_seeker_list_short = []

