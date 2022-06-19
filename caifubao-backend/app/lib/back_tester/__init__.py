from app.lib.db_tool import mongoengine_tool


class BackTester(object):

    def __init__(self):
        self.scenario = None
        self.trading_day_list = []
        self.stock_list = []

    def before_back_test(self):
        mongoengine_tool.connect_to_db()

    def after_back_test(self):
        mongoengine_tool.disconnect_from_db()

    def load_test_scenario(self):
        pass

    def setup_backtest(self, scenario):
        self.scenario = scenario
        self.load_test_scenario()

    def exec_test_scenario(self):
        pass

    def get_stock_list(self):
        pass

    def get_quote_list(self):
        pass

    def perform_daily_analysis(self):
        self.generate_factors()
        self.generate_signals()
        self.generate_trade_plan()

    def generate_factors(self):
        pass

    def generate_signals(self):
        pass

    def generate_trade_plan(self):
        pass

    def exec_trade_plan(self):
        pass

    def generate_backtest_report(self):
        pass
