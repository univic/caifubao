import pandas as pd
from app.model.stock import BasicStock, StockDailyQuote
from app.lib.factor_facotry.processors import fq_factor


class FactorProcesser(object):

    def __init__(self):
        self.stock_code = None
        self.stock_obj = None
        self.quote_df = []
        self.processer_list = []

    def dispatcher(self):
        self.generate_process_plan()
        self.get_quote_df()
        p = fq_factor.FQFactorProcessor(self)
        p.perform_factor_calc()

    def generate_process_plan(self):
        pass

    def generate_factors(self, code):
        self.stock_obj = BasicStock.objects(code=code).first()
        self.stock_code = code
        self.dispatcher()

    def get_quote_df(self):
        quote_qs = StockDailyQuote.objects(code=self.stock_code)
        # convert to df
        quote_json = quote_qs.as_pymongo()
        self.quote_df = pd.DataFrame(quote_json)
        self.quote_df.set_index("date", inplace=True)

    def get_processer_list(self):
        pass

    def before_exec(self):
        pass

    def after_exec_pass(self):
        pass


if __name__ == '__main__':
    from app.lib.db_tool import mongoengine_tool
    mongoengine_tool.connect_to_db()
    obj = FactorProcesser()
    obj.generate_factors("sh601166")
