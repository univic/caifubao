from app.model.stock import StockIndex, IndividualStock
# from app.lib.factor_facotry.quote_factor import


class FactorProcesser(object):

    def __init__(self):
        self.obj = None
        self.obj_list = []
        self.processer_list = []

    def dispatcher(self):
        for item in self.obj_list:
            for func in self.processer_list:
                func(item)

    def generate_zh_a_stock_factors(self):
        self.obj = IndividualStock
        self.dispatcher()

    def generate_zh_a_index_factors(self):
        pass

    def get_obj_list(self):
        self.obj_list = self.obj.objects()

    def get_processer_list(self):
        pass

    def before_exec(self):
        pass

    def after_exec_pass(self):
        pass
