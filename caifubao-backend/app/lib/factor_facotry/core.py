from app.model.stock import StockIndex, IndividualStock


class FactorProcesser(object):

    def __init__(self):
        self.obj = None
        self.obj_list = []

    def dispatcher(self):
        pass

    def generate_zh_a_stock_factors(self):
        pass

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
