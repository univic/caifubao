import logging


logger = logging.getLogger(__name__)


class TradeOperator(object):

    def __init__(self, strategy_name, portfolio_name):
        # get class name
        self.module_name = self.__class__.__name__
        self.strategy_name = strategy_name
        self.portfolio_name = portfolio_name
        logger.info(f'Module {self.module_name} is initializing')


# if __name__ == '__main__':
#     obj = TradeOperator()
