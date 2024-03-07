import logging
from app.lib import GeneralWorker
from app.lib.opportunity_seeker import processors


logger = logging.getLogger(__name__)


class OpportunitySeeker(GeneralWorker):
    def __init__(self, strategy_director, portfolio_manager, scenario):
        self.module_name = self.__class__.__name__
        meta_type = 'opportunity'
        super().__init__(strategy_director, portfolio_manager, scenario)