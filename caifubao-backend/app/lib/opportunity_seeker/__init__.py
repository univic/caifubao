import logging
from app.lib import GeneralWorker
from app.lib.opportunity_seeker import processors


logger = logging.getLogger(__name__)


class OpportunitySeeker(GeneralWorker):
    def __init__(self, stock, processor_name_list):
        module_name = 'OpportunitySeeker'
        meta_type = 'opportunity'
        super().__init__(stock=stock,
                         module_name=module_name,
                         meta_type=meta_type,
                         processor_registry=processors,
                         processor_name_list=processor_name_list
                         )
