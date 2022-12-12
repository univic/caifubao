import logging
from app.lib import GeneralFactory
from app.lib.opportunity_seeker import processors


logger = logging.getLogger()


class OpportunitySeeker(GeneralFactory):
    def __init__(self, stock, processor_name_list):
        module_name = 'OpportunitySeeker'
        meta_type = 'opportunity'
        super().__init__(stock=stock,
                         module_name=module_name,
                         meta_type=meta_type,
                         processor_registry=processors,
                         processor_name_list=processor_name_list
                         )
