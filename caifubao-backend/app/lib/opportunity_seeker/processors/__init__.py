from app.lib.opportunity_seeker.processors import moving_average

registry = {
    # "MA_LONG_10_UPCROSS_20_WHILE_120_UPWARD": {
    #     'processor_object': moving_average.MACrossSignalProcessor,
    #     'handler': 'run',
    #     'kwargs': {
    #         'PRI_MA': "MA_10",        # Primary MA line
    #         'REF_MA': "MA_20",        # MA line for reference
    #         'CROSS_TYPE': 'UP',   # MA lines can up or down cross each other,
    #     },
    #     'factor_dependency': ['MA_10', 'MA_20'],
    #     'quote_dependent': False,
    # }
}