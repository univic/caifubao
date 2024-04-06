from app.lib.opportunity_seeker.processors import moving_average

registry = {
    "LONG_MA10_UPCROSS_MA20_WHILE_MA120_UPWARD": {
        'name': 'LONG_MA10_UPCROSS_MA20_WHILE_MA120_UPWARD',
        'processor_object': moving_average.MACrossSignalProcessor,
        'handler': 'run',
        # 'kwargs': {
        #     'PRI_MA': "MA_10",        # Primary MA line
        #     'REF_MA': "MA_20",        # MA line for reference
        #     'CROSS_TYPE': 'UP',   # MA lines can up or down cross each other,
        # },
        'include_signal': ['MA_10', 'MA_20', 'MA_120'],
        'exclude_siganl':[],
    }
}