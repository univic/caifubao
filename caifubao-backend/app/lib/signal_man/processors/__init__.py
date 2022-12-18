from app.lib.signal_man.processors import moving_average

registry = {
    "MA_10_UPCROSS_20": {
        'processor_object': moving_average.MACrossSignalProcessor,
        'handler': 'run',
        'kwargs': {
            'PRI_MA': "MA_10",        # Primary MA line
            'REF_MA': "MA_20",        # MA line for reference
            'CROSS_TYPE': 'UP',   # MA lines can UP or DOWN cross each other,
        },
        'factor_dependency': ['MA_10', 'MA_20'],
        'type': 'spot',
        'quote_dependent': False,
    },
    "HFQ_PRICE_ABOVE_MA_120": {
        'processor_object': moving_average.PriceMARelationProcessor,
        'handler': 'run',
        'kwargs': {
            'PRI_MA': "MA_120",  # Primary MA line
            'RELATION_TYPE': 'ABOVE',  # price's relative position to primary MA line, ABOVE or BELOW
        },
        'factor_dependency': ['MA_120'],
        'type': 'continuous',
        'quote_dependent': True,
    }
}

