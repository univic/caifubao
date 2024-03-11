
from app.lib.factor_facotry.processors import fq_factor, moving_average


factor_processor_registry = {
    "FQ_FACTOR": {
        'name': "FQ_FACTOR",
        'processor_object': fq_factor.FQFactorProcessor,
        'handler': 'run',
        'backtest_overall_anaylsis': True,
    },
    "MA_10": {
        'name': "MA_10",
        'processor_object': moving_average.MovingAverageFactorProcessor,
        'handler': 'run',
        'backtest_overall_anaylsis': True,
        'kwargs': {
            'MA': 10,
            }
    },
    "MA_20": {
        'name': "MA_20",
        'processor_object': moving_average.MovingAverageFactorProcessor,
        'handler': 'run',
        'backtest_overall_anaylsis': True,
        'kwargs': {
            'MA': 20,
        }
    }
}

