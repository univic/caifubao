
from app.lib.factor_facotry.processors import fq_factor, moving_average


factor_registry = {
    "FQ_FACTOR": {
        'processor_object': fq_factor.FQFactorProcessor,
        'handler': 'generate_factor',
        'backtest_batch_anaylsis': True,
    },
    "MA_10": {
        'processor_object': moving_average.MovingAverageFactorProcessor,
        'handler': 'generate_factor',
        'backtest_batch_anaylsis': True,
        'kwargs': {
            'MA': 10,
            }
    }
}

