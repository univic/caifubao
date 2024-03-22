
from app.lib.factor_facotry.processors import fq_factor, moving_average


factor_processor_registry = {
    "FQ_FACTOR": {
        'name': "FQ_FACTOR",
        'processor_object': fq_factor.FQFactorProcessor,
        'handler': 'run',
        'backtest_overall_anaylsis': True,
        'partial_process_offset': 1,
    },
    "MA_10": {
        'name': "MA_10",
        'processor_object': moving_average.MovingAverageFactorProcessor,
        'handler': 'run',
        'backtest_overall_anaylsis': True,
        'partial_process_offset': 10,
        'kwargs': {
            'MA': 10,
            }
    },
    "MA_20": {
        'name': "MA_20",
        'processor_object': moving_average.MovingAverageFactorProcessor,
        'handler': 'run',
        'backtest_overall_anaylsis': True,
        'partial_process_offset': 20,
        'kwargs': {
            'MA': 20,
        },
    },
    "MA_120": {
        'name': "MA_120",
        'processor_object': moving_average.MovingAverageFactorProcessor,
        'handler': 'run',
        'backtest_overall_anaylsis': True,
        'partial_process_offset': 120,
        'kwargs': {
            'MA': 120,
        }
    }
}