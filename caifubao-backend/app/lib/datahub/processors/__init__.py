from app.lib.datahub.processors.china_a_stock import ChinaAStock


registry = {
    "ChinaAStock": {
        'processor_object': ChinaAStock,
        'handler': 'initialize',
        'backtest_overall_anaylsis': True,
    },
}

