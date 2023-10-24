from app.lib.datahub.processors.china_a_stock import ChinaAStock


registry = {
    "ChinaAStock": {
        'module': 'processors.china_a_stock',
        'object_name': "ChinaAStock",
        'processor_object': ChinaAStock,
        'handler': 'initialize',
        'backtest_overall_anaylsis': True,
    },
}

