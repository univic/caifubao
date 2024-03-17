from app.lib.datahub.processors.china_a_stock import ChinaAStock


registry = {
    "ChinaAStock_daily": {
        'module': 'processors.china_a_stock',
        'name': "ChinaAStock_daily",
        'processor_object': ChinaAStock,
        'handler': 'run',
        'backtest_overall_anaylsis': True,
    },
}

