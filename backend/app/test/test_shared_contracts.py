# -*- coding: utf-8 -*-
"""The backend consumes the shared contracts package, not local copies."""


def test_backend_friction_constants_are_the_shared_ones():
    from app.services import backtest_service
    from caifubao_contracts import friction

    assert backtest_service.COMMISSION_RATE == friction.COMMISSION_RATE
    assert backtest_service.MIN_COMMISSION == friction.MIN_COMMISSION_CNY
    assert backtest_service.STAMP_DUTY_RATE == friction.SELL_STAMP_DUTY_RATE
    assert backtest_service.SLIPPAGE == friction.SLIPPAGE_PER_SIDE


def test_model_modules_reexport_the_shared_documents():
    from app.model import (
        factor,
        factor_eval,
        industry,
        scoring,
        service_token,
        signal,
        stock,
        strategy_replay,
    )
    from caifubao_contracts.models import (
        factor as shared_factor,
        factor_eval as shared_factor_eval,
        industry as shared_industry,
        scoring as shared_scoring,
        service_token as shared_service_token,
        signal as shared_signal,
        stock as shared_stock,
        strategy_replay as shared_strategy_replay,
    )

    assert stock.StockDailyQuote is shared_stock.StockDailyQuote
    assert scoring.StockScorePrediction is shared_scoring.StockScorePrediction
    assert signal.StockSignalDaily is shared_signal.StockSignalDaily
    assert factor.StockFactorDaily is shared_factor.StockFactorDaily
    assert (
        industry.StockIndustryClassification
        is shared_industry.StockIndustryClassification
    )
    assert factor_eval.FactorEvalReport is shared_factor_eval.FactorEvalReport
    assert service_token.ServiceToken is shared_service_token.ServiceToken
    assert strategy_replay.StrategyReplay is shared_strategy_replay.StrategyReplay
