# -*- coding: utf-8 -*-
# API Blueprint Registration

from flask import Flask

from app.api.health import health_bp
from app.api.v1.admin import admin_bp
from app.api.v1.auth import auth_bp
from app.api.v1.data_quality import data_quality_bp
from app.api.v1.datahub_status import datahub_status_bp
from app.api.v1.indices import indices_bp
from app.api.v1.integrations.openclaw import (
    openclaw_bp,
)
from app.api.v1.integrations.openclaw import (
    register_routes as register_openclaw_routes,
)
from app.api.v1.market import market_bp
from app.api.v1.portfolios import portfolios_bp
from app.api.v1.quotes import quotes_bp
from app.api.v1.scores import scores_bp
from app.api.v1.score_experiments import score_experiments_bp
from app.api.v1.system import system_bp
from app.api.v1.backtest import backtest_bp
from app.api.v1.signals import signals_bp
from app.api.v1.tasks import tasks_bp


def register_blueprint(app: Flask):
    """Register all API blueprints"""
    app.register_blueprint(auth_bp)
    app.register_blueprint(admin_bp)
    app.register_blueprint(indices_bp)
    app.register_blueprint(market_bp)
    app.register_blueprint(portfolios_bp)
    app.register_blueprint(backtest_bp)
    app.register_blueprint(datahub_status_bp)
    app.register_blueprint(data_quality_bp)
    app.register_blueprint(quotes_bp)
    app.register_blueprint(scores_bp)
    app.register_blueprint(score_experiments_bp)
    app.register_blueprint(signals_bp)
    app.register_blueprint(tasks_bp)
    register_openclaw_routes()
    app.register_blueprint(openclaw_bp)
    app.register_blueprint(system_bp)
    app.register_blueprint(health_bp)
