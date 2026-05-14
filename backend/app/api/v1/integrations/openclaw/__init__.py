# -*- coding: utf-8 -*-
# Author : Gemini CLI
# Date: 2026-04-16

import datetime

from flask import Blueprint, g, jsonify

from app.lib.auth_decorators import service_token_required

openclaw_bp = Blueprint(
    "openclaw", __name__, url_prefix="/api/v1/integrations/openclaw"
)


@openclaw_bp.route("/", methods=["GET"])
@service_token_required(scope="openclaw:data-read")
def index():
    """Integration health check and info"""
    return jsonify(
        {
            "success": True,
            "message": "Caifubao OpenClaw Integration API",
            "version": "v1-mvp",
            "service_identity": g.service_token.name,
            "timestamp": datetime.datetime.utcnow().isoformat(),
        }
    )


def register_routes():
    """
    Import sub-modules to register routes.
    Must be called BEFORE blueprint is registered to the app.
    """
    from . import (
        factors,
        health,
        quality,
        quotes,
        recommendations,
        signals,
        stocks,
    )

    # Access them to satisfy potential linting, though import triggers registration
    _ = (
        factors,
        health,
        quality,
        quotes,
        recommendations,
        signals,
        stocks,
    )
