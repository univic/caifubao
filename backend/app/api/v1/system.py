# -*- coding: utf-8 -*-
"""System-level endpoints — no auth required."""

import os

from flask import Blueprint, jsonify

system_bp = Blueprint("system", __name__, url_prefix="/api/system")


@system_bp.route("/env", methods=["GET"])
def get_environment():
    """Return the current deployment environment.

    No auth required — used by the frontend login page and env badge.
    """
    raw = os.getenv("APP_ENV", "DEV").upper()
    if raw in ("PRODUCTION", "PROD"):
        env = "prod"
    elif raw == "TEST":
        env = "test"
    else:
        env = "dev"
    return jsonify({"env": env})
