# -*- coding: utf-8 -*-
"""Health endpoints for K8s probes.

``/health`` is a pure liveness check: the process is up and the WSGI stack can
answer. It must not depend on MongoDB, otherwise a database outage would restart
every API pod in a crash loop.

``/health/ready`` is the readiness check: it verifies that the application can
reach MongoDB. K8s removes the pod from Service endpoints while it fails, which
is the correct behaviour for a database outage (restarting would not help).
"""

import logging

from flask import Blueprint, jsonify

from app.lib.db_watcher.mongoengine_tool import ConnTestDocument

logger = logging.getLogger(__name__)

health_bp = Blueprint("health", __name__)


@health_bp.route("/health", methods=["GET"])
def health_check():
    """Liveness check — the process can serve HTTP."""
    return jsonify({"status": "ok"}), 200


@health_bp.route("/health/ready", methods=["GET"])
def readiness_check():
    """Readiness check — MongoDB is reachable."""
    try:
        ConnTestDocument.objects.first()
    except Exception:
        logger.warning(
            "Readiness check failed: MongoDB is not reachable", exc_info=True
        )
        return jsonify({"status": "unavailable", "database": "unreachable"}), 503
    return jsonify({"status": "ok", "database": "reachable"}), 200
