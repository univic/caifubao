# -*- coding: utf-8 -*-
# Health check endpoint for K8s probes

from flask import Blueprint, jsonify

health_bp = Blueprint("health", __name__)


@health_bp.route("/health", methods=["GET"])
def health_check():
    """Health check endpoint for K8s liveness/readiness probes"""
    return jsonify({"status": "ok"}), 200
