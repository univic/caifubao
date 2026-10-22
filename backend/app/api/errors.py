# -*- coding: utf-8 -*-
"""Uniform JSON error responses for the API.

Before this module an unhandled exception (or a 404/405) returned Flask's HTML
error page while every endpoint and the Vue client expect
``{success, message, request_id, generated_at, data}``. Clients then failed to
parse the body and showed a generic network error instead of the real cause.
"""

import logging
import uuid
from datetime import datetime, timezone

from flask import Flask, g, jsonify, request
from werkzeug.exceptions import HTTPException

logger = logging.getLogger(__name__)


def _request_id() -> str:
    existing = getattr(g, "request_id", None) if g else None
    return existing or str(uuid.uuid4())


def _payload(message: str, status_code: int, error_code: str) -> dict:
    return {
        "success": False,
        "message": message,
        "error_code": error_code,
        "request_id": _request_id(),
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "data": None,
    }


def register_error_handlers(app: Flask) -> None:
    """Return JSON for HTTP errors and unexpected exceptions."""

    @app.errorhandler(HTTPException)
    def handle_http_exception(error: HTTPException):
        # Keep the HTTP status; only the representation becomes JSON.
        return (
            jsonify(
                _payload(
                    error.description or error.name,
                    error.code or 500,
                    f"HTTP_{error.code or 500}",
                )
            ),
            error.code or 500,
        )

    @app.errorhandler(Exception)
    def handle_unexpected_exception(error: Exception):
        logger.exception(
            "Unhandled API exception on %s %s", request.method, request.path
        )
        return (
            jsonify(_payload("Internal server error", 500, "INTERNAL_ERROR")),
            500,
        )
