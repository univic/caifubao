# -*- coding: utf-8 -*-
# Author : Gemini CLI
# Date: 2026-04-16

from functools import wraps
from flask import request, jsonify, g
import datetime
from app.utilities.auth_util import verify_service_token


def service_token_required(scope=None):
    """
    Decorator to protect OpenClaw integration endpoints with service tokens.
    Usage: @service_token_required(scope="openclaw:data-read")
           @service_token_required(scope=["openclaw:score-read", "openclaw:data-read"])
    """

    def decorator(f):
        @wraps(f)
        def decorated_function(*args, **kwargs):
            auth_header = request.headers.get("Authorization")
            if not auth_header or not auth_header.startswith("Bearer "):
                return jsonify(
                    {
                        "success": False,
                        "message": "Missing or invalid Authorization header",
                        "error_code": "AUTH_HEADER_MISSING",
                    }
                ), 401

            token = auth_header.split(" ")[1]
            token_doc, error = verify_service_token(token, required_scopes=scope)

            if error:
                status_code = 403 if "scope" in error.lower() else 401
                return jsonify(
                    {"success": False, "message": error, "error_code": "AUTH_FAILED"}
                ), status_code

            # Update tracking info
            token_doc.update(
                set__last_used_at=datetime.datetime.now(datetime.UTC),
                set__last_used_ip=request.remote_addr,
            )

            # Store token in context for use in endpoint
            g.service_token = token_doc

            return f(*args, **kwargs)

        return decorated_function

    return decorator
