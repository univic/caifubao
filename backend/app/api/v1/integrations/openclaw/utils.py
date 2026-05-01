# -*- coding: utf-8 -*-
# Author : Gemini CLI
# Date: 2026-04-16

import uuid
import datetime
from flask import jsonify


def wrap_response(data=None, success=True, message="Success", data_as_of=None):
    """
    Standardize the response format for OpenClaw integration APIs.
    """
    response = {
        "success": success,
        "message": message,
        "request_id": str(uuid.uuid4()),
        "generated_at": datetime.datetime.utcnow().isoformat(),
        "data": data,
    }

    if data_as_of:
        response["data_as_of"] = data_as_of

    return jsonify(response)
