# -*- coding: utf-8 -*-
# Author : Gemini CLI
# Date: 2026-04-16

import uuid
import datetime
import logging
from flask import jsonify

logger = logging.getLogger(__name__)


def _get_latest_date(model_cls, date_field="date", filter_kwargs=None):
    """Query the latest available date from a mongoengine model.

    Returns ISO-8601 string or None on failure / no data.

    Date-related filter keys (date, date__gte, date__lte) are stripped so
    that data_as_of reflects the actual data freshness boundary, not the
    caller's query date range.
    """
    try:
        qs = model_cls.objects
        if filter_kwargs:
            clean_kwargs = {
                k: v
                for k, v in filter_kwargs.items()
                if k not in ("date", "date__gte", "date__lte")
            }
            if clean_kwargs:
                qs = qs(**clean_kwargs)
        latest = qs.only(date_field).order_by(f"-{date_field}").first()
        if latest is not None:
            val = getattr(latest, date_field, None)
            if val is not None:
                if hasattr(val, "isoformat"):
                    return val.isoformat()
                return str(val)
    except Exception:
        logger.debug(
            "_get_latest_date failed for %s", model_cls.__name__, exc_info=True
        )
    return None


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
