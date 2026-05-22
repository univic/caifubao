# -*- coding: utf-8 -*-
# Author : Gemini CLI
# Date: 2026-04-16

from flask import request

from app.api.v1.integrations.openclaw.utils import _get_latest_date, wrap_response
from app.api.v1.quotes import _format_datetime, _normalize_symbol
from app.lib.auth_decorators import service_token_required
from app.model.data_asset_status import DataAssetStatus

from . import openclaw_bp


def _serialize_quality_claw(status):
    return {
        "code": status.code,
        "object_type": status.object_type,
        "asset_type": status.asset_type,
        "asset_name": status.asset_name,
        "first_data_date": _format_datetime(status.first_data_date),
        "latest_data_date": _format_datetime(status.latest_data_date),
        "data_count": status.data_count,
        "expected_count": status.expected_count,
        "coverage_rate": status.coverage_rate,
        "status": status.status,
        "status_reason": status.status_reason,
        "last_calculated_at": _format_datetime(status.last_calculated_at),
        "last_success_at": _format_datetime(status.last_success_at),
    }


@openclaw_bp.route("/quality", methods=["GET"])
@service_token_required(scope="openclaw:data-read")
def get_data_quality():
    """
    Get data quality and freshness status for OpenClaw.
    Supports filtering by symbol.
    """
    symbol = request.args.get("symbol")
    asset_type = request.args.get("asset_type")

    query = {}
    if symbol:
        query["code"] = _normalize_symbol(symbol)
    if asset_type:
        query["asset_type"] = asset_type

    quality_qs = DataAssetStatus.objects(**query).order_by("code", "asset_type")

    # Limitation for safety
    items = quality_qs.limit(500)

    data_as_of = (
        _get_latest_date(
            DataAssetStatus, date_field="last_calculated_at", filter_kwargs=query
        )
        if query
        else None
    )

    return wrap_response(
        data={
            "items": [_serialize_quality_claw(q) for q in items],
            "total": quality_qs.count(),
        },
        data_as_of=data_as_of,
    )
