# -*- coding: utf-8 -*-
# Author : OpenCode
# Date: 2026-05-15
"""OpenClaw dedicated health-check endpoint.

Provides a single API call for downstream consumers to determine whether
caifubao data is fresh enough for investment analysis.  Reuses the same
status-building primitives as the internal /api/datahub/status endpoint.
"""

from app.api.v1.datahub_status import (
    _build_category_status,
    _compute_freshness_grade,
    _format_datetime,
    _get_pipeline_status,
    _resolve_reference_dates,
)
from app.api.v1.integrations.openclaw.utils import wrap_response
from app.lib.auth_decorators import service_token_required
from app.model.stock import IndividualStock, StockIndex

from . import openclaw_bp


@openclaw_bp.route("/health", methods=["GET"])
@service_token_required(scope="openclaw:data-read")
def integration_health():
    """Data readiness check for downstream analysis consumers.

    Returns:
      - pipeline overall_healthy: whether all critical jobs succeeded
      - pipeline jobs: per-job status, error details, skipped reasons
      - freshness grade: FRESH/STALE/EXPIRED/ERROR/NO_DATA
      - data_as_of: latest quote date the freshness assessment is based on
    """
    reference_dates = _resolve_reference_dates()
    latest_trading_day = reference_dates.get("latest_complete_trading_day")
    today_start = None
    if latest_trading_day is not None:
        import datetime as _dt

        if isinstance(latest_trading_day, _dt.datetime):
            today_start = latest_trading_day.replace(
                hour=0, minute=0, second=0, microsecond=0
            )
        else:
            today_start = _dt.datetime.combine(latest_trading_day, _dt.time.min)

    pipeline_status = _get_pipeline_status(today_start)

    stock_category = _build_category_status(
        IndividualStock, "individual_stock", reference_dates
    )
    index_category = _build_category_status(StockIndex, "stock_index", reference_dates)

    freshness = _compute_freshness_grade(
        stock_category, pipeline_status, reference_dates
    )

    data_as_of = stock_category.get("latest_quote_date")

    return wrap_response(
        data={
            "pipeline": {
                "overall_healthy": pipeline_status.get("overall_healthy"),
                "summary": pipeline_status.get("summary"),
                "jobs": pipeline_status.get("jobs", {}),
            },
            "freshness": freshness,
            "coverage": {
                "stock": {
                    "total": stock_category.get("total_count", 0),
                    "up_to_date": stock_category.get("up_to_date_count", 0),
                    "lag_1_day": stock_category.get("lag_1_day_count", 0),
                    "expired": stock_category.get("expired_count", 0),
                    "no_data": stock_category.get("no_data_count", 0),
                },
                "index": {
                    "total": index_category.get("total_count", 0),
                    "up_to_date": index_category.get("up_to_date_count", 0),
                    "lag_1_day": index_category.get("lag_1_day_count", 0),
                    "expired": index_category.get("expired_count", 0),
                    "no_data": index_category.get("no_data_count", 0),
                },
            },
            "reference_dates": {
                "latest_trading_day": _format_datetime(
                    reference_dates.get("latest_complete_trading_day")
                ),
                "previous_trading_day": _format_datetime(
                    reference_dates.get("previous_complete_trading_day")
                ),
            },
        },
        data_as_of=data_as_of,
    )
