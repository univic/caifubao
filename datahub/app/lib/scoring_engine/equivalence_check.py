# -*- coding: utf-8 -*-
"""C1 batch-vs-per-stock equivalence check for one evaluation day.

Operator tool for perf tasks 3.5/5.3: run the *same* trading day through the
legacy per-stock path (``batch_prefetch=False``) and the batched per-day path
(``batch_prefetch=True``), both with ``replace=True``, then diff every persisted
business field for the frozen active cohort.

Read-only by default: without ``apply=True`` the check only reports the plan
(cohort size, data available for that date). With ``apply=True`` it runs both
paths; the second (batch) pass is the last writer, so the persisted state ends
up identical to what production now produces. Any diff means the batch path must
not ship — the C1 rollback gate (``DATAHUB_SCORING_BATCH=0``).
"""

import datetime
import logging
import os
import time

logger = logging.getLogger(__name__)

#: Persisted fields diffed between the two paths (same list as the pytest
#: harness). ``generated_at``/``updated_at`` are write timestamps, not business
#: output, so they are excluded by design.
SNAPSHOT_FIELDS = (
    "stock_code",
    "stock_name",
    "date",
    "horizon",
    "score",
    "rank",
    "percentile",
    "recommendation",
    "base_price",
    "target_date",
    "status",
    "explanation",
    "verification",
    "input_snapshot",
    "model_version",
)


def _normalize(value):
    if isinstance(value, datetime.datetime):
        return value.isoformat()
    if isinstance(value, dict):
        return {key: _normalize(item) for key, item in sorted(value.items())}
    if isinstance(value, (list, tuple)):
        return [_normalize(item) for item in value]
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    # ObjectId / DBRef / LazyReference and other BSON wrappers: compare by string.
    return str(value)


def snapshot_predictions(records) -> dict:
    """``{(stock_code, horizon): {field: normalized_value}}`` for one cohort."""
    snapshot = {}
    for record in records:
        entry = {
            field: _normalize(getattr(record, field, None)) for field in SNAPSHOT_FIELDS
        }
        stock = getattr(record, "stock", None)
        entry["stock"] = _normalize(getattr(stock, "id", stock))
        snapshot[(record.stock_code, record.horizon)] = entry
    return snapshot


def diff_snapshots(legacy: dict, batch: dict) -> list[str]:
    """Field-level diffs between the per-stock and batched snapshots."""
    diffs = []
    for key in sorted(set(legacy) | set(batch)):
        if key not in legacy:
            diffs.append(f"{key}: missing from the per-stock path")
            continue
        if key not in batch:
            diffs.append(f"{key}: missing from the batch path")
            continue
        for field, expected in legacy[key].items():
            actual = batch[key][field]
            if actual != expected:
                diffs.append(f"{key} {field}: per-stock={expected!r} batch={actual!r}")
    return diffs


def _jsonable(value):
    if isinstance(value, datetime.datetime):
        return value.isoformat()
    if isinstance(value, dict):
        return {key: _jsonable(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_jsonable(item) for item in value]
    return value


def _day_predictions(service, date, horizon, codes):
    return list(
        service.prediction_model.objects(
            date=date,
            horizon=horizon,
            model_version=service.model_version,
            stock_code__in=codes,
        )
    )


def _run_pass(service, date, horizon, mode):
    started = time.perf_counter()
    if mode == "ranked":
        result = service.score_all_stocks_ranked(
            date=date, horizon=horizon, replace=True
        )
    else:
        result = service.score_all_stocks(date=date, horizon=horizon, replace=True)
    return result, time.perf_counter() - started


def run_equivalence_check(
    service_factory,
    *,
    date: datetime.datetime,
    horizons,
    mode: str = "raw",
    apply: bool = False,
    max_diffs: int = 20,
) -> dict:
    """Compare the per-stock and batched paths for one date.

    ``service_factory(batch_prefetch: bool)`` must return a ready
    ``StockScoringService`` (tests inject the in-memory fakes). Only
    ``apply=True`` runs scoring; both passes use ``replace=True`` so the
    comparison is independent of the pre-existing rows.
    """
    from app.lib.scoring_engine.scoring_service import normalize_date

    if mode not in ("raw", "ranked"):
        raise ValueError(f"unsupported scoring mode: {mode}")
    # ``mode="raw"`` goes through ``score_all_stocks``, which re-dispatches on
    # DATAHUB_SCORING_MODE. With that env set to ranked the raw pass would
    # silently run the ranked path and the report would label it "raw" — fail
    # closed instead of comparing one path against itself under two names.
    # ``mode="ranked"`` calls ``score_all_stocks_ranked`` directly, so the env
    # cannot affect it.
    env_mode = os.getenv("DATAHUB_SCORING_MODE", "raw").strip().lower()
    if mode == "raw" and env_mode == "ranked":
        raise ValueError(
            "DATAHUB_SCORING_MODE='ranked' contradicts --mode 'raw'; "
            "unset the env var or pass --mode ranked"
        )
    date = normalize_date(date)
    horizons = [int(horizon) for horizon in horizons]
    report = {
        "date": date.isoformat(),
        "mode": mode,
        "horizons": horizons,
        "applied": bool(apply),
        "warnings": [],
        "horizons_checked": [],
        "diff_count": None,
        "diffs": [],
        "ok": None,
    }

    probe = service_factory(False)
    codes = [stock.code for stock in probe.stock_model.objects(active_status=0)]
    report["active_codes"] = len(codes)
    report["market"] = {
        "quotes": probe.quote_model.objects(date=date).count(),
        "factors": probe.factor_model.objects(date=date).count(),
        "signals": probe.signal_model.objects(date=date).count(),
    }
    if not codes:
        raise RuntimeError("no active stocks to check")

    # Preflight: the batch path reads the deepest horizon's history window and
    # the industry cache reads metrics strictly before the date. Reporting both
    # tells the operator whether the (slow) window sync is needed and whether
    # the run will actually exercise the industry branch.
    max_history = max(
        max(
            config["minimum_quote_count"],
            config["breakout_lookback"],
            config["risk_lookback"],
        )
        for config in (probe._get_horizon_config(horizon) for horizon in horizons)
    )
    window_start = probe._history_window_start(date, max_history)
    window_quotes = probe.quote_model.objects(
        code__in=codes, date__gte=window_start, date__lt=date
    ).count()
    report["window"] = {
        "start": window_start.isoformat(),
        "quote_rows": window_quotes,
        "expected_rows": len(codes) * max_history,
        "index_rows": probe.quote_model.objects(
            code="sh000300", date__gte=window_start, date__lte=date
        ).count(),
        "industry_metrics_before_date": probe.industry_metrics_model.objects(
            date__lt=date
        ).count(),
    }
    if not report["market"]["quotes"]:
        report["warnings"].append(
            f"no quotes for {date:%Y-%m-%d}; every stock would be BLOCKED"
        )
    if not window_quotes:
        report["warnings"].append(
            "no quotes in the history window; sync it before running the check"
        )
    elif window_quotes < report["window"]["expected_rows"] / 2:
        report["warnings"].append(
            "history window looks sparse; many codes may take the per-code fallback"
        )
    if not report["window"]["industry_metrics_before_date"]:
        report["warnings"].append(
            "no industry daily metrics before this date; the industry component "
            "takes its neutral branch (still equivalent, weaker coverage)"
        )

    if not apply:
        report["plan"] = (
            "read-only preview: pass --apply to run the per-stock path and the "
            "batched path with replace=True and diff every persisted field"
        )
        return report

    all_diffs: list[str] = []
    for horizon in horizons:
        legacy_service = service_factory(False)
        legacy_result, legacy_seconds = _run_pass(legacy_service, date, horizon, mode)
        legacy = snapshot_predictions(
            _day_predictions(legacy_service, date, horizon, codes)
        )
        batch_service = service_factory(True)
        batch_result, batch_seconds = _run_pass(batch_service, date, horizon, mode)
        batch = snapshot_predictions(
            _day_predictions(batch_service, date, horizon, codes)
        )

        diffs = diff_snapshots(legacy, batch)
        all_diffs.extend(diffs)
        report["horizons_checked"].append(
            {
                "horizon": horizon,
                "rows": len(batch),
                "expected_rows": len(codes),
                "legacy_seconds": round(legacy_seconds, 2),
                "batch_seconds": round(batch_seconds, 2),
                "speedup": (
                    round(legacy_seconds / batch_seconds, 2) if batch_seconds else None
                ),
                "legacy_result": _jsonable(legacy_result),
                "batch_result": _jsonable(batch_result),
                "diff_count": len(diffs),
                "diffs": diffs[:max_diffs],
            }
        )
        logger.info(
            "equivalence %s h=%d: rows=%d/%d legacy=%.1fs batch=%.1fs diffs=%d",
            date.strftime("%Y-%m-%d"),
            horizon,
            len(batch),
            len(codes),
            legacy_seconds,
            batch_seconds,
            len(diffs),
        )
        if len(batch) != len(codes):
            report["warnings"].append(
                f"h{horizon}: {len(batch)} persisted rows for {len(codes)} active codes"
            )

    report["diff_count"] = len(all_diffs)
    report["diffs"] = all_diffs[:max_diffs]
    report["ok"] = not all_diffs
    report["checked_at"] = datetime.datetime.now(datetime.UTC).isoformat()
    return report
