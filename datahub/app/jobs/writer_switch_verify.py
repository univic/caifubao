"""Read-only writer-switch verifier (TASK-404 slice 2).

Compares the research environment against the legacy stable environment for a
trade date using the acceptance metrics defined in the private
data-authority-cutover design doc (§5.2):

- freshness: the latest business date per collection matches on both sides;
- count: the same-day document counts differ by at most a relative tolerance
  (universe drift bandwidth, default 0.5%);
- samples: at least N random (business-key) documents from research are
  compared field-by-field against the stable document with the same business
  key (timestamp/bookkeeping fields excluded).

The verifier NEVER writes to either data domain; its only DB write is the
``datahub_job_runs`` row that records the verification outcome (ops-local,
same as the health-watcher). Exit code 1 when any check fails.

Connection model mirrors the legacy sync engine: the research side uses the
default MONGODB_* env (mongoengine), the legacy stable side uses the
MONGODB_SRC_* env through ``sync_engine``'s read-only client builder. Supply
the stable credentials from a read-only user via a one-off secret attached
with the job launcher's ``--env-from-secret`` flag — never bake them into the
image or the manifest.
"""

from __future__ import annotations

import argparse
import datetime
import json
import logging
import math
import random
import sys

from pymongo import MongoClient

from app.lib.utilities import job_run_helper

logger = logging.getLogger(__name__)

# Per-collection comparison spec: business key for sample matching, the date
# field for freshness/count, and the same aliases the sync/snapshot runners
# use. Scoring is included because §5.2 covers it even though the legacy sync
# surface does not carry it.
COLLECTION_SPECS = {
    "quote": {
        "collection": "stock_daily_quote",
        "date_field": "date",
        "keys": ["code", "date"],
    },
    "daily_basic": {
        "collection": "stock_daily_basic",
        "date_field": "date",
        "keys": ["code", "date"],
    },
    "factor": {
        "collection": "stock_factor_daily",
        "date_field": "date",
        "keys": ["stock_code", "date"],
    },
    "signal": {
        "collection": "stock_signal_daily",
        "date_field": "date",
        "keys": ["stock_code", "date", "signal_name"],
    },
    "scoring": {
        "collection": "stock_score_predictions",
        "date_field": "date",
        "keys": ["stock_code", "date", "horizon", "model_version"],
    },
}

# Bookkeeping fields excluded from the field-by-field sample comparison: they
# legitimately differ between independently produced environments.
SKIP_FIELDS = {
    "_id",
    # mongoengine reference fields (DBRef/ObjectId) are minted per environment
    # — nothing syncs the stock universe, so factor/signal/scoring docs always
    # carry env-specific `stock` refs. The business keys identify the stock.
    "stock",
    "created_at",
    "updated_at",
    "synced_at",
    "sync_time",
    "generated_at",
    "imported_at",
    "verified_at",
    "processed_at",
}

DEFAULT_TOLERANCE = 0.005
DEFAULT_SAMPLES = 20
SYNC_JOB_NAME = "datahub_writer_switch_verify"
SYNC_JOB_FAMILY = "writer_switch_verify"
SYNC_JOB_TRIGGER = "manual"
SYNC_JOB_SOURCE = "cli"


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Read-only research vs legacy-stable comparison for a writer "
            "switch (TASK-404 §5.2 metrics). Writes nothing but the "
            "datahub_job_runs verification record."
        )
    )
    parser.add_argument(
        "--collections",
        default=None,
        help=(
            "Comma-separated collection aliases "
            f"({','.join(COLLECTION_SPECS)}); default: all"
        ),
    )
    parser.add_argument(
        "--trade-date",
        default=None,
        help="YYYY-MM-DD to compare; default: per-collection latest research date",
    )
    parser.add_argument(
        "--samples",
        type=int,
        default=DEFAULT_SAMPLES,
        help=f"Minimum random business-key samples per collection (default {DEFAULT_SAMPLES})",
    )
    parser.add_argument(
        "--tolerance",
        type=float,
        default=DEFAULT_TOLERANCE,
        help=(
            "Max relative count difference (universe drift bandwidth, "
            f"default {DEFAULT_TOLERANCE})"
        ),
    )
    parser.add_argument(
        "--job-name", default=SYNC_JOB_NAME, help="Job name for run tracking"
    )
    parser.add_argument(
        "--job-family", default=SYNC_JOB_FAMILY, help="Job family for run tracking"
    )
    parser.add_argument("--trigger", default=SYNC_JOB_TRIGGER)
    parser.add_argument("--source", default=SYNC_JOB_SOURCE)
    args = parser.parse_args(argv)
    if args.samples < 1:
        parser.error("--samples must be >= 1")
    if args.tolerance < 0:
        parser.error("--tolerance must be >= 0")
    return args


def _resolve_collections(raw: str | None) -> list[str]:
    """Resolve aliases to verified collection names."""
    if not raw:
        return list(COLLECTION_SPECS)
    resolved = []
    for token in [t.strip() for t in raw.split(",") if t.strip()]:
        if token not in COLLECTION_SPECS:
            raise ValueError(
                f"unknown collection alias {token!r} "
                f"(choose from {','.join(COLLECTION_SPECS)})"
            )
        resolved.append(token)
    return resolved


def _build_stable_client() -> MongoClient:
    """Reuse the legacy sync engine's read-only source client builder.

    Fail-closed pre-flight: without explicit legacy credentials the verifier
    must refuse to run rather than silently compare research against itself
    (a vacuous pass).
    """
    from app.conf import app_config as cfg
    from app.lib.datahub.sync_engine import _build_src_client

    missing = [
        name
        for name, value in (
            ("MONGODB_SRC_HOST", cfg.MONGODB_SRC_HOST),
            ("MONGODB_SRC_NAME", cfg.MONGODB_SRC_NAME),
            ("MONGODB_SRC_USER", cfg.MONGODB_SRC_USERNAME),
            ("MONGODB_SRC_PASS", cfg.MONGODB_SRC_PASSWORD),
        )
        if not value
    ]
    if missing:
        raise RuntimeError(
            "legacy stable credentials missing: "
            + ", ".join(missing)
            + " — refusing to compare research against itself; attach a "
            "read-only secret via --env-from-secret"
        )
    return _build_src_client()


def _stable_db(client: MongoClient):
    from app.conf import app_config as cfg
    from app.lib.datahub.sync_engine import _get_src_db

    return _get_src_db(client, cfg)


def _as_date(value):
    """Normalize a stored date/datetime to a date for comparisons."""
    if isinstance(value, datetime.datetime):
        return value.date()
    return value


def _latest_date(db, spec) -> object | None:
    field = spec["date_field"]
    doc = db[spec["collection"]].find_one(sort=[(field, -1)], projection={field: 1})
    return _as_date(doc.get(field)) if doc else None


def _same_day_query(spec, trade_date: datetime.date) -> dict:
    """Range query over the date field (it may be stored as datetime)."""
    field = spec["date_field"]
    start = datetime.datetime.combine(trade_date, datetime.time.min)
    end = datetime.datetime.combine(trade_date, datetime.time.max)
    return {field: {"$gte": start, "$lte": end}}


def _count_day(db, spec, trade_date: datetime.date) -> int:
    return db[spec["collection"]].count_documents(_same_day_query(spec, trade_date))


def _relative_diff(a: int, b: int) -> float:
    if a == 0 and b == 0:
        return 0.0
    reference = max(a, b)
    return abs(a - b) / reference


def _values_equal(left, right) -> bool:
    """Field comparison: exact for most BSON types, tolerance for floats."""
    if isinstance(left, bool) or isinstance(right, bool):
        # bool strictness: 1 == True in Python, but a coerced flag is a real
        # data difference between environments
        return isinstance(left, bool) and isinstance(right, bool) and left == right
    if isinstance(left, float) and isinstance(right, (int, float)):
        return math.isclose(left, right, rel_tol=1e-9, abs_tol=1e-12)
    if isinstance(left, (int, float)) and isinstance(right, float):
        return math.isclose(left, right, rel_tol=1e-9, abs_tol=1e-12)
    if isinstance(left, datetime.datetime) and isinstance(right, datetime.datetime):
        return left == right
    return left == right


def _doc_diffs(research_doc: dict, stable_doc: dict | None) -> list[str]:
    """Return human-readable field mismatches for one sample pair."""
    if stable_doc is None:
        return ["stable: MISSING for business key"]
    diffs = []
    for field in sorted(research_doc):
        if field in SKIP_FIELDS:
            continue
        if field not in stable_doc:
            diffs.append(f"{field}: missing on stable side")
            continue
        if not _values_equal(research_doc[field], stable_doc[field]):
            diffs.append(f"{field}: {research_doc[field]!r} != {stable_doc[field]!r}")
    for field in sorted(stable_doc):
        if field in SKIP_FIELDS or field in research_doc:
            continue
        diffs.append(f"{field}: missing on research side")
    return diffs


def _check_collection(
    alias: str,
    research_db,
    stable_db,
    trade_date: datetime.date | None,
    samples: int,
    tolerance: float,
) -> dict:
    spec = COLLECTION_SPECS[alias]
    result: dict = {
        "collection": spec["collection"],
        "status": "PASS",
        "checks": {},
    }

    def _fail(check: str, message: str) -> None:
        result["status"] = "FAIL"
        result["checks"][check]["error"] = message

    # freshness: latest business date equality
    research_latest = _latest_date(research_db, spec)
    stable_latest = _latest_date(stable_db, spec)
    freshness_ok = research_latest is not None and research_latest == stable_latest
    result["checks"]["freshness"] = {
        "research": str(research_latest) if research_latest else None,
        "stable": str(stable_latest) if stable_latest else None,
        "pass": freshness_ok,
    }
    if not freshness_ok:
        _fail(
            "freshness",
            "latest business date differs or research has no data",
        )

    # count: same-day relative difference within tolerance
    effective_date = trade_date or research_latest
    if effective_date is None:
        result["checks"]["count"] = {"pass": False, "error": "no data to compare"}
        result["status"] = "FAIL"
    else:
        trade_date = effective_date
        research_count = _count_day(research_db, spec, trade_date)
        stable_count = _count_day(stable_db, spec, trade_date)
        diff = _relative_diff(research_count, stable_count)
        count_ok = diff <= tolerance
        result["checks"]["count"] = {
            "trade_date": str(trade_date),
            "research": research_count,
            "stable": stable_count,
            "relative_diff": round(diff, 6),
            "denominator": "max(research, stable)",
            "tolerance": tolerance,
            "pass": count_ok,
        }
        if not count_ok:
            _fail("count", "relative count difference above tolerance")

        # samples: random business-key documents compared field-by-field
        picked = list(
            research_db[spec["collection"]]
            .find(_same_day_query(spec, trade_date), projection=spec["keys"])
            .sort([(k, 1) for k in spec["keys"]])
            .limit(1000)
        )
        if alias == "scoring":
            # §5.2 固定样本: deterministic even stride over the sorted
            # universe so every verification pass compares the same basket
            ordered = sorted(
                picked, key=lambda d: tuple(str(d[k]) for k in spec["keys"])
            )
            stride = max(1, len(ordered) // samples) if ordered else 1
            chosen = ordered[::stride][:samples]
        else:
            chosen = (
                random.Random(0).sample(picked, min(samples, len(picked)))
                if picked
                else []
            )
        sample_results = []
        for probe in chosen:
            key_filter = {k: probe[k] for k in spec["keys"]}
            research_doc = research_db[spec["collection"]].find_one(key_filter)
            stable_doc = stable_db[spec["collection"]].find_one(key_filter)
            sample_results.append(
                {
                    "key": {k: str(probe[k]) for k in spec["keys"]},
                    "pass": not _doc_diffs(research_doc, stable_doc),
                    "diffs": _doc_diffs(research_doc, stable_doc),
                }
            )
        samples_ok = bool(sample_results) and all(s["pass"] for s in sample_results)
        result["checks"]["samples"] = {
            "requested": samples,
            "compared": len(sample_results),
            "pass": samples_ok,
            "failures": [s for s in sample_results if not s["pass"]][:10],
        }
        if len(sample_results) < samples:
            _fail(
                "samples",
                f"insufficient samples: compared {len(sample_results)} < "
                f"requested {samples}",
            )
        elif not samples_ok:
            _fail("samples", "field mismatches found")

    return result


def _research_db():
    """Pymongo handle for the local environment via the mongoengine models."""
    from app.model.stock import StockDailyQuote

    return StockDailyQuote._get_collection().database


def run_verify(
    aliases: list[str],
    trade_date: datetime.date | None,
    samples: int,
    tolerance: float,
    research_db=None,
    stable_client: MongoClient | None = None,
) -> dict:
    """Run all checks and return the report dict (also used directly by tests)."""
    if research_db is None:
        research_db = _research_db()
    if stable_client is None:
        stable_client = _build_stable_client()
    stable_db = _stable_db(stable_client)

    started = datetime.datetime.now(datetime.UTC)
    results = [
        _check_collection(alias, research_db, stable_db, trade_date, samples, tolerance)
        for alias in aliases
    ]
    failed = [r for r in results if r["status"] != "PASS"]
    summary = {
        "status": "PASS" if not failed else "FAIL",
        "trade_date": str(trade_date) if trade_date else None,
        "tolerance": tolerance,
        "samples": samples,
        "collections": results,
        "started_at": started.isoformat(),
        "finished_at": datetime.datetime.now(datetime.UTC).isoformat(),
    }
    return summary


def _init_db_connection() -> None:
    from app.lib.db_watcher.mongoengine_tool import mongo_watcher

    mongo_watcher.get_db_connection()


def _run_with_tracking(args: argparse.Namespace) -> dict:
    _init_db_connection()
    try:
        job_run_helper.mark_stale_running_job_runs_failed(job_family=args.job_family)
    except Exception:
        logger.exception("Stale RUNNING job-run cleanup failed; continuing")

    context = job_run_helper.JobRunContext(
        job_name=args.job_name,
        job_family=args.job_family,
        trigger=args.trigger,
        source=args.source,
        scheduled_at=job_run_helper.utc_now_naive(),
        extra={
            "collections": args.collections,
            "trade_date": args.trade_date,
            "samples": args.samples,
            "tolerance": args.tolerance,
        },
    )
    job_run = job_run_helper.create_job_run(context)

    aliases = _resolve_collections(args.collections)
    trade_date = (
        datetime.date.fromisoformat(args.trade_date) if args.trade_date else None
    )
    try:
        summary = run_verify(
            aliases=aliases,
            trade_date=trade_date,
            samples=args.samples,
            tolerance=args.tolerance,
        )
        status = "SUCCESS" if summary["status"] == "PASS" else "FAILED"
        job_run_helper.finish_job_run(
            job_run,
            status=status,
            summary=summary,
        )
        return summary
    except Exception as exc:
        job_run_helper.finish_job_run(
            job_run,
            status="FAILED",
            summary={"error": f"{type(exc).__name__}: {exc}"},
        )
        raise


def main(argv: list[str] | None = None) -> None:
    logging.basicConfig(level=logging.INFO)
    args = parse_args(argv)
    try:
        summary = _run_with_tracking(args)
    except Exception as exc:
        print(
            json.dumps(
                {"status": "ERROR", "error": f"{type(exc).__name__}: {exc}"},
                default=str,
            )
        )
        sys.exit(1)
    print(json.dumps(summary, default=str, sort_keys=True))
    sys.exit(0 if summary["status"] == "PASS" else 1)


if __name__ == "__main__":
    main()
