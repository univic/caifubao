"""Read-only writer-switch verifier (TASK-404 slice 2, fq-adj-factor-fix R1/R2).

Compares the research environment against the legacy stable environment for a
trade date using the acceptance metrics defined in the private
data-authority-cutover design doc (§5.2):

- freshness: the completed session under test is present on both sides. When
  ``--trade-date`` is given it is the reference; otherwise the expected latest
  completed session is derived from the A-share trade calendar
  (``finance_market.trade_calendar``) with a 15:00 Asia/Shanghai cutoff, so two
  mutually stale environments cannot pass. A raw latest date beyond the
  reference is an in-progress session and is flagged, never used as the
  reference;
- count: each declared count class (instrument type from each side's
  ``basic_stock.object_type`` intersected with the supported universe) is
  compared with its own verdict and tolerance; a class with zero rows on
  either side FAILS, a research index-coverage collapse FAILS (strict superset
  + a declared ``min_research_rows`` floor + a code-level superset check), and
  a total can never mask a single-class regression;
- samples: at least N business-key documents are drawn from the complete
  same-day key set and compared field-by-field against the stable document
  with the same business key, restricted to the fields the declared scope
  classifies as compared.

Declared field scope
--------------------

Every observed field MUST be classified by the declared scope, exactly once,
for the collection/row-class it appears in. The classes are:

- ``required_fields``: compared field-by-field; a mismatch, a one-sided
  absence, or absence on both sides FAILS.
- ``derived_fields``: FQ/HFQ-derived (the recomputed ``fq_factor``/OHLC-hfq and
  the MA factors computed from ``close_hfq``). Excluded from parity by default
  because legacy stable is frozen pre-fq-adj-factor-fix and is not the source
  of truth for them; compared, and failing on mismatch, under
  ``--compare-derived``.
- ``research_populated_fields``: populated by the current writer but omitted by
  the frozen legacy ingest. Each MUST be present and non-null on research
  (absence FAILS) and each records its producing writer/source/code revision
  and where that revision came from; absence or null on the legacy side is
  informational, but a non-null value that differs on both sides FAILS.
- ``stable_only_fields``: declared legacy-only fields (informational).
- ``excluded_fields``: bookkeeping fields with an explicit per-field reason
  (``_id``, ``stock`` DBRefs, ``_cls``, and timestamps). Folding these into the
  declaration means a new silent skip is impossible.

Any observed key that is not in exactly one class FAILS the run by name, both
per sampled document and via a full-day key discovery (aggregation
``$objectToArray``) over every document on the reference date.

Source-of-truth check
---------------------

The tushare source check is ON by default: for the sampled supported
individual-stock codes it verifies the recomputed FQ fields against tushare
``pro.adj_factor`` itself, reusing
``tushare_interface.adj_factor``/``to_tushare_ts_code`` exactly as the FQ
writer does: ``fq_factor`` must equal the source factor (carrying the most
recent known factor forward for rows the source omits) and
``close_hfq == round(close * fq_factor, 4)``, with OHLC-hfq scaling by the
same ratio. Invalid/non-finite/non-positive source rows are skipped the same
way ``build_fq_factor_frame`` skips them; the check FAILS only when the
resulting factor series is empty. A tushare error, an empty response, or an
unusable series FAILS the run; it is never skipped and never falls back to
legacy parity. ``--skip-source-tushare`` is for offline debugging only: a PASS
without the source check is NOT an R1 acceptance. The rounding tolerance
(``5e-4``) is reported. The tushare import is lazy: the module stays importable
without network access.

The verifier NEVER writes to either data domain; its writes are the
``datahub_job_runs`` record plus the stale-RUNNING cleanup that
run tracking performs through ``job_run_helper``. Exit code 1 when any check
fails.

Connection model mirrors the legacy sync engine: the research side uses the
default MONGODB_* env (mongoengine), the legacy stable side uses the
MONGODB_SRC_* env through ``sync_engine``'s read-only client builder. Supply
the stable credentials from a read-only user via a one-off secret attached
with the job launcher's ``--env-from-secret`` flag — never bake them into the
image or the manifest.
"""

from __future__ import annotations

import argparse
import bisect
import datetime
import hashlib
import json
import logging
import math
import os
import random
import sys
from pathlib import Path

from pymongo import MongoClient

from app.lib.utilities import data_capability_helper, job_run_helper

logger = logging.getLogger(__name__)

# Rounding tolerance for the source-of-truth check. Stored FQ/HFQ values are
# rounded to 4 decimals by the writer, so a difference within 5e-4 is a
# rounding artifact, not a data difference. Reported for every run.
FQ_SOURCE_TOLERANCE = 5e-4

# China A-share close; a session on ``today`` is only complete after it.
_CN_TZ = datetime.timezone(datetime.timedelta(hours=8))
MARKET_CLOSE = datetime.time(15, 0)
# Measured research index universe on the acceptance trade date; declared in
# the scope (so lowering it bumps scope_version) rather than hard-coded here.
DEFAULT_MIN_RESEARCH_INDEX_ROWS = 562

DEFAULT_TOLERANCE = 0.005
DEFAULT_SAMPLES = 20
SYNC_JOB_NAME = "datahub_writer_switch_verify"
SYNC_JOB_FAMILY = "writer_switch_verify"
SYNC_JOB_TRIGGER = "manual"
SYNC_JOB_SOURCE = "cli"

COUNT_CLASS_MODES = ("relative", "coverage_superset", "research_excludes")

# Reason tied to the frozen legacy environment; recorded with the declared
# scope in both the report and the datahub_job_runs summary.
SCOPE_REASON = (
    "legacy stable is frozen pre-fq-adj-factor-fix: its FQ/HFQ values and the "
    "factors derived from close_hfq predate the 2026-08-31 adj_factor "
    "discontinuity fix, so legacy-stable equality is not the source of truth "
    "for them. Research FQ/HFQ is verified against the real tushare adj_factor "
    "instead."
)
DERIVED_FIELD_REASON = (
    "FQ/HFQ-derived and recomputed on research from the real tushare "
    "adj_factor; legacy stable is frozen pre-fq-adj-factor-fix"
)

# Bookkeeping fields excluded from the field-by-field comparison, each with an
# explicit reason. They legitimately differ between independently produced
# environments.
_BASE_EXCLUDED_FIELD_REASONS = {
    "_id": "Mongo ObjectId is minted per environment; not synchronized data",
    "_cls": "mongoengine inheritance discriminator; not data content",
    "stock": (
        "mongoengine reference (DBRef/ObjectId) is minted per environment; "
        "the business key identifies the stock"
    ),
    "created_at": "bookkeeping timestamp; independently produced per environment",
    "updated_at": "bookkeeping timestamp; independently produced per environment",
    "synced_at": "bookkeeping sync timestamp; independently produced per environment",
    "sync_time": "bookkeeping sync timestamp; independently produced per environment",
    "generated_at": "bookkeeping timestamp; independently produced per environment",
    "imported_at": "bookkeeping import timestamp; independently produced per environment",
    "verified_at": "bookkeeping verification timestamp; independently produced per environment",
    "processed_at": "bookkeeping processing timestamp; independently produced per environment",
}

FIELD_CLASS_ORDER = (
    "required_fields",
    "derived_fields",
    "research_populated_fields",
    "stable_only_fields",
    "excluded_fields",
)

# Collections whose sample basket is a deterministic even stride rather than a
# seeded random draw (§5.2 fixed sample).
DETERMINISTIC_SAMPLE_COLLECTIONS = frozenset({"stock_score_predictions"})


def _row_scope(
    required_fields,
    derived_fields=(),
    research_populated_fields=(),
    stable_only_fields=(),
    excluded_fields=None,
):
    """Build one declared row-class scope, folding in the base exclusions."""
    excluded = dict(_BASE_EXCLUDED_FIELD_REASONS)
    excluded.update(excluded_fields or {})
    return {
        "required_fields": tuple(sorted(required_fields)),
        "derived_fields": tuple(sorted(derived_fields)),
        "research_populated_fields": tuple(sorted(research_populated_fields)),
        "stable_only_fields": tuple(sorted(stable_only_fields)),
        "excluded_fields": excluded,
    }


# Per-collection comparison spec: business key for sample matching, the code
# field used to resolve each side's instrument type, the date field for
# freshness/count, declared count classes, and the declared field scope per row
# class. Scoring is included because §5.2 covers it even though the legacy sync
# surface does not carry it.
COLLECTION_SPECS = {
    "quote": {
        "collection": "stock_daily_quote",
        "date_field": "date",
        "keys": ["code", "date"],
        "code_field": "code",
        "count_classes": {
            "individual_stock": {"mode": "relative"},
            "stock_index": {
                "mode": "coverage_superset",
                "min_research_rows": DEFAULT_MIN_RESEARCH_INDEX_ROWS,
                "missing_code_allowance": 0,
            },
            "unsupported_universe": {"mode": "research_excludes", "allowance": 0},
        },
        "default_row_class": "individual_stock",
        "row_classes": {
            "individual_stock": _row_scope(
                required_fields=[
                    "amplitude",
                    "change_amount",
                    "change_rate",
                    "close",
                    "code",
                    "date",
                    "high",
                    "low",
                    "open",
                    "previous_close",
                    "trade_amount",
                    "trade_status",
                    "turnover_rate",
                    "volume",
                ],
                derived_fields=[
                    "close_hfq",
                    "fq_factor",
                    "high_hfq",
                    "low_hfq",
                    "open_hfq",
                ],
                research_populated_fields=[
                    "isST",
                    "pbMRQ",
                    "pcfNcfTTM",
                    "peTTM",
                    "psTTM",
                ],
            ),
            "stock_index": _row_scope(
                required_fields=[
                    "close",
                    "code",
                    "date",
                    "high",
                    "low",
                    "open",
                    "volume",
                ],
            ),
        },
    },
    "daily_basic": {
        "collection": "stock_daily_basic",
        "date_field": "date",
        "keys": ["code", "date"],
        "code_field": "code",
        "count_classes": {
            "individual_stock": {"mode": "relative"},
            "unsupported_universe": {"mode": "research_excludes", "allowance": 0},
        },
        "default_row_class": "individual_stock",
        "row_classes": {
            "individual_stock": _row_scope(
                required_fields=[
                    "circ_mv",
                    "code",
                    "date",
                    "dv_ttm",
                    "pb",
                    "pe_ttm",
                    "ps_ttm",
                    "total_mv",
                    "turnover_rate",
                ],
            ),
        },
    },
    "factor": {
        "collection": "stock_factor_daily",
        "date_field": "date",
        "keys": ["stock_code", "date"],
        "code_field": "stock_code",
        "count_classes": {
            "individual_stock": {"mode": "relative"},
            "unsupported_universe": {"mode": "research_excludes", "allowance": 0},
        },
        "default_row_class": "individual_stock",
        "row_classes": {
            "individual_stock": _row_scope(
                required_fields=[
                    "category",
                    "date",
                    "stock_code",
                    "stock_name",
                ],
                # Moving averages are computed from close_hfq.
                derived_fields=["ma_10", "ma_120", "ma_20", "ma_30", "ma_60"],
            ),
        },
    },
    "signal": {
        "collection": "stock_signal_daily",
        "date_field": "date",
        "keys": ["stock_code", "date", "signal_name"],
        "code_field": "stock_code",
        "count_classes": {
            "individual_stock": {"mode": "relative"},
            "unsupported_universe": {"mode": "research_excludes", "allowance": 0},
        },
        "default_row_class": "individual_stock",
        "row_classes": {
            "individual_stock": _row_scope(
                required_fields=[
                    "category",
                    "date",
                    "direction",
                    "factor_snapshot",
                    "price_snapshot",
                    "reason",
                    "signal_name",
                    "signal_type",
                    "signal_version",
                    "source_freshness",
                    "stock_code",
                    "stock_name",
                    "strength",
                ],
            ),
        },
    },
    "scoring": {
        "collection": "stock_score_predictions",
        "date_field": "date",
        "keys": ["stock_code", "date", "horizon", "model_version"],
        "code_field": "stock_code",
        "count_classes": {
            "individual_stock": {"mode": "relative"},
            "unsupported_universe": {"mode": "research_excludes", "allowance": 0},
        },
        "default_row_class": "individual_stock",
        "row_classes": {
            "individual_stock": _row_scope(
                required_fields=[
                    "base_price",
                    "date",
                    "explanation",
                    "horizon",
                    "input_snapshot",
                    "model_version",
                    "percentile",
                    "rank",
                    "recommendation",
                    "score",
                    "status",
                    "stock_code",
                    "stock_name",
                    "target_date",
                    "verification",
                ],
            ),
        },
    },
}


def _scope_payload() -> dict:
    """Canonical, hashable representation of the declared scope."""
    payload = {}
    for alias, spec in sorted(COLLECTION_SPECS.items()):
        row_classes = {}
        for row_class, scope in sorted(spec["row_classes"].items()):
            row_classes[row_class] = {
                "required_fields": sorted(scope["required_fields"]),
                "derived_fields": sorted(scope["derived_fields"]),
                "research_populated_fields": sorted(scope["research_populated_fields"]),
                "stable_only_fields": sorted(scope["stable_only_fields"]),
                "excluded_fields": {
                    field: scope["excluded_fields"][field]
                    for field in sorted(scope["excluded_fields"])
                },
            }
        payload[alias] = {
            "collection": spec["collection"],
            "date_field": spec["date_field"],
            "code_field": spec["code_field"],
            "keys": list(spec["keys"]),
            "deterministic_samples": (
                spec["collection"] in DETERMINISTIC_SAMPLE_COLLECTIONS
            ),
            "default_row_class": spec["default_row_class"],
            "count_classes": {
                name: dict(body) for name, body in sorted(spec["count_classes"].items())
            },
            "row_classes": row_classes,
        }
    payload["reason"] = SCOPE_REASON
    payload["derived_reason"] = DERIVED_FIELD_REASON
    return payload


def _compute_scope_version() -> str:
    canonical = json.dumps(
        _scope_payload(), sort_keys=True, separators=(",", ":"), ensure_ascii=True
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


# Stable identifier of the declared scope: a prior PASS cannot be cited after
# the declaration changes.
SCOPE_VERSION = _compute_scope_version()


def _writer_code_revision() -> dict:
    """Code revision of the quote writer that populates research-only fields.

    Prefers an explicit deployment revision, otherwise the sha256 of the
    ``zh_a_daily`` normalization writer, and always reports where the value
    came from.
    """
    override = os.getenv("DATAHUB_CODE_REVISION", "").strip()
    if override:
        return {
            "revision": override,
            "revision_source": "DATAHUB_CODE_REVISION",
        }
    path = (
        Path(__file__).resolve().parents[1]
        / "lib"
        / "datahub"
        / "data_source"
        / "handler"
        / "zh_a_daily.py"
    )
    try:
        digest = hashlib.sha256(path.read_bytes()).hexdigest()[:12]
    except OSError:
        return {"revision": "unknown", "revision_source": "unavailable"}
    return {
        "revision": digest,
        "revision_source": f"source_file_sha256:{path.name}",
    }


_QUOTE_WRITER = (
    "datahub quote writer (zh_a_daily normalization + StockDailyQuote upsert)"
)
_WRITER_REVISION = _writer_code_revision()

# Provenance for every research-populated field: producing writer, source, code
# revision and where the revision came from.
RESEARCH_POPULATED_PROVENANCE = {
    field: {
        "writer": _QUOTE_WRITER,
        "source": "tushare/baostock zh_a_daily daily quote",
        **_WRITER_REVISION,
    }
    for field in ("isST", "pbMRQ", "pcfNcfTTM", "peTTM", "psTTM")
}


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
        help=(
            "YYYY-MM-DD completed session to compare; default: the expected "
            "latest completed session from the A-share trade calendar (15:00 "
            "Asia/Shanghai cutoff), falling back to the per-collection latest "
            "research date when no calendar is available. A raw latest date "
            "beyond the reference is reported as an in-progress session, not "
            "used as the reference"
        ),
    )
    parser.add_argument(
        "--samples",
        type=int,
        default=DEFAULT_SAMPLES,
        help=f"Minimum business-key samples per collection (default {DEFAULT_SAMPLES})",
    )
    parser.add_argument(
        "--tolerance",
        type=float,
        default=DEFAULT_TOLERANCE,
        help=(
            "Max relative count difference for relative count classes "
            f"(universe drift bandwidth, default {DEFAULT_TOLERANCE})"
        ),
    )
    parser.add_argument(
        "--compare-derived",
        action="store_true",
        help=(
            "Also compare FQ/HFQ-derived fields against legacy stable "
            "(excluded by default; any mismatch then FAILS)"
        ),
    )
    parser.add_argument(
        "--verify-source-tushare",
        dest="verify_source_tushare",
        action="store_true",
        default=True,
        help=(
            "(default) verify sampled codes' fq_factor/close_hfq/OHLC-hfq "
            "against tushare pro.adj_factor itself; a source error/empty/"
            "unusable series FAILS (never skipped, never legacy parity)"
        ),
    )
    parser.add_argument(
        "--skip-source-tushare",
        dest="verify_source_tushare",
        action="store_false",
        help=(
            "offline debugging only: skip the tushare source check; a PASS "
            "without it is NOT an R1 acceptance"
        ),
    )
    parser.add_argument(
        "--job-name", default=SYNC_JOB_NAME, help="Job name for run tracking"
    )
    parser.add_argument(
        "--job-family", default=SYNC_JOB_FAMILY, help="Job family for run tracking"
    )
    parser.add_argument(
        "--trigger", default=SYNC_JOB_TRIGGER, help="Run trigger for tracking"
    )
    parser.add_argument(
        "--source", default=SYNC_JOB_SOURCE, help="Run source for tracking"
    )
    args = parser.parse_args(argv)
    if args.samples < 1:
        parser.error("--samples must be >= 1")
    if args.tolerance < 0:
        parser.error("--tolerance must be >= 0")
    return args


def _resolve_collections(raw: str | None) -> list[str]:
    """Resolve aliases to verified collection names (fail closed on blanks)."""
    if raw is None:
        return list(COLLECTION_SPECS)
    if not raw.strip():
        raise ValueError("no collections selected: --collections is blank")
    resolved = []
    for token in [t.strip() for t in raw.split(",") if t.strip()]:
        if token not in COLLECTION_SPECS:
            raise ValueError(
                f"unknown collection alias {token!r} "
                f"(choose from {','.join(COLLECTION_SPECS)})"
            )
        resolved.append(token)
    if not resolved:
        raise ValueError("no collections selected: --collections has no aliases")
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
    """Document count on a trade date, used to prove the sample pool is whole."""
    return db[spec["collection"]].count_documents(_same_day_query(spec, trade_date))


def _trade_calendar(db) -> list:
    """A-share trading days from ``finance_market`` (read-only, best effort)."""
    try:
        collection = db["finance_market"]
    except KeyError:
        return []
    try:
        doc = collection.find_one(
            {"name": "ChinaAStock"}, projection={"_id": 0, "trade_calendar": 1}
        )
    except Exception:  # pragma: no cover - defensive against driver variants
        return []
    days = {
        _as_date(value)
        for value in (doc or {}).get("trade_calendar") or []
        if _as_date(value) is not None
    }
    return sorted(days)


def _latest_completed_session(db, now=None) -> object | None:
    """Latest A-share session whose data is expected to be complete.

    A session on ``today`` only counts after the 15:00 Asia/Shanghai close.
    Returns ``None`` when no trade calendar is available, in which case callers
    fall back to the latest research date and record that the reference is not
    calendar-anchored.
    """
    calendar = _trade_calendar(db)
    if not calendar:
        return None
    now = now or datetime.datetime.now(_CN_TZ)
    today = now.date()
    cutoff_passed = now.time() >= MARKET_CLOSE
    candidates = [
        day for day in calendar if day < today or (day == today and cutoff_passed)
    ]
    return candidates[-1] if candidates else None


def _relative_diff(a: int, b: int) -> float:
    if a == 0 and b == 0:
        return 0.0
    reference = max(a, b)
    return abs(a - b) / reference


def _values_equal(left, right) -> bool:
    """Field comparison: exact for most BSON types, tolerance for floats."""
    if isinstance(left, bool) or isinstance(right, bool):
        # bool strictness: 1 == True in Python, but a coerced flag is a real
        # data difference between independently produced environments
        return isinstance(left, bool) and isinstance(right, bool) and left == right
    if isinstance(left, float) and isinstance(right, (int, float)):
        return math.isclose(left, right, rel_tol=1e-9, abs_tol=1e-12)
    if isinstance(left, (int, float)) and isinstance(right, float):
        return math.isclose(left, right, rel_tol=1e-9, abs_tol=1e-12)
    if isinstance(left, datetime.datetime) and isinstance(right, datetime.datetime):
        return left == right
    return left == right


def _field_class_map(scope) -> dict:
    """Map each declared field to the class(es) declaring it."""
    mapping: dict = {}
    for class_name in FIELD_CLASS_ORDER:
        values = scope[class_name]
        fields = values.keys() if class_name == "excluded_fields" else values
        for field in fields:
            mapping.setdefault(field, []).append(class_name)
    return mapping


def _validate_declared_scope() -> None:
    """Fail closed when the declaration is ambiguous or under-specified.

    Called at import and again at the start of every run, so a scope edit
    cannot ship an ambiguous field classification or drop a provenance entry.
    """
    for alias, spec in COLLECTION_SPECS.items():
        if spec["default_row_class"] not in spec["row_classes"]:
            raise RuntimeError(
                f"declared scope: {alias} default row class is not declared"
            )
        for row_class, scope in spec["row_classes"].items():
            for field, classes in _field_class_map(scope).items():
                if len(classes) != 1:
                    raise RuntimeError(
                        f"declared scope conflict: {alias}/{row_class}/{field} "
                        f"in {classes}"
                    )
            for field in scope["research_populated_fields"]:
                if field not in RESEARCH_POPULATED_PROVENANCE:
                    raise RuntimeError(
                        f"research-populated field without provenance: "
                        f"{alias}/{row_class}/{field}"
                    )
        for name, class_spec in spec["count_classes"].items():
            mode = class_spec.get("mode", "relative")
            if mode not in COUNT_CLASS_MODES:
                raise RuntimeError(
                    f"declared count class with unknown mode: {alias}/{name}={mode}"
                )
            if mode == "coverage_superset" and "min_research_rows" not in class_spec:
                raise RuntimeError(
                    f"coverage_superset class without min_research_rows: {alias}/{name}"
                )


def _scope_report(spec, compare_derived: bool) -> dict:
    """Report the declared scope: version, reason and the excluded classes."""
    excluded: dict = {}
    derived: set = set()
    research_populated: set = set()
    for scope in spec["row_classes"].values():
        excluded.update(scope["excluded_fields"])
        derived.update(scope["derived_fields"])
        research_populated.update(scope["research_populated_fields"])
    return {
        "version": SCOPE_VERSION,
        "reason": SCOPE_REASON,
        "compare_derived": compare_derived,
        "derived_fields": sorted(derived),
        "derived_reason": DERIVED_FIELD_REASON,
        "excluded_fields": excluded,
        "research_populated_fields": {
            field: RESEARCH_POPULATED_PROVENANCE[field]
            for field in sorted(research_populated)
        },
    }


def _instrument_type_map(db) -> dict:
    """Per-side code -> object_type from ``basic_stock`` (the instrument source).

    Falls back to the mongoengine inheritance discriminator when a legacy doc
    omits the stored ``object_type``; never uses a code-prefix heuristic to
    decide index vs individual stock.
    """
    try:
        collection = db["basic_stock"]
    except KeyError:
        return {}
    try:
        cursor = collection.find(
            {}, projection={"code": 1, "object_type": 1, "_cls": 1}
        )
    except Exception:  # pragma: no cover - defensive against driver variants
        return {}
    discriminator = {"IndividualStock": "individual_stock", "StockIndex": "stock_index"}
    mapping = {}
    for doc in cursor:
        code = doc.get("code")
        object_type = doc.get("object_type") or discriminator.get(doc.get("_cls"))
        if code is not None and object_type:
            mapping[str(code)] = str(object_type)
    return mapping


def _row_class_for_code(spec, type_map, code):
    if code is None:
        return None
    value = type_map.get(str(code))
    return value if value in spec["row_classes"] else None


def _row_class_for_sample(spec, research_doc, stable_doc, research_types, stable_types):
    for doc, types in ((research_doc, research_types), (stable_doc, stable_types)):
        if not doc:
            continue
        resolved = _row_class_for_code(spec, types, doc.get(spec["code_field"]))
        if resolved:
            return resolved
    return spec["default_row_class"]


class _Missing:
    pass


_MISSING = _Missing()


def _compare_sample(research_doc, stable_doc, scope, compare_derived: bool = False):
    """Compare one sampled business key against the declared row-class scope.

    Returns ``diffs`` (compared-field mismatches and required-field absences),
    ``undeclared`` (observed keys not in exactly one class), ``presence_failures``
    (research-populated fields missing/null on research) and ``informational``
    notes.
    """
    result = {
        "diffs": [],
        "undeclared": [],
        "presence_failures": [],
        "informational": [],
    }
    research_doc = research_doc or {}
    stable_doc = stable_doc or {}
    if not research_doc and not stable_doc:
        result["diffs"].append("no document on either side")
        return result

    field_class = _field_class_map(scope)
    for field in sorted(set(research_doc) | set(stable_doc)):
        classes = field_class.get(field, [])
        if len(classes) != 1:
            result["undeclared"].append(field)

    compared = set(scope["required_fields"])
    if compare_derived:
        compared |= set(scope["derived_fields"])
    for field in sorted(compared):
        in_research = field in research_doc
        in_stable = field in stable_doc
        if in_research and in_stable:
            if not _values_equal(research_doc[field], stable_doc[field]):
                result["diffs"].append(
                    f"{field}: {research_doc[field]!r} != {stable_doc[field]!r}"
                )
        elif in_research:
            if stable_doc:
                result["diffs"].append(f"{field}: missing on stable side")
            else:
                result["informational"].append(
                    "stable side has no document for this business key "
                    "(coverage is governed by the count classes)"
                )
        elif in_stable:
            result["diffs"].append(f"{field}: missing on research side")
        elif field in scope["required_fields"]:
            result["diffs"].append(f"{field}: required field absent on both sides")

    for field in sorted(scope["research_populated_fields"]):
        if not research_doc:
            continue
        value = research_doc.get(field, _MISSING)
        if value is _MISSING or value is None:
            result["presence_failures"].append(
                f"{field}: declared research-populated but missing or null on research"
            )
            continue
        stable_value = stable_doc.get(field, _MISSING)
        if stable_value is _MISSING or stable_value is None:
            result["informational"].append(
                f"{field}: absent on legacy stable (frozen "
                f"pre-fq-adj-factor-fix); research={value!r}"
            )
        elif not _values_equal(value, stable_value):
            result["diffs"].append(
                f"{field}: {value!r} != {stable_value!r} (declared "
                "research-populated but non-null on both sides)"
            )

    for field in sorted(scope["stable_only_fields"]):
        if field in stable_doc and field not in research_doc:
            result["informational"].append(
                f"{field}: declared legacy-only field absent on research"
            )
    return result


def _count_by_class(db, spec, trade_date: datetime.date, type_map):
    """Count same-day rows per declared class using each side's instrument type.

    Returns ``(counts, codes, excluded)``. The supported-universe rule
    (``is_bse_stock_code``) is applied as universe membership: unsupported rows
    land in the declared ``unsupported_universe`` class. A supported code the
    instrument source cannot classify is counted as unclassified and fails the
    count check rather than being silently bucketed.
    """
    counts = {name: 0 for name in spec["count_classes"]}
    codes = {name: set() for name in spec["count_classes"]}
    excluded = {"unclassified": 0}
    code_field = spec["code_field"]
    query = _same_day_query(spec, trade_date)
    for doc in db[spec["collection"]].find(query, projection={code_field: 1}):
        code = doc.get(code_field)
        if code is None:
            excluded["unclassified"] += 1
            continue
        if data_capability_helper.is_bse_stock_code(code):
            if "unsupported_universe" in counts:
                counts["unsupported_universe"] += 1
                codes["unsupported_universe"].add(str(code))
            else:
                excluded["unclassified"] += 1
            continue
        object_type = type_map.get(str(code))
        if object_type in counts:
            counts[object_type] += 1
            codes[object_type].add(str(code))
        else:
            excluded["unclassified"] += 1
    return counts, codes, excluded


def _day_field_keys(db, spec, trade_date, type_map) -> dict:
    """Every top-level field key on every same-day document, per row class.

    Uses an aggregation ``$objectToArray`` projection so the scan is bounded to
    the reference date's key names; falls back to iterating documents for test
    doubles or drivers without aggregation.
    """
    collection = db[spec["collection"]]
    query = _same_day_query(spec, trade_date)
    code_field = spec["code_field"]
    pipeline = [
        {"$match": query},
        {
            "$project": {
                "_keys": {
                    "$map": {
                        "input": {"$objectToArray": "$$ROOT"},
                        "as": "kv",
                        "in": "$$kv.k",
                    }
                },
                code_field: 1,
            }
        },
    ]
    rows = None
    try:
        rows = list(collection.aggregate(pipeline))
    except Exception:
        rows = None
    discovered: dict = {}
    if rows is not None:
        for row in rows:
            row_class = (
                _row_class_for_code(spec, type_map, row.get(code_field))
                or spec["default_row_class"]
            )
            discovered.setdefault(row_class, set()).update(row.get("_keys") or [])
        return discovered
    for doc in collection.find(query):
        row_class = (
            _row_class_for_code(spec, type_map, doc.get(code_field))
            or spec["default_row_class"]
        )
        discovered.setdefault(row_class, set()).update(doc.keys())
    return discovered


def _choose_samples(picked, spec, samples: int):
    keys = spec["keys"]
    ordered = sorted(picked, key=lambda d: tuple(str(d[k]) for k in keys))
    if not ordered:
        return []
    if spec["collection"] in DETERMINISTIC_SAMPLE_COLLECTIONS:
        # §5.2 fixed sample: deterministic even stride over the sorted
        # universe so every verification pass compares the same basket
        stride = max(1, len(ordered) // samples)
        return ordered[::stride][:samples]
    return random.Random(0).sample(ordered, min(samples, len(ordered)))


def _check_collection(
    alias: str,
    research_db,
    stable_db,
    trade_date: datetime.date | None,
    samples: int,
    tolerance: float,
    compare_derived: bool = False,
) -> dict:
    spec = COLLECTION_SPECS[alias]
    result: dict = {
        "collection": spec["collection"],
        "status": "PASS",
        "scope": _scope_report(spec, compare_derived),
        "checks": {},
    }

    def _fail(check: str, message: str) -> None:
        result["status"] = "FAIL"
        entry = result["checks"].setdefault(check, {})
        previous = entry.get("error")
        entry["error"] = f"{previous}; {message}" if previous else message

    research_latest = _latest_date(research_db, spec)
    stable_latest = _latest_date(stable_db, spec)

    # Reference session: an explicit --trade-date wins; otherwise anchor on the
    # expected latest completed session from the trade calendar so two mutually
    # stale environments cannot pass. Without a calendar we fall back to the
    # latest research date and say so.
    expected_session = None
    if trade_date is not None:
        reference_date = _as_date(trade_date)
        reference_source = "requested"
    else:
        expected_session = _latest_completed_session(research_db)
        if expected_session is not None:
            reference_date = _as_date(expected_session)
            reference_source = "trade_calendar"
        else:
            reference_date = research_latest
            reference_source = "research_latest"

    if reference_date is None:
        result["checks"]["freshness"] = {
            "pass": False,
            "error": "no data to compare",
        }
        result["checks"]["count"] = {"pass": False, "error": "no data to compare"}
        result["status"] = "FAIL"
        return result

    # count: declared classes, each with its own verdict and tolerance.
    research_types = _instrument_type_map(research_db)
    stable_types = _instrument_type_map(stable_db)
    research_counts, research_class_codes, research_excluded = _count_by_class(
        research_db, spec, reference_date, research_types
    )
    stable_counts, stable_class_codes, stable_excluded = _count_by_class(
        stable_db, spec, reference_date, stable_types
    )

    classes = {}
    classes_ok = True
    for name, class_spec in spec["count_classes"].items():
        research_count = research_counts.get(name, 0)
        stable_count = stable_counts.get(name, 0)
        diff = _relative_diff(research_count, stable_count)
        mode = class_spec.get("mode", "relative")
        entry = {
            "mode": mode,
            "research": research_count,
            "stable": stable_count,
            "relative_diff": round(diff, 6),
            "tolerance": tolerance,
        }
        if mode == "research_excludes":
            # The supported-universe rule keeps these rows out of research by
            # construction; the verdict is that research must not leak them.
            allowance = class_spec.get("allowance", 0)
            entry["allowance"] = allowance
            entry["pass"] = research_count <= allowance
            if not entry["pass"]:
                entry["error"] = "research contains unsupported-universe rows"
                classes_ok = False
        elif mode == "coverage_superset":
            min_rows = class_spec["min_research_rows"]
            missing_allowance = class_spec.get("missing_code_allowance", 0)
            research_codes = research_class_codes.get(name, set())
            stable_codes = stable_class_codes.get(name, set())
            missing_codes = sorted(stable_codes - research_codes)
            superset_ok = research_count >= stable_count
            floor_ok = research_count >= min_rows
            codes_ok = len(missing_codes) <= missing_allowance
            zero_ok = research_count > 0 and stable_count > 0
            entry.update(
                {
                    "min_research_rows": min_rows,
                    "missing_code_allowance": missing_allowance,
                    "research_codes": len(research_codes),
                    "stable_codes": len(stable_codes),
                    "missing_stable_code_count": len(missing_codes),
                    "missing_stable_codes": missing_codes[:20],
                    "pass": zero_ok and superset_ok and floor_ok and codes_ok,
                }
            )
            if not entry["pass"]:
                reasons = []
                if not zero_ok:
                    reasons.append("class has zero rows on at least one side")
                if not superset_ok:
                    reasons.append(
                        f"research count {research_count} < stable {stable_count}"
                    )
                if not floor_ok:
                    reasons.append(
                        f"research count {research_count} < min_research_rows "
                        f"{min_rows}"
                    )
                if not codes_ok:
                    reasons.append(
                        f"{len(missing_codes)} stable code(s) missing on research"
                    )
                entry["error"] = "; ".join(reasons)
                classes_ok = False
        else:
            if research_count == 0 or stable_count == 0:
                entry["pass"] = False
                entry["error"] = "class has zero rows on at least one side"
                classes_ok = False
            else:
                entry["pass"] = diff <= tolerance
                if not entry["pass"]:
                    entry["error"] = "relative count difference above tolerance"
                    classes_ok = False
        classes[name] = entry

    unclassified = {
        "research": research_excluded.get("unclassified", 0),
        "stable": stable_excluded.get("unclassified", 0),
    }
    unclassified_ok = unclassified["research"] == 0 and unclassified["stable"] == 0
    count_pass = classes_ok and unclassified_ok
    research_total = sum(research_counts.values())
    stable_total = sum(stable_counts.values())
    result["checks"]["count"] = {
        "trade_date": str(reference_date),
        "classes": classes,
        "universe_excluded": {
            "research": research_counts.get("unsupported_universe", 0),
            "stable": stable_counts.get("unsupported_universe", 0),
            "reason": (
                "unsupported universe is declared as its own class; research "
                "must not contain it (research_excludes)"
            ),
        },
        "unclassified": unclassified,
        "total": {
            "research": research_total,
            "stable": stable_total,
            "used_for_verdict": False,
        },
        "pass": count_pass,
    }
    if not classes_ok:
        _fail("count", "one or more declared count classes failed")
    if not unclassified_ok:
        _fail(
            "count",
            "rows could not be classified from basic_stock.object_type: "
            f"research={unclassified['research']} stable={unclassified['stable']}",
        )

    # freshness: the completed session under test must exist on both sides.
    # A raw latest date beyond the reference is an in-progress session and is
    # flagged, never silently treated as the reference.
    research_in_progress = (
        research_latest is not None and research_latest > reference_date
    )
    stable_in_progress = stable_latest is not None and stable_latest > reference_date
    freshness_ok = (
        research_total > 0
        and stable_total > 0
        and research_latest is not None
        and research_latest >= reference_date
        and stable_latest is not None
        and stable_latest >= reference_date
    )
    result["checks"]["freshness"] = {
        "reference_date": str(reference_date),
        "reference_source": reference_source,
        "expected_session": str(expected_session) if expected_session else None,
        "requested_trade_date": str(trade_date) if trade_date else None,
        "research_latest": str(research_latest) if research_latest else None,
        "stable_latest": str(stable_latest) if stable_latest else None,
        "research_in_progress": research_in_progress,
        "stable_in_progress": stable_in_progress,
        "research_rows_on_reference": research_total,
        "stable_rows_on_reference": stable_total,
        "pass": freshness_ok,
    }
    if not freshness_ok:
        message = "reference trade date missing on a side or a side is stale"
        if reference_source == "research_latest":
            message += (
                "; no A-share trade calendar available, reference is the latest "
                "research date and mutual staleness cannot be ruled out"
            )
        _fail("freshness", message)

    # samples: the complete same-day key set is the pool; the seeded draw is
    # taken from all of it, and the pool is checked against the raw day count so
    # a DB-side cap cannot silently bias the basket.
    picked = list(
        research_db[spec["collection"]].find(
            _same_day_query(spec, reference_date),
            projection=spec["keys"],
        )
    )
    day_rows = _count_day(research_db, spec, reference_date)
    pool_complete = len(picked) >= day_rows
    chosen = _choose_samples(picked, spec, samples)

    sample_results = []
    observed_union: set = set()
    undeclared_union: set = set()
    informational_union: list = []
    for probe in chosen:
        key_filter = {k: probe[k] for k in spec["keys"]}
        research_doc = research_db[spec["collection"]].find_one(key_filter)
        stable_doc = stable_db[spec["collection"]].find_one(key_filter)
        row_class = _row_class_for_sample(
            spec, research_doc, stable_doc, research_types, stable_types
        )
        scope = spec["row_classes"][row_class]
        comparison = _compare_sample(research_doc, stable_doc, scope, compare_derived)
        observed_union |= set(research_doc or {}) | set(stable_doc or {})
        undeclared_union |= set(comparison["undeclared"])
        informational_union.extend(comparison["informational"])
        sample_results.append(
            {
                "key": {k: str(probe[k]) for k in spec["keys"]},
                "row_class": row_class,
                "pass": not (
                    comparison["diffs"]
                    or comparison["undeclared"]
                    or comparison["presence_failures"]
                ),
                "diffs": comparison["diffs"],
                "undeclared": comparison["undeclared"],
                "presence_failures": comparison["presence_failures"],
                "informational": comparison["informational"],
            }
        )

    # Full-day key discovery: any undeclared key anywhere on the reference date
    # (not only in the sampled basket) fails by name.
    day_field_keys: dict = {}
    for db, types in ((research_db, research_types), (stable_db, stable_types)):
        for row_class, keys in _day_field_keys(db, spec, reference_date, types).items():
            day_field_keys.setdefault(row_class, set()).update(keys)
    discovered_undeclared: set = set()
    for row_class, keys in day_field_keys.items():
        scope = spec["row_classes"].get(row_class)
        if scope is None:
            discovered_undeclared.update(keys)
            continue
        field_class = _field_class_map(scope)
        discovered_undeclared.update(
            key for key in keys if len(field_class.get(key, [])) != 1
        )
    undeclared_union |= discovered_undeclared
    observed_union |= {key for keys in day_field_keys.values() for key in keys}

    compared = len(sample_results)
    failures = [s for s in sample_results if not s["pass"]]
    samples_ok = (
        pool_complete and compared >= samples and not failures and not undeclared_union
    )
    # De-duplicate the informational notes while keeping a deterministic order.
    informational_notes = sorted(set(informational_union))
    result["checks"]["samples"] = {
        "requested": samples,
        "compared": compared,
        "pool_size": len(picked),
        "day_rows": day_rows,
        "pool_complete": pool_complete,
        "compare_derived": compare_derived,
        "sampled_keys": [{k: str(probe[k]) for k in spec["keys"]} for probe in chosen],
        "observed_fields": sorted(observed_union),
        "undeclared_fields": sorted(undeclared_union),
        "day_discovery": {
            row_class: sorted(keys)
            for row_class, keys in sorted(day_field_keys.items())
        },
        "informational": informational_notes[:20],
        "informational_total": len(informational_notes),
        "pass": samples_ok,
        "failures": failures[:10],
    }
    if not pool_complete:
        _fail(
            "samples",
            f"sample pool incomplete: {len(picked)} < {day_rows} same-day keys",
        )
    elif compared < samples:
        _fail(
            "samples",
            f"insufficient samples: compared {compared} < requested {samples}",
        )
    elif undeclared_union:
        _fail(
            "samples",
            "undeclared fields observed: " + ", ".join(sorted(undeclared_union)),
        )
    elif failures:
        _fail("samples", "field mismatches or research-populated presence failures")

    return result


def _source_factor_map(frame):
    """Parse a tushare adj_factor frame like ``build_fq_factor_frame`` does.

    Returns ``(factors_by_date, invalid_rows)``; invalid/non-finite/
    non-positive rows are skipped exactly as the writer skips them, so a stray
    NaN does not false-FAIL acceptance.
    """
    factors: dict = {}
    invalid = 0
    for _, row in frame.iterrows():
        raw_date = row.get("trade_date")
        raw_factor = row.get("adj_factor")
        try:
            text = raw_date if isinstance(raw_date, str) else str(int(raw_date))
            day = datetime.date(int(text[0:4]), int(text[4:6]), int(text[6:8]))
        except (TypeError, ValueError):
            invalid += 1
            continue
        value = _finite_float(raw_factor)
        if value is None or value <= 0:
            invalid += 1
            continue
        factors[day] = value
    return factors, invalid


def _carry_forward(days, values, day):
    """Most recent known source factor at/before ``day`` (back-fill if earlier).

    Mirrors ``build_fq_factor_frame``: forward-fill for rows the source omits,
    with the earliest known factor used before the first source row.
    """
    index = bisect.bisect_right(days, day) - 1
    if index >= 0:
        return values[index]
    return values[0]


def _finite_float(value):
    """Return ``value`` as a finite float, or ``None`` when it is not usable.

    Only real numeric types are accepted: bools and non-numeric types
    (including numeric strings) are rejected rather than coerced.
    """
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    number = float(value)
    return number if math.isfinite(number) else None


def _source_check_code(code: str, rows: list, tushare) -> dict:
    """Verify one code's stored FQ fields against the tushare adj_factor series."""
    entry: dict = {"code": code, "rows": len(rows), "pass": False}
    try:
        ts_code = tushare.to_tushare_ts_code(code)
    except Exception as exc:
        entry["error"] = f"ts_code mapping failed: {type(exc).__name__}: {exc}"
        return entry
    days = sorted(
        _as_date(row.get("date")) for row in rows if row.get("date") is not None
    )
    if not days:
        entry["error"] = "no dated research rows for code"
        return entry
    start = days[0].strftime("%Y%m%d")
    end = days[-1].strftime("%Y%m%d")
    try:
        frame = tushare.adj_factor(ts_code, start, end)
    except Exception as exc:
        entry["error"] = f"tushare adj_factor failed: {type(exc).__name__}: {exc}"
        return entry
    if frame is None or getattr(frame, "empty", False):
        entry["error"] = "tushare adj_factor returned no rows"
        return entry
    factors, invalid = _source_factor_map(frame)
    if not factors:
        entry["error"] = "tushare adj_factor returned no usable factor values" + (
            f" ({invalid} invalid row(s) skipped)" if invalid else ""
        )
        return entry
    if invalid:
        # Mirrors build_fq_factor_frame: invalid source rows are skipped, not a
        # failure, as long as usable factors remain.
        entry["invalid_source_rows_skipped"] = invalid

    ordered = sorted(factors.items())
    factor_days = [day for day, _ in ordered]
    factor_values = [value for _, value in ordered]
    failures = []
    for row in rows:
        day = _as_date(row.get("date"))
        if day is None:
            failures.append("row without a date")
            continue
        expected = _carry_forward(factor_days, factor_values, day)
        factor = _finite_float(row.get("fq_factor"))
        if factor is None:
            failures.append(f"{day}: fq_factor missing or non-numeric")
            continue
        if abs(factor - expected) > FQ_SOURCE_TOLERANCE:
            failures.append(
                f"{day}: fq_factor {row.get('fq_factor')!r} != source {expected!r}"
            )
            continue
        close = _finite_float(row.get("close"))
        if close is None or close == 0:
            failures.append(f"{day}: close missing, zero or non-numeric")
            continue
        expected_close_hfq = round(close * factor, 4)
        close_hfq = _finite_float(row.get("close_hfq"))
        if (
            close_hfq is None
            or abs(close_hfq - expected_close_hfq) > FQ_SOURCE_TOLERANCE
        ):
            failures.append(
                f"{day}: close_hfq {row.get('close_hfq')!r} != "
                f"round(close*fq_factor, 4) {expected_close_hfq!r}"
            )
            continue
        ratio = close_hfq / close
        for field in ("open", "high", "low"):
            raw = _finite_float(row.get(field))
            adjusted = _finite_float(row.get(f"{field}_hfq"))
            if raw is None or adjusted is None:
                failures.append(f"{day}: {field}/{field}_hfq missing or non-numeric")
                continue
            expected_hfq = round(raw * ratio, 4)
            if abs(adjusted - expected_hfq) > FQ_SOURCE_TOLERANCE:
                failures.append(
                    f"{day}: {field}_hfq {row.get(f'{field}_hfq')!r} != "
                    f"round({field}*close_ratio, 4) {expected_hfq!r}"
                )
        if len(failures) >= 10:
            break
    entry["pass"] = not failures
    if failures:
        entry["failures"] = failures[:10]
    return entry


def _source_check(research_db, trade_date, samples: int, tushare=None) -> dict:
    """Source-of-truth acceptance for sampled quote codes against tushare.

    Never consults legacy stable (and always the quote collection, regardless
    of ``--collections``): a tushare error, empty response or unusable factor
    series FAILS the run outright, and fewer sampled codes than requested also
    FAILS rather than silently checking a smaller basket.
    """
    tolerances = {
        "fq_factor": FQ_SOURCE_TOLERANCE,
        "close_hfq": FQ_SOURCE_TOLERANCE,
        "ohlc_hfq": FQ_SOURCE_TOLERANCE,
        "note": (
            "stored FQ/HFQ values are rounded to 4 decimals; |diff| <= 5e-4 is a "
            "rounding artifact. Invalid source rows are skipped like the writer; "
            "the check fails only when no usable factor remains."
        ),
    }
    if tushare is None:
        # Lazy import: keep the module importable without network/tushare.
        from app.lib.datahub.data_source.interface import tushare_interface

        tushare = tushare_interface

    spec = COLLECTION_SPECS["quote"]
    reference_date = trade_date
    reference_source = "requested"
    if reference_date is None:
        reference_date = _latest_completed_session(research_db)
        reference_source = "trade_calendar"
        if reference_date is None:
            reference_date = _latest_date(research_db, spec)
            reference_source = "research_latest"
    result: dict = {
        "mode": "tushare_adj_factor",
        "requested_trade_date": str(trade_date) if trade_date else None,
        "trade_date": str(reference_date) if reference_date else None,
        "reference_source": reference_source,
        "collection": spec["collection"],
        "tolerances": tolerances,
        "pass": False,
        "codes": [],
    }
    if reference_date is None:
        result["error"] = "no research quote data to verify against tushare"
        return result

    query = _same_day_query(spec, reference_date)
    observed_codes = {
        str(doc[spec["code_field"]])
        for doc in research_db[spec["collection"]].find(
            query, projection={spec["code_field"]: 1}
        )
        if doc.get(spec["code_field"]) is not None
    }
    # FQ/HFQ is written for supported individual stocks only; index rows carry
    # no adj_factor series, so exclude them (and the unsupported universe) by
    # the same instrument-type source the count classes use.
    research_types = _instrument_type_map(research_db)
    codes = sorted(
        code
        for code in observed_codes
        if not data_capability_helper.is_bse_stock_code(code)
        and research_types.get(code) == "individual_stock"
    )
    result["available_supported_codes"] = len(codes)
    if len(codes) < samples:
        result["error"] = (
            "insufficient supported individual-stock codes for the source "
            f"check: {len(codes)} < requested {samples}"
        )
        return result

    chosen = random.Random(0).sample(codes, samples)
    entries = []
    for code in sorted(chosen):
        rows = list(research_db[spec["collection"]].find({spec["code_field"]: code}))
        entries.append(_source_check_code(code, rows, tushare))

    failures = [entry for entry in entries if not entry["pass"]]
    result["requested_codes"] = samples
    result["checked_codes"] = len(entries)
    result["codes"] = entries
    result["failures"] = failures[:10]
    result["pass"] = not failures
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
    compare_derived: bool = False,
    verify_source: bool = True,
    tushare=None,
) -> dict:
    """Run all checks and return the report dict (also used directly by tests)."""
    if not aliases:
        raise ValueError("no collections selected; refusing a vacuous verification")
    _validate_declared_scope()
    if research_db is None:
        research_db = _research_db()
    if stable_client is None:
        stable_client = _build_stable_client()
    stable_db = _stable_db(stable_client)

    started = datetime.datetime.now(datetime.UTC)
    results = [
        _check_collection(
            alias,
            research_db,
            stable_db,
            trade_date,
            samples,
            tolerance,
            compare_derived,
        )
        for alias in aliases
    ]

    source_result = None
    if verify_source:
        source_result = _source_check(research_db, trade_date, samples, tushare=tushare)

    failed = [r for r in results if r["status"] != "PASS"]
    source_failed = source_result is not None and not source_result["pass"]
    status = "PASS" if not failed and not source_failed else "FAIL"
    summary = {
        "status": status,
        "r1_acceptance": status == "PASS" and source_result is not None,
        "source_check_required_for_r1": True,
        "source_check_skipped": not verify_source,
        "trade_date": str(trade_date) if trade_date else None,
        "tolerance": tolerance,
        "samples": samples,
        "compare_derived": compare_derived,
        "verify_source_tushare": verify_source,
        "scope_version": SCOPE_VERSION,
        "scope_reason": SCOPE_REASON,
        "derived_field_reason": DERIVED_FIELD_REASON,
        "collections": results,
        "source_check": source_result,
        "started_at": started.isoformat(),
        "finished_at": datetime.datetime.now(datetime.UTC).isoformat(),
    }
    if not verify_source:
        summary["warning"] = (
            "tushare source check skipped (--skip-source-tushare); a PASS here "
            "is NOT an R1 acceptance"
        )
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
            "compare_derived": args.compare_derived,
            "verify_source_tushare": args.verify_source_tushare,
            "scope_version": SCOPE_VERSION,
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
            compare_derived=args.compare_derived,
            verify_source=args.verify_source_tushare,
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


# Fail closed at import: an ambiguous declaration or a missing provenance entry
# must not be able to run at all.
_validate_declared_scope()


if __name__ == "__main__":
    main()
