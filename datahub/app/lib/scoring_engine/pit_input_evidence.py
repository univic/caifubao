"""Forward-only immutable inputs for causal ranked scoring research.

P2a captures evidence; it deliberately does not generate predictions.  A
future consumer must validate these artifacts before deriving P1-compatible
ranked rows.
"""

from __future__ import annotations

import datetime as dt
from decimal import Decimal, InvalidOperation, ROUND_HALF_EVEN
import hashlib
import json
import math
import os
from pathlib import Path
import tempfile
from typing import Any, Iterable, Mapping, Sequence
from zoneinfo import ZoneInfo

from app.lib.scoring_engine.config import (
    SUPPORTED_HORIZONS,
    get_effective_horizon_config,
    model_config_hash,
)
from app.model.industry import IndustryDailyMetrics, StockIndustryClassification
from app.model.scoring import ScoreModelVersion
from app.model.signal import StockSignalDaily
from app.model.factor import StockFactorDaily
from app.model.stock import FinanceMarket, IndividualStock, StockDailyQuote


SCHEMA_UNIVERSE = "stock-pit-universe-v1"
SCHEMA_INPUTS = "stock-ranked-inputs-v1"
PRODUCER_UNIVERSE = "caifubao.datahub.stock_pit_universe"
PRODUCER_INPUTS = "caifubao.datahub.stock_ranked_inputs"
BUILD_REVISION_ENV = "CAIFUBAO_BUILD_REVISION"
BEIJING = ZoneInfo("Asia/Shanghai")
UTC = dt.UTC
SCALE = 100_000_000
DISCLAIMER = "Research-only evidence; not investment or trading advice."
HASH_FIELDS = (
    "schema_version",
    "producer",
    "as_of",
    "grade",
    "input_snapshot",
    "field_manifest",
    "payload",
)

QUOTE_FIELDS = (
    "code",
    "date",
    "open",
    "close",
    "previous_close",
    "high",
    "low",
    "trade_status",
    "isST",
    "close_hfq",
    "open_hfq",
    "high_hfq",
    "low_hfq",
)
FACTOR_FIELDS = (
    "stock_code",
    "date",
    "ma_10",
    "ma_20",
    "ma_30",
    "ma_60",
    "ma_120",
)
SIGNAL_FIELDS = ("stock_code", "date", "signal_name", "direction", "strength")
INDUSTRY_FIELDS = (
    "stock_code",
    "industry_code_sw_l1",
    "industry_name_sw_l1",
    "assigned_at",
)
INDUSTRY_METRIC_FIELDS = (
    "industry_code",
    "industry_name",
    "date",
    "horizon",
    "model_version",
    "stock_count",
    "avg_score",
    "max_score",
    "min_score",
    "std_dev_score",
    "avg_percentile",
    "avg_rank",
    "buy_count",
    "watch_count",
    "avoid_count",
)
SOURCE_SCHEMAS = {
    "universe": {
        "fields": ("code", "name"),
        "key_fields": ("code",),
        "required_text_fields": {"code", "name"},
    },
    "industry_classification": {
        "fields": INDUSTRY_FIELDS,
        "key_fields": ("stock_code",),
        "required_text_fields": {"stock_code"},
    },
    "quotes": {
        "fields": QUOTE_FIELDS,
        "key_fields": ("code", "date"),
        "required_text_fields": {"code"},
        "float_fields": {
            "open",
            "close",
            "previous_close",
            "high",
            "low",
            "close_hfq",
            "open_hfq",
            "high_hfq",
            "low_hfq",
        },
        "int_fields": {"trade_status", "isST"},
    },
    "fallback_quotes": {
        "fields": QUOTE_FIELDS,
        "key_fields": ("code", "date"),
        "required_text_fields": {"code"},
        "float_fields": {
            "open",
            "close",
            "previous_close",
            "high",
            "low",
            "close_hfq",
            "open_hfq",
            "high_hfq",
            "low_hfq",
        },
        "int_fields": {"trade_status", "isST"},
    },
    "index_quotes": {
        "fields": QUOTE_FIELDS,
        "key_fields": ("code", "date"),
        "required_text_fields": {"code"},
        "float_fields": {
            "open",
            "close",
            "previous_close",
            "high",
            "low",
            "close_hfq",
            "open_hfq",
            "high_hfq",
            "low_hfq",
        },
        "int_fields": {"trade_status", "isST"},
    },
    "factors": {
        "fields": FACTOR_FIELDS,
        "key_fields": ("stock_code", "date"),
        "required_text_fields": {"stock_code"},
        "float_fields": {"ma_10", "ma_20", "ma_30", "ma_60", "ma_120"},
    },
    "signals": {
        "fields": SIGNAL_FIELDS,
        "key_fields": ("stock_code", "date", "signal_name"),
        "required_text_fields": {"stock_code", "signal_name", "direction"},
        "float_fields": {"strength"},
    },
    "industry_metrics": {
        "fields": INDUSTRY_METRIC_FIELDS,
        "key_fields": ("industry_code", "date", "horizon", "model_version"),
        "required_text_fields": {"industry_code", "model_version"},
        "float_fields": {
            "avg_score",
            "max_score",
            "min_score",
            "std_dev_score",
            "avg_percentile",
            "avg_rank",
        },
        "int_fields": {
            "horizon",
            "stock_count",
            "buy_count",
            "watch_count",
            "avoid_count",
        },
    },
}
UNIVERSE_SOURCES = {"calendar", "industry_classification", "universe"}
INPUT_SOURCES = {
    "calendar",
    "factors",
    "fallback_quotes",
    "industry_metrics",
    "index_quotes",
    "quotes",
    "signals",
}
UNIVERSE_INPUTS = ("code", "calendar", "universe", "industry_classification")
INPUT_INPUTS = (
    "code",
    "model",
    "universe_artifact",
    "calendar",
    "quotes",
    "index_quotes",
    "factors",
    "fallback_quotes",
    "signals",
    "industry_metrics",
)


def _rfc3339(value: dt.datetime) -> str:
    if not isinstance(value, dt.datetime) or value.tzinfo is None:
        raise ValueError("timestamp must be timezone-aware")
    return value.astimezone(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")


def _instant(value: Any, field: str) -> dt.datetime:
    if isinstance(value, str):
        try:
            value = dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError as exc:
            raise ValueError(f"{field} must be timezone-aware RFC3339") from exc
    if not isinstance(value, dt.datetime) or value.tzinfo is None:
        raise ValueError(f"{field} must be timezone-aware RFC3339")
    return value.astimezone(UTC)


def _date(value: Any, field: str) -> dt.date:
    if isinstance(value, dt.datetime):
        return value.date()
    if isinstance(value, dt.date):
        return value
    if isinstance(value, str):
        try:
            return dt.date.fromisoformat(value)
        except ValueError as exc:
            raise ValueError(f"{field} must be YYYY-MM-DD") from exc
    raise ValueError(f"{field} must be YYYY-MM-DD")


def canonical_json(value: Any) -> bytes:
    return json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")


def content_hash(value: Any) -> str:
    return hashlib.sha256(canonical_json(value)).hexdigest()


def _scaled_float(value: float) -> dict[str, int]:
    if not math.isfinite(value):
        raise ValueError("non-finite floating source value")
    try:
        source = Decimal(str(value))
        scaled = (source * SCALE).quantize(Decimal("1"), rounding=ROUND_HALF_EVEN)
    except InvalidOperation as exc:
        raise ValueError("invalid floating source value") from exc
    if Decimal(int(scaled)) / SCALE != source:
        raise ValueError("floating source value exceeds declared 1e-8 precision")
    return {"scaled_1e8": int(scaled)}


def normalize_value(value: Any) -> Any:
    """Normalize source data to the shared artifact's integer-only JSON."""
    if value is None or isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, float):
        return _scaled_float(value)
    if isinstance(value, dt.datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=UTC)
        return _rfc3339(value)
    if isinstance(value, dt.date):
        return value.isoformat()
    if isinstance(value, Mapping):
        return {str(key): normalize_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [normalize_value(item) for item in value]
    # ObjectId, lazy references, numpy scalar identifiers, and enums are only
    # provenance identifiers here; their stable lexical value is sufficient.
    return str(value)


def denormalize_value(value: Any) -> Any:
    """Decode fixed-unit values for deterministic downstream validation."""
    if isinstance(value, Mapping):
        if set(value) == {"scaled_1e8"}:
            scaled = value["scaled_1e8"]
            if isinstance(scaled, bool) or not isinstance(scaled, int):
                raise ValueError("scaled_1e8 must be an integer")
            return float(Decimal(scaled) / SCALE)
        return {str(key): denormalize_value(item) for key, item in value.items()}
    if isinstance(value, list):
        return [denormalize_value(item) for item in value]
    return value


def _contains_float(value: Any) -> bool:
    if isinstance(value, float):
        return True
    if isinstance(value, Mapping):
        return any(_contains_float(item) for item in value.values())
    if isinstance(value, list):
        return any(_contains_float(item) for item in value)
    return False


def _schema_paths(value: Any, path: str) -> dict[str, Any]:
    paths: dict[str, Any] = {path: value}
    if isinstance(value, Mapping):
        for key, item in value.items():
            paths.update(_schema_paths(item, f"{path}/{key}"))
    elif isinstance(value, list) and value:
        # Artifact source rows have one fixed producer schema. Inspecting one
        # normalized row avoids walking hundreds of thousands of history rows
        # solely to rediscover the same manifest paths.
        paths.update(_schema_paths(value[0], f"{path}/*"))
    return paths


def _declared_source_paths() -> tuple[dict[str, str], dict[str, str]]:
    """Return source-row paths and numeric units from the producer schemas."""
    paths: dict[str, str] = {}
    units: dict[str, str] = {}
    for source_name, schema in SOURCE_SCHEMAS.items():
        float_fields = schema.get("float_fields", set())
        int_fields = schema.get("int_fields", set())
        for field in schema["fields"]:
            path = f"payload/sources/{source_name}/*/{field}"
            paths[path] = source_name
            if field in float_fields:
                scaled_path = f"{path}/scaled_1e8"
                paths[scaled_path] = source_name
                units[scaled_path] = "1e-8 source unit"
            elif field in int_fields:
                units[path] = "source integer"
    return paths, units


def _validate_source_rows(name: str, rows: Sequence[Any]) -> None:
    schema = SOURCE_SCHEMAS.get(name)
    if schema is None:
        return
    expected_fields = set(schema["fields"])
    float_fields = schema.get("float_fields", set())
    int_fields = schema.get("int_fields", set())
    key_fields = schema["key_fields"]
    required_text_fields = schema.get("required_text_fields", set())
    previous_key = None
    seen = set()
    for index, row in enumerate(rows):
        if not isinstance(row, Mapping) or set(row) != expected_fields:
            raise ValueError(f"source row schema mismatch: {name}[{index}]")
        for field in key_fields:
            value = row[field]
            if value is None or (isinstance(value, str) and not value.strip()):
                raise ValueError(f"source business key is empty: {name}.{field}")
        for field in required_text_fields:
            value = row[field]
            if not isinstance(value, str) or not value.strip():
                raise ValueError(f"source text field is invalid: {name}.{field}")
        for field in float_fields:
            value = row[field]
            if value is None:
                continue
            if not isinstance(value, Mapping) or set(value) != {"scaled_1e8"}:
                raise ValueError(f"source fixed-unit value mismatch: {name}.{field}")
            scaled = value["scaled_1e8"]
            if isinstance(scaled, bool) or not isinstance(scaled, int):
                raise ValueError(f"source fixed-unit value mismatch: {name}.{field}")
        for field in int_fields:
            value = row[field]
            if value is not None and (
                isinstance(value, bool) or not isinstance(value, int)
            ):
                raise ValueError(f"source integer value mismatch: {name}.{field}")
        for field in expected_fields & {"date", "assigned_at"}:
            value = row[field]
            if value is None:
                continue
            instant = _instant(value, f"source.{name}.{field}")
            if value != _rfc3339(instant):
                raise ValueError(f"source instant is not canonical: {name}.{field}")
        key = tuple(str(row.get(field) or "") for field in key_fields)
        if key in seen:
            raise ValueError(f"duplicate source business key: {name}[{index}]")
        if previous_key is not None and key < previous_key:
            raise ValueError(f"source rows are not sorted: {name}")
        seen.add(key)
        previous_key = key


def _validate_payload_declaration(
    path: str,
    declaration: Any,
    code_version: str,
    expected_inputs: Sequence[str] | None = None,
) -> Mapping[str, Any]:
    if not isinstance(declaration, Mapping):
        raise ValueError(f"field manifest missing path: {path}")
    if declaration.get("kind") != "payload" or declaration.get("derived") is not False:
        raise ValueError(f"field manifest payload classification mismatch: {path}")
    if declaration.get("producer_version") != code_version:
        raise ValueError(f"field producer version mismatch: {path}")
    if not declaration.get("inputs"):
        raise ValueError(f"field input dependencies missing: {path}")
    if expected_inputs is not None and declaration.get("inputs") != list(
        expected_inputs
    ):
        raise ValueError(f"field input dependencies mismatch: {path}")
    return declaration


def _field_dependencies(path: str, payload_inputs: Sequence[str]) -> list[str]:
    parts = path.split("/")
    if len(parts) >= 2 and parts[0] in {"payload", "input_snapshot"}:
        if parts[1] == "sources" and len(parts) >= 3:
            return [parts[2]]
        if parts[1] in {"model", "model_config"}:
            return ["model"]
        if parts[1].startswith("universe_artifact"):
            return ["universe_artifact"]
        if parts[1] == "session":
            return ["calendar"]
        if parts[1] == "code":
            return ["code"]
    return list(payload_inputs)


def _field_manifest(
    *,
    build_revision: str,
    input_snapshot: Mapping[str, Any],
    payload: Mapping[str, Any],
    payload_inputs: Sequence[str],
) -> dict:
    fields = {
        name: {
            "kind": "payload",
            "derived": False,
            "producer_version": build_revision,
            "inputs": list(payload_inputs),
        }
        for name in (
            "schema_version",
            "as_of",
            "producer",
            "grade",
            "input_snapshot",
            "field_manifest",
            "payload",
        )
    }
    for name in ("artifact_id", "artifact_hash", "data_as_of"):
        fields[name] = {
            "kind": "narrative",
            "derived": True,
            "producer_version": build_revision,
            "inputs": list(payload_inputs),
        }
    fields["disclaimer"] = {"kind": "narrative", "derived": False}
    fields["generated_at"] = {"kind": "lifecycle", "derived": False}
    numeric_units = {}
    for root_name, root_value in (
        ("input_snapshot", input_snapshot),
        ("payload", payload),
    ):
        for path, value in _schema_paths(root_value, root_name).items():
            fields[path] = {
                "kind": "payload",
                "derived": False,
                "producer_version": build_revision,
                "inputs": _field_dependencies(path, payload_inputs),
            }
            if isinstance(value, int) and not isinstance(value, bool):
                if path.endswith("/scaled_1e8"):
                    numeric_units[path] = "1e-8 source unit"
                elif path.endswith("/row_count"):
                    numeric_units[path] = "row"
                else:
                    numeric_units[path] = "source integer"
    source_paths, source_units = _declared_source_paths()
    for path, source_name in source_paths.items():
        fields[path] = {
            "kind": "payload",
            "derived": False,
            "producer_version": build_revision,
            "inputs": [source_name],
        }
    numeric_units.update(source_units)
    return {
        "asserted_field_set": ["payload"],
        "fields": fields,
        "numeric_units": numeric_units,
    }


def build_artifact(
    *,
    schema_version: str,
    producer: str,
    as_of: dt.datetime,
    generated_at: dt.datetime,
    build_revision: str,
    input_snapshot: Mapping[str, Any],
    payload: Mapping[str, Any],
    payload_inputs: Sequence[str],
) -> dict:
    revision = str(build_revision or "").strip()
    if not revision:
        raise ValueError("build revision is required")
    as_of_text = _rfc3339(as_of)
    generated_text = _rfc3339(generated_at)
    normalized_snapshot = dict(input_snapshot)
    normalized_payload = dict(payload)
    if _contains_float(normalized_snapshot) or _contains_float(normalized_payload):
        raise ValueError("artifact inputs must be normalized before publication")
    manifest = _field_manifest(
        build_revision=revision,
        input_snapshot=normalized_snapshot,
        payload=normalized_payload,
        payload_inputs=payload_inputs,
    )
    hashed = {
        "schema_version": schema_version,
        "producer": producer,
        "as_of": as_of_text,
        "grade": "RESEARCH",
        "input_snapshot": normalized_snapshot,
        "field_manifest": manifest,
        "payload": normalized_payload,
    }
    if _contains_float(hashed):
        raise ValueError("hashed artifact content must not contain JSON floats")
    artifact_hash = content_hash(hashed)
    artifact_id = (
        "sha256:"
        + hashlib.sha256(
            f"{producer}\n{as_of_text}\n{artifact_hash}".encode("utf-8")
        ).hexdigest()
    )
    return {
        **hashed,
        "artifact_id": artifact_id,
        "artifact_hash": artifact_hash,
        "generated_at": generated_text,
        "data_as_of": as_of_text,
        "disclaimer": DISCLAIMER,
    }


def validate_artifact(artifact: Mapping[str, Any]) -> dict:
    if not isinstance(artifact, Mapping):
        raise ValueError("artifact must be an object")
    missing = {
        "artifact_id",
        "artifact_hash",
        "generated_at",
        "data_as_of",
        *HASH_FIELDS,
    } - set(artifact)
    if missing:
        raise ValueError("artifact missing fields: " + ", ".join(sorted(missing)))
    if artifact.get("grade") != "RESEARCH" or artifact.get("disclaimer") != DISCLAIMER:
        raise ValueError("artifact must remain research-only")
    as_of = _instant(artifact["as_of"], "as_of")
    generated_at = _instant(artifact["generated_at"], "generated_at")
    if artifact["as_of"] != _rfc3339(as_of):
        raise ValueError("as_of must use canonical UTC Z form")
    if artifact["generated_at"] != _rfc3339(generated_at):
        raise ValueError("generated_at must use canonical UTC Z form")
    if generated_at < as_of:
        raise ValueError("generated_at must not precede as_of")
    if _instant(artifact["data_as_of"], "data_as_of") != as_of or artifact[
        "data_as_of"
    ] != _rfc3339(as_of):
        raise ValueError("data_as_of must equal as_of")
    hashed = {field: artifact[field] for field in HASH_FIELDS}
    if _contains_float(hashed):
        raise ValueError("hashed artifact content must not contain JSON floats")
    expected_hash = content_hash(hashed)
    if artifact["artifact_hash"] != expected_hash:
        raise ValueError("artifact_hash mismatch")
    expected_id = (
        "sha256:"
        + hashlib.sha256(
            f"{artifact['producer']}\n{artifact['as_of']}\n{expected_hash}".encode(
                "utf-8"
            )
        ).hexdigest()
    )
    if artifact["artifact_id"] != expected_id:
        raise ValueError("artifact_id mismatch")
    snapshot = artifact.get("input_snapshot")
    if not isinstance(snapshot, Mapping):
        raise ValueError("input_snapshot must be an object")
    payload = artifact.get("payload")
    if not isinstance(payload, Mapping):
        raise ValueError("payload must be an object")
    code_version = (snapshot.get("code") or {}).get("version")
    if not isinstance(code_version, str) or not code_version.strip():
        raise ValueError("input_snapshot.code.version is required")
    expected_payload_inputs = {
        SCHEMA_UNIVERSE: UNIVERSE_INPUTS,
        SCHEMA_INPUTS: INPUT_INPUTS,
    }.get(artifact["schema_version"])
    sources = snapshot.get("sources")
    payload_sources = payload.get("sources")
    if not isinstance(sources, Mapping) or not isinstance(payload_sources, Mapping):
        raise ValueError("artifact sources must be objects")
    if set(sources) != set(payload_sources):
        raise ValueError("input and payload source names must match")
    expected_sources = {
        SCHEMA_UNIVERSE: UNIVERSE_SOURCES,
        SCHEMA_INPUTS: INPUT_SOURCES,
    }.get(artifact["schema_version"])
    if expected_sources is not None and set(sources) != expected_sources:
        raise ValueError("artifact source names do not match schema")
    for name, source in sources.items():
        if not isinstance(source, Mapping) or set(source) != {
            "content_hash",
            "row_count",
            "as_of",
        }:
            raise ValueError(f"source snapshot must be an object: {name}")
        rows = payload_sources[name]
        if not isinstance(rows, list):
            raise ValueError(f"payload source must be a list: {name}")
        _validate_source_rows(name, rows)
        source_hash = source.get("content_hash")
        if (
            not isinstance(source_hash, str)
            or len(source_hash) != 64
            or any(character not in "0123456789abcdef" for character in source_hash)
            or source_hash != content_hash(rows)
        ):
            raise ValueError(f"source hash mismatch: {name}")
        row_count = source.get("row_count")
        if (
            isinstance(row_count, bool)
            or not isinstance(row_count, int)
            or row_count != len(rows)
        ):
            raise ValueError(f"source row_count mismatch: {name}")
        source_as_of = _instant(source.get("as_of"), f"source.{name}.as_of")
        if source.get("as_of") != _rfc3339(source_as_of) or source_as_of != as_of:
            raise ValueError(f"source as_of mismatch: {name}")
    field_manifest = artifact.get("field_manifest")
    fields = (
        field_manifest.get("fields") if isinstance(field_manifest, Mapping) else None
    )
    units = (
        field_manifest.get("numeric_units")
        if isinstance(field_manifest, Mapping)
        else None
    )
    if not isinstance(fields, Mapping) or not isinstance(units, Mapping):
        raise ValueError("field_manifest fields and numeric_units are required")
    if field_manifest.get("asserted_field_set") != ["payload"]:
        raise ValueError("field manifest asserted_field_set mismatch")
    for path in HASH_FIELDS:
        _validate_payload_declaration(
            path,
            fields.get(path),
            code_version,
            expected_payload_inputs,
        )
    for path in ("artifact_id", "artifact_hash", "data_as_of"):
        declaration = fields.get(path)
        if not isinstance(declaration, Mapping) or (
            declaration.get("kind") != "narrative"
            or declaration.get("derived") is not True
            or declaration.get("producer_version") != code_version
            or not declaration.get("inputs")
            or (
                expected_payload_inputs is not None
                and declaration.get("inputs") != list(expected_payload_inputs)
            )
        ):
            raise ValueError(f"field manifest narrative mismatch: {path}")
    expected_non_payload = {
        "generated_at": ("lifecycle", False),
        "disclaimer": ("narrative", False),
    }
    for path, (kind, derived) in expected_non_payload.items():
        declaration = fields.get(path)
        if not isinstance(declaration, Mapping) or (
            declaration.get("kind") != kind or declaration.get("derived") is not derived
        ):
            raise ValueError(f"field manifest classification mismatch: {path}")
    source_paths, source_units = _declared_source_paths()
    for path, source_name in source_paths.items():
        declaration = _validate_payload_declaration(
            path, fields.get(path), code_version
        )
        if declaration.get("inputs") != [source_name]:
            raise ValueError(f"field input dependencies mismatch: {path}")
    for path, unit in source_units.items():
        if units.get(path) != unit:
            raise ValueError(f"numeric unit mismatch: {path}")
    for path in (
        *HASH_FIELDS,
        "artifact_id",
        "artifact_hash",
        "generated_at",
        "data_as_of",
        "disclaimer",
    ):
        if path not in fields:
            raise ValueError(f"field manifest missing path: {path}")
    for root_name, root_value in (
        ("input_snapshot", snapshot),
        ("payload", artifact["payload"]),
    ):
        for path, value in _schema_paths(root_value, root_name).items():
            _validate_payload_declaration(
                path,
                fields.get(path),
                code_version,
                (
                    _field_dependencies(path, expected_payload_inputs)
                    if expected_payload_inputs is not None
                    else None
                ),
            )
            if (
                isinstance(value, int)
                and not isinstance(value, bool)
                and path not in units
            ):
                raise ValueError(f"numeric unit missing: {path}")
    schema = artifact["schema_version"]
    if schema == SCHEMA_UNIVERSE:
        if artifact["producer"] != PRODUCER_UNIVERSE:
            raise ValueError("universe artifact producer mismatch")
        session = _date(payload.get("session"), "payload.session")
        raw_calendar = payload_sources["calendar"]
        calendar = _calendar_dates(raw_calendar)
        if raw_calendar != [value.isoformat() for value in calendar]:
            raise ValueError("calendar source must be sorted unique ISO dates")
        if not payload_sources["universe"]:
            raise ValueError("captured universe is empty")
        validate_capture_window(
            session=session, calendar=calendar, now=as_of, phase="universe"
        )
        validate_capture_window(
            session=session,
            calendar=calendar,
            now=generated_at,
            phase="universe",
        )
        for row in payload_sources["industry_classification"]:
            assigned_at = row.get("assigned_at")
            if (
                assigned_at
                and _instant(assigned_at, "industry_classification.assigned_at") > as_of
            ):
                raise ValueError("industry classification assigned_at is in the future")
    elif schema == SCHEMA_INPUTS:
        if artifact["producer"] != PRODUCER_INPUTS:
            raise ValueError("input artifact producer mismatch")
        session = _date(payload.get("session"), "payload.session")
        raw_calendar = payload_sources["calendar"]
        calendar = _calendar_dates(raw_calendar)
        if raw_calendar != [value.isoformat() for value in calendar]:
            raise ValueError("calendar source must be sorted unique ISO dates")
        _, session_close, _ = validate_capture_window(
            session=session, calendar=calendar, now=generated_at, phase="inputs"
        )
        if as_of != session_close.astimezone(UTC):
            raise ValueError("input artifact as_of must equal session close")
        for source_name in (
            "quotes",
            "fallback_quotes",
            "index_quotes",
            "factors",
            "signals",
            "industry_metrics",
        ):
            _require_rows_not_after(
                payload_sources[source_name],
                date_field="date",
                cutoff=session_close,
                source=source_name,
            )
        model = snapshot.get("model") or {}
        if not isinstance(model, Mapping) or set(model) != {
            "model_version",
            "config_hash",
            "scoring_mode",
            "horizons",
        }:
            raise ValueError("input_snapshot.model pins are incomplete")
        if (
            not isinstance(model["model_version"], str)
            or not model["model_version"].strip()
        ):
            raise ValueError("input_snapshot.model.model_version is required")
        if model["scoring_mode"] != "ranked":
            raise ValueError("input_snapshot.model.scoring_mode must be ranked")
        horizons = model["horizons"]
        if (
            not isinstance(horizons, list)
            or not horizons
            or any(
                isinstance(value, bool)
                or not isinstance(value, int)
                or value not in SUPPORTED_HORIZONS
                for value in horizons
            )
            or horizons != sorted(set(horizons))
        ):
            raise ValueError("input_snapshot.model.horizons are invalid")
        config_hash = model["config_hash"]
        if (
            not isinstance(config_hash, str)
            or len(config_hash) != 64
            or any(character not in "0123456789abcdef" for character in config_hash)
        ):
            raise ValueError("input_snapshot.model.config_hash is invalid")
        raw_config = denormalize_value(payload.get("model_config"))
        if not isinstance(raw_config, dict):
            raise ValueError("payload.model_config must be an object")
        if model.get("config_hash") != model_config_hash(raw_config):
            raise ValueError("model config hash mismatch")
        universe_input = snapshot.get("universe_artifact") or {}
        if not isinstance(universe_input, Mapping) or set(universe_input) != {
            "artifact_id",
            "artifact_hash",
            "as_of",
        }:
            raise ValueError("input_snapshot.universe_artifact is incomplete")
        universe_as_of = _instant(
            universe_input["as_of"], "input_snapshot.universe_artifact.as_of"
        )
        if universe_input["as_of"] != _rfc3339(universe_as_of):
            raise ValueError("universe artifact as_of must use canonical UTC Z form")
        if (
            not isinstance(universe_input["artifact_id"], str)
            or not universe_input["artifact_id"].startswith("sha256:")
            or len(universe_input["artifact_id"]) != 71
            or any(
                character not in "0123456789abcdef"
                for character in universe_input["artifact_id"][7:]
            )
            or not isinstance(universe_input["artifact_hash"], str)
            or len(universe_input["artifact_hash"]) != 64
            or any(
                character not in "0123456789abcdef"
                for character in universe_input["artifact_hash"]
            )
        ):
            raise ValueError("universe artifact identity is invalid")
        if payload.get("universe_artifact_id") != universe_input.get(
            "artifact_id"
        ) or payload.get("universe_artifact_hash") != universe_input.get(
            "artifact_hash"
        ):
            raise ValueError("universe artifact binding mismatch")
    return dict(artifact)


def write_artifact_exclusive(
    path: str | Path,
    artifact: Mapping[str, Any],
    *,
    now_fn=lambda: dt.datetime.now(UTC),
) -> None:
    publication = dict(artifact)
    publication["generated_at"] = _rfc3339(_instant(now_fn(), "publication time"))
    validate_artifact(publication)
    output = Path(path)
    output.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            dir=output.parent,
            prefix=f".{output.name}.",
            suffix=".tmp",
            delete=False,
        ) as handle:
            temporary_path = Path(handle.name)
            json.dump(publication, handle, ensure_ascii=False, sort_keys=True, indent=2)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.link(temporary_path, output)
    finally:
        if temporary_path is not None:
            temporary_path.unlink(missing_ok=True)


def load_artifact(path: str | Path) -> dict:
    with Path(path).open(encoding="utf-8") as handle:
        return validate_artifact(json.load(handle))


def _calendar_dates(calendar: Iterable[Any]) -> list[dt.date]:
    dates = sorted({_date(value, "calendar date") for value in calendar})
    if not dates:
        raise ValueError("ChinaAStock trading calendar is empty")
    return dates


def validate_capture_window(
    *, session: dt.date, calendar: Iterable[Any], now: dt.datetime, phase: str
) -> tuple[dt.datetime, dt.datetime, dt.datetime]:
    dates = _calendar_dates(calendar)
    try:
        index = dates.index(session)
    except ValueError as exc:
        raise ValueError("capture date is not a ChinaAStock trading session") from exc
    if index == 0 or index + 1 >= len(dates):
        raise ValueError("capture date requires previous and next calendar sessions")
    previous_close = dt.datetime.combine(dates[index - 1], dt.time(15), BEIJING)
    session_open = dt.datetime.combine(session, dt.time(9, 30), BEIJING)
    session_close = dt.datetime.combine(session, dt.time(15), BEIJING)
    next_open = dt.datetime.combine(dates[index + 1], dt.time(9, 30), BEIJING)
    current = _instant(now, "now").astimezone(BEIJING)
    if phase == "universe":
        if not previous_close <= current < session_open:
            raise ValueError(
                "universe capture must run after prior close and before open"
            )
    elif phase == "inputs":
        if not session_close <= current < next_open:
            raise ValueError("input capture must run after close and before next open")
    else:
        raise ValueError("phase must be universe or inputs")
    return session_open, session_close, next_open


def _rows(queryset, fields: Sequence[str]) -> list[dict]:
    if hasattr(queryset, "only"):
        queryset = queryset.only(*fields)
    if hasattr(queryset, "as_pymongo"):
        queryset = queryset.as_pymongo()
    rows = []
    for item in queryset:
        if isinstance(item, Mapping):
            raw = item
        else:
            raw = getattr(item, "_data", None) or vars(item)
        rows.append({field: normalize_value(raw.get(field)) for field in fields})
    return rows


def _sort_and_require_unique(
    rows: list[dict], key_fields: Sequence[str], source: str
) -> list[dict]:
    rows.sort(key=lambda row: tuple(str(row.get(field) or "") for field in key_fields))
    seen = set()
    for row in rows:
        key = tuple(json.dumps(row.get(field), sort_keys=True) for field in key_fields)
        if key in seen:
            raise ValueError(f"duplicate {source} business key: {key}")
        seen.add(key)
    return rows


def _require_rows_not_after(
    rows: Sequence[Mapping[str, Any]],
    *,
    date_field: str,
    cutoff: dt.datetime,
    source: str,
) -> None:
    for row in rows:
        value = row.get(date_field)
        if value is not None and _instant(value, f"{source}.{date_field}") > cutoff:
            raise ValueError(f"{source} row is after capture cutoff")


def _source_snapshot(sources: Mapping[str, list], as_of: dt.datetime) -> dict:
    as_of_text = _rfc3339(as_of)
    return {
        name: {
            "content_hash": content_hash(rows),
            "row_count": len(rows),
            "as_of": as_of_text,
        }
        for name, rows in sorted(sources.items())
    }


def _require_build_revision(value: str | None = None) -> str:
    revision = (
        value if value is not None else os.getenv(BUILD_REVISION_ENV, "")
    ).strip()
    if not revision:
        raise ValueError(f"{BUILD_REVISION_ENV} is required")
    return revision


class PitInputEvidenceCapture:
    """Mongo read adapter for two-phase forward PIT evidence capture."""

    def __init__(
        self,
        *,
        stock_model=IndividualStock,
        quote_model=StockDailyQuote,
        factor_model=StockFactorDaily,
        signal_model=StockSignalDaily,
        industry_model=StockIndustryClassification,
        industry_metrics_model=IndustryDailyMetrics,
        model_version_model=ScoreModelVersion,
        market_model=FinanceMarket,
        now_fn=lambda: dt.datetime.now(UTC),
        build_revision: str | None = None,
    ):
        self.stock_model = stock_model
        self.quote_model = quote_model
        self.factor_model = factor_model
        self.signal_model = signal_model
        self.industry_model = industry_model
        self.industry_metrics_model = industry_metrics_model
        self.model_version_model = model_version_model
        self.market_model = market_model
        self.now_fn = now_fn
        self.build_revision = _require_build_revision(build_revision)

    def _calendar(self) -> list:
        market = self.market_model.objects(name="ChinaAStock").first()
        if market is None:
            raise ValueError("ChinaAStock market is missing")
        return list(getattr(market, "trade_calendar", None) or [])

    def capture_universe(self, session: dt.date) -> dict:
        now = _instant(self.now_fn(), "now")
        calendar = self._calendar()
        validate_capture_window(
            session=session, calendar=calendar, now=now, phase="universe"
        )
        members = _rows(self.stock_model.objects(active_status=0), ("code", "name"))
        members = _sort_and_require_unique(members, ("code",), "universe")
        if not members:
            raise ValueError("captured universe is empty")
        codes = [row["code"] for row in members]
        industries = _rows(
            self.industry_model.objects(stock_code__in=codes), INDUSTRY_FIELDS
        )
        industries = _sort_and_require_unique(
            industries, ("stock_code",), "industry classification"
        )
        for row in industries:
            assigned_at = row.get("assigned_at")
            if assigned_at and _instant(assigned_at, "industry.assigned_at") > now:
                raise ValueError("industry classification assigned_at is in the future")
        sources = {
            "calendar": [value.isoformat() for value in _calendar_dates(calendar)],
            "industry_classification": industries,
            "universe": members,
        }
        completed_at = _instant(self.now_fn(), "now")
        validate_capture_window(
            session=session,
            calendar=calendar,
            now=completed_at,
            phase="universe",
        )
        snapshot = {
            "code": {"version": self.build_revision},
            "sources": _source_snapshot(sources, completed_at),
        }
        return build_artifact(
            schema_version=SCHEMA_UNIVERSE,
            producer=PRODUCER_UNIVERSE,
            as_of=completed_at,
            generated_at=completed_at,
            build_revision=self.build_revision,
            input_snapshot=snapshot,
            payload={"session": session.isoformat(), "sources": sources},
            payload_inputs=UNIVERSE_INPUTS,
        )

    def capture_inputs(
        self,
        universe_artifact: Mapping[str, Any],
        *,
        model_version: str,
        horizons: Sequence[int],
    ) -> dict:
        universe = validate_artifact(universe_artifact)
        if universe["schema_version"] != SCHEMA_UNIVERSE:
            raise ValueError("universe artifact schema mismatch")
        session = _date(universe["payload"].get("session"), "payload.session")
        now = _instant(self.now_fn(), "now")
        universe_sources = universe["payload"]["sources"]
        calendar = [
            _date(value, "universe calendar") for value in universe_sources["calendar"]
        ]
        validate_capture_window(
            session=session,
            calendar=calendar,
            now=_instant(universe["as_of"], "universe.as_of"),
            phase="universe",
        )
        _, session_close, _ = validate_capture_window(
            session=session, calendar=calendar, now=now, phase="inputs"
        )
        normalized_horizons = sorted(set(int(value) for value in horizons))
        if not normalized_horizons or any(
            value not in SUPPORTED_HORIZONS for value in normalized_horizons
        ):
            raise ValueError("horizons must contain supported values")
        registered = self.model_version_model.objects(
            model_version=model_version, status="ACTIVE"
        ).first()
        if registered is None or getattr(registered, "scoring_mode", None) != "ranked":
            raise ValueError("model version must be ACTIVE and ranked")
        config = dict(getattr(registered, "config", None) or {})
        expected_config_hash = model_config_hash(config)
        if getattr(registered, "config_hash", None) != expected_config_hash:
            raise ValueError("registered model config_hash mismatch")

        members = universe_sources["universe"]
        codes = [row["code"] for row in members]
        configs = [
            get_effective_horizon_config(value, config) for value in normalized_horizons
        ]
        max_history = max(
            max(
                item["minimum_quote_count"],
                item["breakout_lookback"],
                item["risk_lookback"],
            )
            for item in configs
        )
        max_decay = max(item.get("signal_decay_max_days", 5) for item in configs)
        dates = _calendar_dates(calendar)
        session_index = dates.index(session)
        window_start = dates[max(0, session_index - (max_history + 10))]
        start_dt = dt.datetime.combine(window_start, dt.time(), UTC)
        session_dt = dt.datetime.combine(session, dt.time(), UTC)
        decay_start = session_dt - dt.timedelta(days=max_decay + 1)

        quotes = _rows(
            self.quote_model.objects(
                code__in=codes, date__gte=start_dt, date__lte=session_dt
            ),
            QUOTE_FIELDS,
        )
        quotes = _sort_and_require_unique(quotes, ("code", "date"), "quote")
        history_counts = {code: 0 for code in codes}
        for row in quotes:
            row_date = _instant(row["date"], "quote.date")
            if row_date < session_dt:
                history_counts[row["code"]] += 1
        fallback_quotes = []
        for code in codes:
            if history_counts[code] >= max_history:
                continue
            queryset = (
                self.quote_model.objects(code=code, date__lt=session_dt)
                .order_by("-date")
                .limit(max_history)
            )
            fallback_quotes.extend(_rows(queryset, QUOTE_FIELDS))
        fallback_quotes = _sort_and_require_unique(
            fallback_quotes, ("code", "date"), "fallback quote"
        )
        index_start = start_dt
        fallback_dates = [
            _instant(row["date"], "fallback quote.date")
            for row in fallback_quotes
            if row.get("date")
        ]
        if fallback_dates:
            index_start = min(index_start, min(fallback_dates))
        index_quotes = _rows(
            self.quote_model.objects(
                code="sh000300", date__gte=index_start, date__lte=session_dt
            ),
            QUOTE_FIELDS,
        )
        index_quotes = _sort_and_require_unique(
            index_quotes, ("code", "date"), "index quote"
        )
        factors = _rows(
            self.factor_model.objects(stock_code__in=codes, date=session_dt),
            FACTOR_FIELDS,
        )
        factors = _sort_and_require_unique(factors, ("stock_code", "date"), "factor")
        signals = _rows(
            self.signal_model.objects(
                stock_code__in=codes, date__gte=decay_start, date__lte=session_dt
            ),
            SIGNAL_FIELDS,
        )
        signals = _sort_and_require_unique(
            signals, ("stock_code", "date", "signal_name"), "signal"
        )
        industry_codes = sorted(
            {
                row.get("industry_code_sw_l1")
                for row in universe_sources["industry_classification"]
                if row.get("industry_code_sw_l1")
            }
        )
        metrics = _rows(
            self.industry_metrics_model.objects(
                industry_code__in=industry_codes,
                date__lt=session_dt,
                horizon__in=normalized_horizons,
                model_version=model_version,
            ),
            INDUSTRY_METRIC_FIELDS,
        )
        metrics = _sort_and_require_unique(
            metrics,
            ("industry_code", "date", "horizon", "model_version"),
            "industry metric",
        )
        _require_rows_not_after(
            quotes, date_field="date", cutoff=session_close, source="quote"
        )
        _require_rows_not_after(
            fallback_quotes,
            date_field="date",
            cutoff=session_close,
            source="fallback quote",
        )
        _require_rows_not_after(
            index_quotes,
            date_field="date",
            cutoff=session_close,
            source="index quote",
        )
        _require_rows_not_after(
            factors, date_field="date", cutoff=session_close, source="factor"
        )
        _require_rows_not_after(
            signals, date_field="date", cutoff=session_close, source="signal"
        )
        _require_rows_not_after(
            metrics,
            date_field="date",
            cutoff=session_close,
            source="industry metric",
        )
        sources = {
            "calendar": [value.isoformat() for value in dates],
            "factors": factors,
            "fallback_quotes": fallback_quotes,
            "industry_metrics": metrics,
            "index_quotes": index_quotes,
            "quotes": quotes,
            "signals": signals,
        }
        snapshot = {
            "code": {"version": self.build_revision},
            "model": {
                "model_version": model_version,
                "config_hash": expected_config_hash,
                "scoring_mode": "ranked",
                "horizons": normalized_horizons,
            },
            "universe_artifact": {
                "artifact_id": universe["artifact_id"],
                "artifact_hash": universe["artifact_hash"],
                "as_of": universe["as_of"],
            },
            "sources": _source_snapshot(sources, session_close),
        }
        completed_at = _instant(self.now_fn(), "now")
        validate_capture_window(
            session=session,
            calendar=calendar,
            now=completed_at,
            phase="inputs",
        )
        return build_artifact(
            schema_version=SCHEMA_INPUTS,
            producer=PRODUCER_INPUTS,
            as_of=session_close,
            generated_at=completed_at,
            build_revision=self.build_revision,
            input_snapshot=snapshot,
            payload={
                "session": session.isoformat(),
                "model_config": normalize_value(config),
                "universe_artifact_id": universe["artifact_id"],
                "universe_artifact_hash": universe["artifact_hash"],
                "sources": sources,
            },
            payload_inputs=INPUT_INPUTS,
        )
