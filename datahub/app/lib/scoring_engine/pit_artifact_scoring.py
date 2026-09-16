"""Ranked scoring from immutable P2a point-in-time artifacts."""

from __future__ import annotations

import datetime as dt
import hashlib
import json
import os
from pathlib import Path
from typing import Any, Mapping

from pymongo import InsertOne

from app.lib.scoring_engine.config import (
    SUPPORTED_HORIZONS,
    get_effective_horizon_config,
    model_config_hash,
)
from app.lib.scoring_engine.pit_input_evidence import (
    BUILD_REVISION_ENV,
    SCHEMA_INPUTS,
    SCHEMA_UNIVERSE,
    _date,
    _instant,
    _rfc3339,
    denormalize_value,
    validate_artifact,
)
from app.lib.scoring_engine.prediction_integrity import (
    COMMITMENT_SCHEMA,
    attach_prediction_commitments,
    verify_prediction_commitment,
)
from app.lib.scoring_engine.scoring_service import (
    StockScoringService,
    _Row,
    normalize_date,
)
from app.model.scoring import ScoreModelVersion, StockScorePrediction


RESULT_SCHEMA = "stock-ranked-predictions-p2b-v1"
DISCLAIMER = "Research-only evidence; not investment or trading advice."


def _row(value: Mapping[str, Any]) -> _Row:
    decoded = denormalize_value(dict(value))
    for field in ("date", "assigned_at"):
        raw = decoded.get(field)
        if raw:
            decoded[field] = _instant(raw, field)
    return _Row(decoded)


def _json_value(value: Any) -> Any:
    if isinstance(value, dt.datetime):
        return _rfc3339(value)
    if isinstance(value, dt.date):
        return value.isoformat()
    if isinstance(value, Mapping):
        return {key: _json_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_value(item) for item in value]
    return value


def _day_datetime(value: Any, field: str) -> dt.datetime:
    return dt.datetime.combine(_date(value, field), dt.time(), dt.UTC)


class _ArtifactPrefetch:
    def __init__(self, *, session, codes, horizons, universe_sources, input_sources):
        self.date = session
        self.codes = list(codes)
        self.horizons = list(horizons)
        quotes = [_row(value) for value in input_sources["quotes"]]
        fallback = [_row(value) for value in input_sources["fallback_quotes"]]
        self._day_quotes = {
            value.code: value for value in quotes if value.date == self.date
        }
        self._history = {}
        for value in quotes:
            if value.date < self.date:
                self._history.setdefault(value.code, []).append(value)
        self._fallback = {}
        for value in fallback:
            self._fallback.setdefault(value.code, []).append(value)
        for rows in (*self._history.values(), *self._fallback.values()):
            rows.sort(key=lambda value: value.date, reverse=True)
        self._factors = {
            value.stock_code: value
            for value in map(_row, input_sources["factors"])
            if value.date == self.date
        }
        self._signals = {}
        for value in map(_row, input_sources["signals"]):
            self._signals.setdefault(value.stock_code, []).append(value)
        self._index_quotes = sorted(
            map(_row, input_sources["index_quotes"]), key=lambda value: value.date
        )
        classifications = {
            value.stock_code: value
            for value in map(_row, universe_sources["industry_classification"])
        }
        latest_metrics = {}
        for value in map(_row, input_sources["industry_metrics"]):
            key = (value.industry_code, value.horizon)
            current = latest_metrics.get(key)
            if current is None or value.date > current.date:
                latest_metrics[key] = value
        self._industry = {
            horizon: {
                code: (
                    classification,
                    latest_metrics.get((classification.industry_code_sw_l1, horizon)),
                )
                for code, classification in classifications.items()
            }
            for horizon in self.horizons
        }

    def quote(self, code):
        return self._day_quotes.get(code)

    def factor(self, code):
        return self._factors.get(code)

    def day_signals(self, code):
        return sorted(
            (row for row in self._signals.get(code, []) if row.date == self.date),
            key=lambda row: row.signal_name or "",
        )

    def history(self, code, limit):
        rows = self._history.get(code, [])
        if len(rows) >= int(limit):
            return rows[: int(limit)]
        return self._fallback.get(code, [])[: int(limit)]

    def decay_signals(self, code):
        return sorted(
            (row for row in self._signals.get(code, []) if row.date < self.date),
            key=lambda row: row.date,
            reverse=True,
        )

    def index_quotes_for(self, _code, history_quotes, quote):
        dates = [row.date for row in history_quotes if getattr(row, "date", None)]
        if getattr(quote, "date", None):
            dates.append(quote.date)
        if not dates:
            return []
        start, end = min(dates), max(dates)
        return [row for row in self._index_quotes if start <= row.date <= end]

    def industry_lookup(self, horizon):
        return self._industry.get(horizon, {})


class PitArtifactScoringConsumer:
    """Validate a P2a pair and compute predictions without database access."""

    def __init__(
        self,
        universe_artifact: Mapping[str, Any],
        input_artifact: Mapping[str, Any],
        *,
        input_artifact_uri: str,
        build_revision: str | None = None,
    ):
        self.universe_artifact = validate_artifact(universe_artifact)
        self.input_artifact = validate_artifact(input_artifact)
        if self.universe_artifact["schema_version"] != SCHEMA_UNIVERSE:
            raise ValueError("universe artifact schema mismatch")
        if self.input_artifact["schema_version"] != SCHEMA_INPUTS:
            raise ValueError("input artifact schema mismatch")
        self.input_artifact_uri = str(input_artifact_uri or "").strip()
        if not self.input_artifact_uri:
            raise ValueError("input artifact URI is required")
        self.build_revision = str(
            build_revision or os.getenv(BUILD_REVISION_ENV, "")
        ).strip()
        if not self.build_revision:
            raise ValueError(f"consumer build revision requires {BUILD_REVISION_ENV}")
        self._validate_pair()

    def _validate_pair(self):
        universe = self.universe_artifact
        inputs = self.input_artifact
        universe_payload = universe["payload"]
        input_payload = inputs["payload"]
        universe_snapshot = universe["input_snapshot"]
        input_snapshot = inputs["input_snapshot"]
        if universe_payload["session"] != input_payload["session"]:
            raise ValueError("artifact session mismatch")
        pin = input_snapshot["universe_artifact"]
        if (
            pin["artifact_id"] != universe["artifact_id"]
            or pin["artifact_hash"] != universe["artifact_hash"]
        ):
            raise ValueError("universe artifact identity mismatch")
        if pin["as_of"] != universe["as_of"]:
            raise ValueError("universe artifact as_of mismatch")
        universe_calendar = universe_payload["sources"]["calendar"]
        input_calendar = input_payload["sources"]["calendar"]
        if (
            universe_calendar != input_calendar
            or universe_snapshot["sources"]["calendar"]["content_hash"]
            != input_snapshot["sources"]["calendar"]["content_hash"]
        ):
            raise ValueError("artifact calendar mismatch")

        members = universe_payload["sources"]["universe"]
        self.codes = [row["code"] for row in members]
        code_set = set(self.codes)
        for source_name, field in (("industry_classification", "stock_code"),):
            for row in universe_payload["sources"][source_name]:
                if row[field] not in code_set:
                    raise ValueError(f"{source_name} row outside frozen universe")
        for source_name, field in (
            ("quotes", "code"),
            ("fallback_quotes", "code"),
            ("factors", "stock_code"),
            ("signals", "stock_code"),
        ):
            for row in input_payload["sources"][source_name]:
                if row[field] not in code_set:
                    raise ValueError(f"{source_name} row outside frozen universe")
        if any(
            row["code"] != "sh000300"
            for row in input_payload["sources"]["index_quotes"]
        ):
            raise ValueError("index_quotes must contain only sh000300")
        industry_codes = {
            row.get("industry_code_sw_l1")
            for row in universe_payload["sources"]["industry_classification"]
            if row.get("industry_code_sw_l1")
        }
        if any(
            row["industry_code"] not in industry_codes
            for row in input_payload["sources"]["industry_metrics"]
        ):
            raise ValueError("industry_metrics row outside frozen universe")

        self.session_date = _date(input_payload["session"], "payload.session")
        self.session = dt.datetime.combine(self.session_date, dt.time(), dt.UTC)
        self.data_as_of = _rfc3339(_instant(inputs["as_of"], "input.as_of"))
        model = input_snapshot["model"]
        self.model_version = model["model_version"]
        self.config_hash = model["config_hash"]
        self.horizons = list(model["horizons"])
        self.scoring_config = denormalize_value(input_payload["model_config"])
        self.cohort_fingerprint = StockScoringService._cohort_fingerprint(self.codes)
        self.input_build_revision = input_snapshot["code"]["version"]
        self._validate_source_semantics(input_payload["sources"])

    def _validate_source_semantics(self, sources):
        configs = [
            get_effective_horizon_config(horizon, self.scoring_config)
            for horizon in self.horizons
        ]
        max_history = max(
            max(
                config["minimum_quote_count"],
                config["breakout_lookback"],
                config["risk_lookback"],
            )
            for config in configs
        )
        max_decay = max(config.get("signal_decay_max_days", 5) for config in configs)
        calendar = [_date(value, "calendar") for value in sources["calendar"]]
        session_index = calendar.index(self.session_date)
        window_start = dt.datetime.combine(
            calendar[max(0, session_index - (max_history + 10))], dt.time(), dt.UTC
        )
        quote_dates = {}
        for row in sources["quotes"]:
            row_date = _instant(row["date"], "quotes.date")
            if not window_start <= row_date <= self.session:
                raise ValueError("quotes row outside captured scoring window")
            if row_date < self.session:
                quote_dates.setdefault(row["code"], []).append(row_date)

        fallback_dates = {}
        for row in sources["fallback_quotes"]:
            row_date = _instant(row["date"], "fallback_quotes.date")
            if row_date >= self.session:
                raise ValueError("fallback_quotes must precede the session")
            fallback_dates.setdefault(row["code"], []).append(row_date)
        for code, dates in fallback_dates.items():
            if len(quote_dates.get(code, [])) >= max_history:
                raise ValueError("fallback_quotes supplied for complete history")
            if len(dates) > max_history:
                raise ValueError("fallback_quotes exceed configured history limit")

        for row in sources["factors"]:
            if _instant(row["date"], "factors.date") != self.session:
                raise ValueError("factors must be captured for the session")
        decay_start = self.session - dt.timedelta(days=max_decay + 1)
        for row in sources["signals"]:
            row_date = _instant(row["date"], "signals.date")
            if not decay_start <= row_date <= self.session:
                raise ValueError("signals row outside captured decay window")
        for row in sources["industry_metrics"]:
            if (
                row["model_version"] != self.model_version
                or row["horizon"] not in self.horizons
                or _instant(row["date"], "industry_metrics.date") >= self.session
            ):
                raise ValueError("industry_metrics row conflicts with model window")

        index_start = window_start
        if fallback_dates:
            index_start = min(
                index_start, min(min(rows) for rows in fallback_dates.values())
            )
        for row in sources["index_quotes"]:
            row_date = _instant(row["date"], "index_quotes.date")
            if not index_start <= row_date <= self.session:
                raise ValueError("index_quotes row outside captured scoring window")

    def _service(self):
        service = StockScoringService.__new__(StockScoringService)
        service.model_version = self.model_version
        service.scoring_config = self.scoring_config
        service.calendar = [
            dt.datetime.combine(_date(value, "calendar"), dt.time(), dt.UTC)
            for value in self.input_artifact["payload"]["sources"]["calendar"]
        ]
        service._calendar_cache = None
        service.scoring_mode = "ranked"
        service._registry_scoring_mode = "ranked"
        service._runtime_scoring_mode = None
        return service

    def _provenance(self):
        return {
            "freshness": "FRESH",
            "cohort_fingerprint": self.cohort_fingerprint,
            "data_as_of": self.data_as_of,
            "universe_artifact_id": self.universe_artifact["artifact_id"],
            "universe_artifact_hash": self.universe_artifact["artifact_hash"],
            "input_artifact_id": self.input_artifact["artifact_id"],
            "input_artifact_hash": self.input_artifact["artifact_hash"],
            "model_version": self.model_version,
            "config_hash": self.config_hash,
            "input_build_revision": self.input_build_revision,
            "consumer_build_revision": self.build_revision,
        }

    def build_result(self) -> dict:
        service = self._service()
        universe_sources = self.universe_artifact["payload"]["sources"]
        input_sources = self.input_artifact["payload"]["sources"]
        stocks = {
            row["code"]: _Row({"code": row["code"], "name": row["name"]})
            for row in universe_sources["universe"]
        }
        prefetch = _ArtifactPrefetch(
            session=self.session,
            codes=self.codes,
            horizons=self.horizons,
            universe_sources=universe_sources,
            input_sources=input_sources,
        )
        predictions = []
        provenance = self._provenance()
        for horizon in self.horizons:
            raw_by_code = {}
            blocked = []
            for code in self.codes:
                raw = service._compute_raw_components(
                    stocks[code], self.session, horizon, prefetch=prefetch
                )
                if raw is None:
                    blocked.append(code)
                else:
                    raw_by_code[code] = raw
            payloads = service._ranked_payloads_from_raw(
                raw_by_code, self.session, horizon, self.cohort_fingerprint
            )
            for payload in payloads.values():
                payload["input_snapshot"].update(provenance)
            for code in blocked:
                payload = service._build_blocked_prediction(
                    stock=stocks[code],
                    date=self.session,
                    horizon=horizon,
                    target_date=service.get_t_plus_n_day(self.session, horizon),
                    reason="missing_quote",
                    scoring_mode="ranked",
                    cohort_fingerprint=self.cohort_fingerprint,
                )
                payload.pop("stock", None)
                payload["rank"] = None
                payload["percentile"] = None
                payload["input_snapshot"].update(provenance)
                payloads[code] = payload

            rankable = sorted(
                (row for row in payloads.values() if row["status"] != "BLOCKED"),
                key=lambda row: (-float(row["score"] or 0.0), row["stock_code"]),
            )
            total = len(rankable)
            config = service._get_horizon_config(horizon)
            for rank, payload in enumerate(rankable, start=1):
                payload["rank"] = rank
                payload["percentile"] = round(1 - ((rank - 1) / total), 4)
                payload["recommendation"] = service._recommendation(
                    payload["score"], config, payload["percentile"]
                )
            predictions.extend(payloads[code] for code in self.codes)

        json_predictions = _json_value(predictions)
        for row in json_predictions:
            row["date"] = self.session_date.isoformat()
            row["target_date"] = (
                _instant(row["target_date"], "prediction.target_date")
                .date()
                .isoformat()
            )
        prediction_roots = {}
        for horizon in self.horizons:
            cohort = [row for row in json_predictions if row["horizon"] == horizon]
            root = attach_prediction_commitments(cohort)
            prediction_roots[str(horizon)] = {
                "schema_version": COMMITMENT_SCHEMA,
                "root_sha256": root,
                "leaf_count": len(cohort),
            }
        ranked_count = sum(row["status"] != "BLOCKED" for row in json_predictions)
        result = {
            "schema_version": RESULT_SCHEMA,
            "session": self.session_date.isoformat(),
            "grade": "RESEARCH",
            "disclaimer": DISCLAIMER,
            "model": {
                "model_version": self.model_version,
                "config_hash": self.config_hash,
                "scoring_mode": "ranked",
                "horizons": self.horizons,
                "config": _json_value(self.scoring_config),
            },
            "artifacts": {
                "universe": {
                    "artifact_id": self.universe_artifact["artifact_id"],
                    "artifact_hash": self.universe_artifact["artifact_hash"],
                    "as_of": self.universe_artifact["as_of"],
                    "build_revision": self.universe_artifact["input_snapshot"]["code"][
                        "version"
                    ],
                },
                "inputs": {
                    "artifact_id": self.input_artifact["artifact_id"],
                    "artifact_hash": self.input_artifact["artifact_hash"],
                    "artifact_uri": self.input_artifact_uri,
                    "as_of": self.input_artifact["as_of"],
                    "build_revision": self.input_build_revision,
                },
            },
            "consumer_build_revision": self.build_revision,
            "prediction_roots": prediction_roots,
            "summary": {
                "blocked_count": len(json_predictions) - ranked_count,
                "member_count": len(self.codes),
                "prediction_count": len(json_predictions),
                "ranked_count": ranked_count,
            },
            "predictions": json_predictions,
        }
        validate_result(result)
        return result


def validate_result(result: Mapping[str, Any]) -> dict:
    if result.get("schema_version") != RESULT_SCHEMA:
        raise ValueError("result schema mismatch")
    model = result.get("model") or {}
    artifacts = result.get("artifacts") or {}
    roots = result.get("prediction_roots")
    if result.get("grade") != "RESEARCH" or result.get("disclaimer") != DISCLAIMER:
        raise ValueError("result research boundary mismatch")
    try:
        session = _date(result.get("session"), "result.session")
    except (TypeError, ValueError) as exc:
        raise ValueError("result session mismatch") from exc
    if result.get("session") != session.isoformat():
        raise ValueError("result session must use YYYY-MM-DD")
    model_version = model.get("model_version")
    horizons = model.get("horizons")
    config = model.get("config")
    if (
        not isinstance(model_version, str)
        or not model_version
        or model.get("scoring_mode") != "ranked"
        or not isinstance(horizons, list)
        or not horizons
        or horizons != sorted(set(horizons))
        or any(value not in SUPPORTED_HORIZONS for value in horizons)
        or not isinstance(config, dict)
        or model.get("config_hash") != model_config_hash(config)
    ):
        raise ValueError("result model pin mismatch")
    universe_artifact = artifacts.get("universe") or {}
    input_artifact = artifacts.get("inputs") or {}
    for name, artifact in (
        ("universe", universe_artifact),
        ("inputs", input_artifact),
    ):
        artifact_hash = artifact.get("artifact_hash")
        artifact_id = artifact.get("artifact_id")
        if (
            not isinstance(artifact_hash, str)
            or len(artifact_hash) != 64
            or any(value not in "0123456789abcdef" for value in artifact_hash)
            or not isinstance(artifact_id, str)
            or not artifact_id.startswith("sha256:")
            or len(artifact_id) != 71
            or any(value not in "0123456789abcdef" for value in artifact_id[7:])
            or not isinstance(artifact.get("build_revision"), str)
            or not artifact["build_revision"]
        ):
            raise ValueError(f"result {name} artifact identity mismatch")
    if (
        not isinstance(input_artifact.get("artifact_uri"), str)
        or not input_artifact["artifact_uri"].strip()
    ):
        raise ValueError("result input artifact URI mismatch")
    input_as_of = _instant(input_artifact.get("as_of"), "inputs.as_of")
    expected_close = dt.datetime.combine(session, dt.time(7), dt.UTC)
    if (
        input_artifact.get("as_of") != _rfc3339(input_as_of)
        or input_as_of != expected_close
    ):
        raise ValueError("result input artifact as_of mismatch")
    universe_as_of = _instant(universe_artifact.get("as_of"), "universe.as_of")
    if universe_artifact.get("as_of") != _rfc3339(universe_as_of):
        raise ValueError("result universe artifact as_of mismatch")
    if not isinstance(roots, Mapping) or set(roots) != {
        str(horizon) for horizon in horizons
    }:
        raise ValueError("result prediction roots mismatch")
    consumer_revision = result.get("consumer_build_revision")
    if not isinstance(consumer_revision, str) or not consumer_revision:
        raise ValueError("result consumer build revision is required")
    predictions = result.get("predictions")
    if not isinstance(predictions, list) or not predictions:
        raise ValueError("result predictions are required")
    keys = set()
    codes_by_horizon = {horizon: set() for horizon in horizons}
    rankable_by_horizon = {horizon: [] for horizon in horizons}
    for row in predictions:
        key = (row.get("stock_code"), row.get("date"), row.get("horizon"))
        if key in keys:
            raise ValueError("duplicate prediction natural key")
        keys.add(key)
        if (
            not isinstance(row.get("stock_code"), str)
            or not row["stock_code"]
            or row.get("date") != session.isoformat()
            or row.get("horizon") not in horizons
        ):
            raise ValueError("prediction natural key mismatch")
        codes_by_horizon[row["horizon"]].add(row["stock_code"])
        if row.get("model_version") != model_version:
            raise ValueError("prediction model_version mismatch")
        snapshot = row.get("input_snapshot") or {}
        if snapshot.get("data_as_of") != input_artifact.get("as_of"):
            raise ValueError("prediction data_as_of mismatch")
        if snapshot.get("input_artifact_hash") != (artifacts.get("inputs") or {}).get(
            "artifact_hash"
        ):
            raise ValueError("prediction input artifact mismatch")
        if (
            snapshot.get("input_artifact_id") != input_artifact.get("artifact_id")
            or snapshot.get("universe_artifact_id")
            != universe_artifact.get("artifact_id")
            or snapshot.get("universe_artifact_hash")
            != universe_artifact.get("artifact_hash")
            or snapshot.get("model_version") != model_version
            or snapshot.get("config_hash") != model.get("config_hash")
            or snapshot.get("consumer_build_revision") != consumer_revision
            or snapshot.get("input_build_revision")
            != input_artifact.get("build_revision")
            or snapshot.get("freshness") != "FRESH"
            or snapshot.get("scoring_mode") != "ranked"
        ):
            raise ValueError("prediction provenance pin mismatch")
        if row.get("status") == "BLOCKED":
            if row.get("rank") is not None or row.get("percentile") is not None:
                raise ValueError("blocked prediction cannot be ranked")
            if row.get("recommendation") != "NONE":
                raise ValueError("blocked prediction recommendation mismatch")
            if snapshot.get("status") != "BLOCKED":
                raise ValueError("blocked prediction snapshot mismatch")
        elif row.get("status") != "PENDING":
            raise ValueError("new PIT prediction status mismatch")
        elif snapshot.get("status") != "RANKED":
            raise ValueError("usable prediction freshness mismatch")
        else:
            rankable_by_horizon[row["horizon"]].append(row)
    summary = result.get("summary") or {}
    member_count = summary.get("member_count")
    if (
        isinstance(member_count, bool)
        or not isinstance(member_count, int)
        or member_count < 1
    ):
        raise ValueError("prediction result member_count mismatch")
    expected = member_count * len(horizons)
    if len(predictions) != expected:
        raise ValueError("prediction set is incomplete")
    code_sets = list(codes_by_horizon.values())
    if any(len(codes) != member_count for codes in code_sets) or any(
        codes != code_sets[0] for codes in code_sets[1:]
    ):
        raise ValueError("prediction cohort membership mismatch")
    cohort_fingerprint = StockScoringService._cohort_fingerprint(code_sets[0])
    rows_by_key = {(row["horizon"], row["stock_code"]): row for row in predictions}
    for horizon, codes in codes_by_horizon.items():
        root_record = roots[str(horizon)]
        if (
            not isinstance(root_record, Mapping)
            or set(root_record) != {"schema_version", "root_sha256", "leaf_count"}
            or root_record.get("schema_version") != COMMITMENT_SCHEMA
            or root_record.get("leaf_count") != member_count
        ):
            raise ValueError("prediction root record mismatch")
        for index, code in enumerate(sorted(codes)):
            row = rows_by_key[(horizon, code)]
            if row["input_snapshot"].get("cohort_fingerprint") != cohort_fingerprint:
                raise ValueError("prediction cohort fingerprint mismatch")
            verify_prediction_commitment(
                row,
                expected_root=root_record.get("root_sha256"),
                expected_leaf_count=member_count,
                expected_leaf_index=index,
            )
    service = StockScoringService.__new__(StockScoringService)
    service.scoring_config = config
    for horizon, rows in rankable_by_horizon.items():
        ranked = sorted(rows, key=lambda row: (-float(row["score"]), row["stock_code"]))
        total = len(ranked)
        effective = service._get_horizon_config(horizon)
        for rank, row in enumerate(ranked, start=1):
            percentile = round(1 - ((rank - 1) / total), 4)
            if (
                row.get("rank") != rank
                or row.get("percentile") != percentile
                or row.get("recommendation")
                != service._recommendation(row["score"], effective, percentile)
            ):
                raise ValueError("prediction ranking mismatch")
    blocked_count = sum(row["status"] == "BLOCKED" for row in predictions)
    if summary != {
        "blocked_count": blocked_count,
        "member_count": member_count,
        "prediction_count": len(predictions),
        "ranked_count": len(predictions) - blocked_count,
    }:
        raise ValueError("prediction result summary mismatch")
    return dict(result)


def publish_predictions(
    result: Mapping[str, Any],
    *,
    model_version_model=ScoreModelVersion,
    prediction_model=StockScorePrediction,
) -> int:
    validated = validate_result(result)
    model = validated["model"]
    registered = model_version_model.objects(
        model_version=model["model_version"], status="ACTIVE"
    ).first()
    if registered is None or getattr(registered, "scoring_mode", None) != "ranked":
        raise ValueError("model version must still be ACTIVE and ranked")
    config = dict(getattr(registered, "config", None) or {})
    expected_hash = model_config_hash(config)
    if (
        getattr(registered, "config_hash", None) != expected_hash
        or expected_hash != model["config_hash"]
    ):
        raise ValueError("registry config_hash mismatch")

    predictions = validated["predictions"]
    filters = {
        "stock_code__in": sorted({row["stock_code"] for row in predictions}),
        "date": normalize_date(
            _day_datetime(predictions[0]["date"], "prediction.date")
        ),
        "horizon__in": model["horizons"],
        "model_version": model["model_version"],
    }
    if list(prediction_model.objects(**filters).only("stock_code", "horizon")):
        raise ValueError("prediction collision; P2b publication is insert-only")

    now = dt.datetime.now(dt.UTC)
    operations = []
    for row in predictions:
        document = dict(row)
        document["date"] = normalize_date(
            _day_datetime(document["date"], "prediction.date")
        )
        document["target_date"] = normalize_date(
            _day_datetime(document["target_date"], "prediction.target_date")
        )
        document["generated_at"] = now
        document["updated_at"] = now
        operations.append(InsertOne(document))
    result_write = prediction_model._get_collection().bulk_write(
        operations, ordered=True
    )
    return int(getattr(result_write, "inserted_count", len(operations)))


def serialize_result(result: Mapping[str, Any]) -> bytes:
    validated = validate_result(result)
    return (
        json.dumps(
            validated,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
        + "\n"
    ).encode("utf-8")


def write_result_exclusive(path: str | Path, result: Mapping[str, Any]) -> str:
    payload = serialize_result(result)
    with Path(path).open("xb") as handle:
        handle.write(payload)
    return hashlib.sha256(payload).hexdigest()


def build_replay_handoff(
    result: Mapping[str, Any], *, artifact_uri: str, artifact_sha256: str
) -> dict[str, dict]:
    validated = validate_result(result)
    uri = str(artifact_uri or "").strip()
    if not uri:
        raise ValueError("result artifact URI is required")
    if (
        not isinstance(artifact_sha256, str)
        or len(artifact_sha256) != 64
        or any(value not in "0123456789abcdef" for value in artifact_sha256)
    ):
        raise ValueError("result artifact_sha256 must be lowercase SHA-256")
    summary = validated["summary"]
    input_as_of = validated["artifacts"]["inputs"]["as_of"]
    fingerprint = validated["predictions"][0]["input_snapshot"]["cohort_fingerprint"]
    return {
        str(horizon): {
            "artifact_uri": uri,
            "artifact_sha256": artifact_sha256,
            "prediction_root_sha256": validated["prediction_roots"][str(horizon)][
                "root_sha256"
            ],
            "cohort_fingerprint": fingerprint,
            "member_codes": sorted(
                row["stock_code"]
                for row in validated["predictions"]
                if row["horizon"] == horizon
            ),
            "member_count": summary["member_count"],
            "data_as_of": input_as_of,
        }
        for horizon in validated["model"]["horizons"]
    }
