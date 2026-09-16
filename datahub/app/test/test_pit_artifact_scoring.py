import copy
import datetime as dt
import hashlib
import json
from types import SimpleNamespace

import pytest

from app.jobs import scoring_runner
from app.lib.scoring_engine.pit_artifact_scoring import (
    PitArtifactScoringConsumer,
    build_replay_handoff,
    publish_predictions,
    serialize_result,
    validate_result,
)
from app.lib.scoring_engine.config import model_config_hash
from app.lib.scoring_engine.pit_input_evidence import PitInputEvidenceCapture
from app.lib.strategy_engine.timing_replay import _prediction_reason
from app.test.test_scoring_batch_equivalence import (
    CALENDAR as PARITY_CALENDAR,
    EVAL_DATE as PARITY_DATE,
    FakeFactor as ParityFactor,
    FakeIndustryClassification as ParityIndustry,
    FakeIndustryMetrics as ParityIndustryMetrics,
    FakePrediction as ParityPrediction,
    FakeQuote as ParityQuote,
    FakeSignal as ParitySignal,
    FakeStock as ParityStock,
    _run as run_production_scoring,
    batch_harness as batch_harness_fixture,
    seed_market,
)
from app.test.test_pit_input_evidence import (
    POST_CLOSE,
    PRE_OPEN,
    SESSION,
    _capture,
    _rehash,
    fake_model,
)


def _artifacts():
    universe = _capture(PRE_OPEN).capture_universe(SESSION)
    inputs = _capture(POST_CLOSE).capture_inputs(
        universe, model_version="ranked-v1", horizons=[20]
    )
    return universe, inputs


def _consumer(universe=None, inputs=None):
    if universe is None or inputs is None:
        universe, inputs = _artifacts()
    return PitArtifactScoringConsumer(
        universe,
        inputs,
        input_artifact_uri="/evidence/inputs.json",
        build_revision="consumer123",
    )


@pytest.fixture
def parity_batch_harness():
    fixture = batch_harness_fixture.__wrapped__(list(PARITY_CALENDAR))
    service = next(fixture)
    try:
        yield service
    finally:
        fixture.close()
        for model in (
            ParityStock,
            ParityQuote,
            ParityFactor,
            ParitySignal,
            ParityPrediction,
            ParityIndustry,
            ParityIndustryMetrics,
        ):
            model.records = []


def test_pair_validation_rejects_calendar_and_member_scope_mismatches():
    universe, inputs = _artifacts()

    wrong_as_of = copy.deepcopy(inputs)
    wrong_as_of["input_snapshot"]["universe_artifact"]["as_of"] = "2026-09-14T23:30:00Z"
    _rehash(wrong_as_of)
    with pytest.raises(ValueError, match="universe artifact as_of mismatch"):
        _consumer(universe, wrong_as_of)

    wrong_calendar = copy.deepcopy(inputs)
    calendar = wrong_calendar["payload"]["sources"]["calendar"]
    calendar.append("2026-09-18")
    wrong_calendar["input_snapshot"]["sources"]["calendar"]["row_count"] += 1
    from app.lib.scoring_engine.pit_input_evidence import content_hash

    wrong_calendar["input_snapshot"]["sources"]["calendar"]["content_hash"] = (
        content_hash(calendar)
    )
    _rehash(wrong_calendar)
    with pytest.raises(ValueError, match="calendar mismatch"):
        _consumer(universe, wrong_calendar)

    outsider = copy.deepcopy(inputs)
    outsider["payload"]["sources"]["factors"][0]["stock_code"] = "sh600999"
    outsider["input_snapshot"]["sources"]["factors"]["content_hash"] = content_hash(
        outsider["payload"]["sources"]["factors"]
    )
    _rehash(outsider)
    with pytest.raises(ValueError, match="outside frozen universe"):
        _consumer(universe, outsider)

    wrong_factor_day = copy.deepcopy(inputs)
    wrong_factor_day["payload"]["sources"]["factors"][0]["date"] = (
        "2026-09-14T00:00:00Z"
    )
    wrong_factor_day["input_snapshot"]["sources"]["factors"]["content_hash"] = (
        content_hash(wrong_factor_day["payload"]["sources"]["factors"])
    )
    _rehash(wrong_factor_day)
    with pytest.raises(ValueError, match="factors must be captured for the session"):
        _consumer(universe, wrong_factor_day)


def test_artifact_scoring_is_deterministic_and_p1_bound():
    consumer = _consumer()
    first = consumer.build_result()
    second = consumer.build_result()

    assert first["predictions"] == second["predictions"]
    assert first["summary"] == {
        "blocked_count": 1,
        "member_count": 2,
        "prediction_count": 2,
        "ranked_count": 1,
    }
    root = first["prediction_roots"]["20"]["root_sha256"]
    artifact_sha256 = hashlib.sha256(serialize_result(first)).hexdigest()
    handoff = build_replay_handoff(
        first,
        artifact_uri="/evidence/predictions.json",
        artifact_sha256=artifact_sha256,
    )["20"]
    assert handoff["artifact_sha256"] == artifact_sha256
    assert handoff["prediction_root_sha256"] == root
    assert handoff["member_codes"] == ["sh600000", "sh600001"]
    assert handoff["member_count"] == 2
    assert handoff["data_as_of"] == "2026-09-15T07:00:00Z"

    by_code = {row["stock_code"]: row for row in first["predictions"]}
    ranked = by_code["sh600000"]
    assert ranked["status"] == "PENDING"
    assert ranked["rank"] == 1
    assert ranked["percentile"] == 1.0
    assert ranked["input_snapshot"]["status"] == "RANKED"
    assert ranked["input_snapshot"]["freshness"] == "FRESH"
    assert (
        ranked["input_snapshot"]["input_artifact_hash"]
        == consumer.input_artifact["artifact_hash"]
    )
    assert ranked["input_snapshot"]["data_as_of"] == "2026-09-15T07:00:00Z"
    assert by_code["sh600001"]["status"] == "BLOCKED"
    assert by_code["sh600001"]["rank"] is None

    manifest = {
        "model_version": "ranked-v1",
        "horizon": 20,
        "cohort_codes": ["sh600000", "sh600001"],
        "prediction_cohorts": {"2026-09-15": handoff},
    }
    assert _prediction_reason(ranked, "2026-09-15", "sh600000", manifest) == (
        1.0,
        None,
    )
    mismatched = copy.deepcopy(manifest)
    mismatched["prediction_cohorts"]["2026-09-15"]["prediction_root_sha256"] = "0" * 64
    assert _prediction_reason(ranked, "2026-09-15", "sh600000", mismatched) == (
        None,
        "prediction_integrity_mismatch",
    )


def test_artifact_adapter_matches_production_ranked_semantics(parity_batch_harness):
    """Exercise the real production component path on a broad synthetic cohort."""
    seed_market()
    duplicate_signals = [
        row
        for row in ParitySignal.records
        if row.stock_code == "sh600000" and row.date == PARITY_DATE
    ]
    duplicate_signals[1].signal_name = "MA20_CROSS_MA60"
    for model in (
        ParityQuote,
        ParityFactor,
        ParitySignal,
        ParityIndustryMetrics,
    ):
        for row in model.records:
            for field, value in vars(row).items():
                if isinstance(value, float):
                    setattr(row, field, round(value, 8))
    config = {"20": {"directions": {"momentum": -1, "trend_alignment": -1}}}
    parity_batch_harness.scoring_config = config

    market_model = fake_model(
        SimpleNamespace(name="ChinaAStock", trade_calendar=PARITY_CALENDAR)
    )
    registry_model = fake_model(
        SimpleNamespace(
            model_version=parity_batch_harness.model_version,
            status="ACTIVE",
            scoring_mode="ranked",
            config=config,
            config_hash=model_config_hash(config),
        )
    )
    capture_kwargs = {
        "stock_model": ParityStock,
        "quote_model": ParityQuote,
        "factor_model": ParityFactor,
        "signal_model": ParitySignal,
        "industry_model": ParityIndustry,
        "industry_metrics_model": ParityIndustryMetrics,
        "model_version_model": registry_model,
        "market_model": market_model,
        "build_revision": "parity-source",
    }
    universe = PitInputEvidenceCapture(
        **capture_kwargs,
        now_fn=lambda: PARITY_DATE + dt.timedelta(minutes=30),
    ).capture_universe(PARITY_DATE.date())
    inputs = PitInputEvidenceCapture(
        **capture_kwargs,
        now_fn=lambda: PARITY_DATE + dt.timedelta(hours=8),
    ).capture_inputs(
        universe,
        model_version=parity_batch_harness.model_version,
        horizons=[5, 20, 60],
    )

    artifact_rows = {
        (row["stock_code"], row["horizon"]): row
        for row in _consumer(universe, inputs).build_result()["predictions"]
    }
    production_rows = run_production_scoring(
        parity_batch_harness, batch=True, ranked=True, horizon=None
    )

    assert set(artifact_rows) == set(production_rows)
    for key, artifact_row in artifact_rows.items():
        production_row = production_rows[key]
        for field in (
            "score",
            "rank",
            "percentile",
            "recommendation",
            "base_price",
            "status",
            "explanation",
        ):
            assert artifact_row.get(field) == production_row[field], (key, field)
        if artifact_row.get("target_date") is not None:
            assert artifact_row["target_date"] == production_row["target_date"][:10]

    assert {horizon for _, horizon in artifact_rows} == {5, 20, 60}
    assert any(row["status"] == "BLOCKED" for row in artifact_rows.values())
    assert any(row["explanation"]["penalties"] for row in artifact_rows.values())
    component_values = {}
    for (_, horizon), row in artifact_rows.items():
        for component in row["explanation"]["components"]:
            component_values.setdefault((horizon, component["id"]), []).append(
                component["raw_value"]
            )
    assert any(
        len(values) != len({json.dumps(value, sort_keys=True) for value in values})
        for values in component_values.values()
    ), "fixture must exercise tied raw component ranks"
    assert any(
        row["score"] < 0
        for (code, horizon), row in artifact_rows.items()
        if horizon == 20 and row["status"] != "BLOCKED"
    )


def test_dry_run_cli_never_initializes_mongo(tmp_path, monkeypatch):
    universe, inputs = _artifacts()
    universe_path = tmp_path / "universe.json"
    inputs_path = tmp_path / "inputs.json"
    output_path = tmp_path / "result.json"
    universe_path.write_text(json.dumps(universe), encoding="utf-8")
    inputs_path.write_text(json.dumps(inputs), encoding="utf-8")

    def fail_db():
        raise AssertionError("dry-run initialized MongoDB")

    monkeypatch.setattr(scoring_runner, "_init_db_connection", fail_db)
    monkeypatch.setenv("CAIFUBAO_BUILD_REVISION", "consumer123")
    scoring_runner.run_score_pit_artifacts(
        SimpleNamespace(
            universe_artifact=str(universe_path),
            input_artifact=str(inputs_path),
            output=str(output_path),
            apply=False,
        )
    )

    result = json.loads(output_path.read_text(encoding="utf-8"))
    assert result["summary"]["prediction_count"] == 2
    with pytest.raises(FileExistsError):
        scoring_runner.run_score_pit_artifacts(
            SimpleNamespace(
                universe_artifact=str(universe_path),
                input_artifact=str(inputs_path),
                output=str(output_path),
                apply=False,
            )
        )


class _Query:
    def __init__(self, rows):
        self.rows = rows

    def first(self):
        return self.rows[0] if self.rows else None

    def only(self, *_fields):
        return self

    def __iter__(self):
        return iter(self.rows)


class _Registry:
    records = []

    @classmethod
    def objects(cls, **query):
        return _Query(
            [
                row
                for row in cls.records
                if all(getattr(row, key) == value for key, value in query.items())
            ]
        )


class _Predictions:
    existing = []
    writes = []

    @classmethod
    def objects(cls, **_query):
        return _Query(cls.existing)

    @classmethod
    def _get_collection(cls):
        return cls

    @classmethod
    def bulk_write(cls, operations, ordered):
        cls.writes.append((list(operations), ordered))
        return SimpleNamespace(inserted_count=len(operations))


def test_publish_preflight_rejects_drift_and_collision_before_write():
    result = _consumer().build_result()
    config = {"20": {"buy_threshold": 70.5}}
    _Registry.records = [
        SimpleNamespace(
            model_version="ranked-v1",
            status="ACTIVE",
            scoring_mode="ranked",
            config=config,
            config_hash=model_config_hash(config),
        )
    ]
    _Predictions.existing = [SimpleNamespace(stock_code="sh600000")]
    _Predictions.writes = []

    with pytest.raises(ValueError, match="prediction collision"):
        publish_predictions(
            result,
            model_version_model=_Registry,
            prediction_model=_Predictions,
        )
    assert _Predictions.writes == []

    tampered = copy.deepcopy(result)
    tampered["predictions"][0]["input_snapshot"]["input_artifact_hash"] = "0" * 64
    with pytest.raises(ValueError, match="prediction input artifact mismatch"):
        validate_result(tampered)

    changed_score = copy.deepcopy(result)
    changed_score["predictions"][0]["score"] = 99.99
    with pytest.raises(ValueError, match="commitment hash mismatch"):
        validate_result(changed_score)


def test_publish_inserts_complete_validated_cohort_once():
    result = _consumer().build_result()
    config = {"20": {"buy_threshold": 70.5}}
    _Registry.records = [
        SimpleNamespace(
            model_version="ranked-v1",
            status="ACTIVE",
            scoring_mode="ranked",
            config=config,
            config_hash=model_config_hash(config),
        )
    ]
    _Predictions.existing = []
    _Predictions.writes = []

    assert (
        publish_predictions(
            result,
            model_version_model=_Registry,
            prediction_model=_Predictions,
        )
        == 2
    )
    assert len(_Predictions.writes) == 1
    operations, ordered = _Predictions.writes[0]
    assert len(operations) == 2
    assert ordered is True

    _Predictions.existing = []
    _Predictions.writes = []
    _Registry.records[0].config_hash = "0" * 64
    with pytest.raises(ValueError, match="registry config_hash mismatch"):
        publish_predictions(
            result,
            model_version_model=_Registry,
            prediction_model=_Predictions,
        )
    assert _Predictions.writes == []
