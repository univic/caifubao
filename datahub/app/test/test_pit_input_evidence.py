import copy
import datetime as dt
import hashlib
from types import SimpleNamespace

import pytest

from app.lib.scoring_engine.config import model_config_hash
from app.lib.scoring_engine.pit_input_evidence import (
    PitInputEvidenceCapture,
    build_artifact,
    content_hash,
    HASH_FIELDS,
    normalize_value,
    validate_artifact,
    validate_capture_window,
    write_artifact_exclusive,
)


class FakeQuery:
    def __init__(self, rows):
        self.rows = list(rows)

    def first(self):
        return self.rows[0] if self.rows else None

    def only(self, *fields):
        return FakeQuery(
            [{field: _raw(row).get(field) for field in fields} for row in self.rows]
        )

    def order_by(self, field):
        reverse = field.startswith("-")
        key = field.lstrip("+-")
        return FakeQuery(
            sorted(self.rows, key=lambda row: _raw(row).get(key), reverse=reverse)
        )

    def limit(self, count):
        return FakeQuery(self.rows[:count])

    def as_pymongo(self):
        return self

    def __iter__(self):
        return iter(self.rows)


def _raw(row):
    return row if isinstance(row, dict) else vars(row)


def _rehash(artifact):
    artifact["artifact_hash"] = content_hash(
        {field: artifact[field] for field in HASH_FIELDS}
    )
    artifact["artifact_id"] = (
        "sha256:"
        + hashlib.sha256(
            (
                f"{artifact['producer']}\n{artifact['as_of']}\n"
                f"{artifact['artifact_hash']}"
            ).encode()
        ).hexdigest()
    )


def _matches(row, query):
    values = _raw(row)
    for key, expected in query.items():
        if key.endswith("__in"):
            if values.get(key[:-4]) not in expected:
                return False
        elif key.endswith("__gte"):
            if values.get(key[:-5]) < expected:
                return False
        elif key.endswith("__lte"):
            if values.get(key[:-5]) > expected:
                return False
        elif key.endswith("__lt"):
            if values.get(key[:-4]) >= expected:
                return False
        elif values.get(key) != expected:
            return False
    return True


def fake_model(*rows):
    class Model:
        records = list(rows)

        @classmethod
        def objects(cls, **query):
            return FakeQuery(row for row in cls.records if _matches(row, query))

    return Model


CALENDAR = [
    dt.datetime(2026, 9, 14, tzinfo=dt.UTC),
    dt.datetime(2026, 9, 15, tzinfo=dt.UTC),
    dt.datetime(2026, 9, 16, tzinfo=dt.UTC),
    dt.datetime(2026, 9, 17, tzinfo=dt.UTC),
]
SESSION = dt.date(2026, 9, 15)
PRE_OPEN = dt.datetime(2026, 9, 15, 0, 30, tzinfo=dt.UTC)
POST_CLOSE = dt.datetime(2026, 9, 15, 8, 0, tzinfo=dt.UTC)


def test_artifact_identity_excludes_generated_at_and_rejects_tampering(tmp_path):
    source = [{"code": "sh600000", "value": {"scaled_1e8": 125000000}}]
    snapshot = {
        "code": {"version": "abc123"},
        "sources": {
            "rows": {
                "content_hash": content_hash(source),
                "row_count": 1,
                "as_of": "2026-09-15T07:00:00Z",
            }
        },
    }
    common = {
        "schema_version": "test-v1",
        "producer": "test.producer",
        "as_of": dt.datetime(2026, 9, 15, 7, tzinfo=dt.UTC),
        "build_revision": "abc123",
        "input_snapshot": snapshot,
        "payload": {"sources": {"rows": source}},
        "payload_inputs": ("code", "rows"),
    }
    first = build_artifact(
        generated_at=dt.datetime(2026, 9, 15, 8, tzinfo=dt.UTC), **common
    )
    second = build_artifact(
        generated_at=dt.datetime(2026, 9, 15, 8, 1, tzinfo=dt.UTC), **common
    )

    assert first["artifact_hash"] == second["artifact_hash"]
    assert first["artifact_id"] == second["artifact_id"]
    assert first["generated_at"] != second["generated_at"]
    assert "1e-8 source unit" in first["field_manifest"]["numeric_units"].values()

    def publication_time():
        return dt.datetime(2026, 9, 15, 8, tzinfo=dt.UTC)

    write_artifact_exclusive(tmp_path / "artifact.json", first, now_fn=publication_time)
    with pytest.raises(FileExistsError):
        write_artifact_exclusive(
            tmp_path / "artifact.json", first, now_fn=publication_time
        )

    tampered = copy.deepcopy(first)
    tampered["payload"]["sources"]["rows"][0]["code"] = "sh600001"
    with pytest.raises(ValueError, match="artifact_hash mismatch"):
        validate_artifact(tampered)

    tampered["artifact_hash"] = content_hash(
        {field: tampered[field] for field in HASH_FIELDS}
    )
    tampered["artifact_id"] = (
        "sha256:"
        + hashlib.sha256(
            (
                f"{tampered['producer']}\n{tampered['as_of']}\n"
                f"{tampered['artifact_hash']}"
            ).encode()
        ).hexdigest()
    )
    with pytest.raises(ValueError, match="source hash mismatch"):
        validate_artifact(tampered)


def test_numeric_normalization_rejects_unrepresented_precision():
    with pytest.raises(ValueError, match="exceeds declared 1e-8 precision"):
        normalize_value(0.123456789)


@pytest.mark.parametrize(
    ("phase", "now", "message"),
    [
        ("universe", POST_CLOSE, "before open"),
        ("inputs", PRE_OPEN, "after close"),
    ],
)
def test_capture_windows_fail_closed(phase, now, message):
    with pytest.raises(ValueError, match=message):
        validate_capture_window(
            session=SESSION, calendar=CALENDAR, now=now, phase=phase
        )


def _capture(now, *, now_fn=None):
    market_model = fake_model(
        SimpleNamespace(name="ChinaAStock", trade_calendar=CALENDAR)
    )
    stock_model = fake_model(
        SimpleNamespace(code="sh600001", name="停牌样本", active_status=0),
        SimpleNamespace(code="sh600000", name="浦发银行", active_status=0),
        SimpleNamespace(code="sh600999", name="非活跃", active_status=2),
    )
    industry_model = fake_model(
        SimpleNamespace(
            stock_code="sh600000",
            industry_code_sw_l1="J66",
            industry_name_sw_l1="金融",
            assigned_at=dt.datetime(2026, 9, 14, tzinfo=dt.UTC),
        )
    )
    quote_model = fake_model(
        # sh600001 intentionally has no D quote; membership must survive.
        SimpleNamespace(
            code="sh600000",
            date=dt.datetime(2026, 9, 15, tzinfo=dt.UTC),
            open=10.125,
            close=10.25,
            previous_close=10.0,
            high=10.5,
            low=9.9,
            trade_status=1,
            isST=0,
            close_hfq=10.25,
            open_hfq=10.125,
            high_hfq=10.5,
            low_hfq=9.9,
        ),
        SimpleNamespace(
            code="sh000300",
            date=dt.datetime(2026, 9, 15, tzinfo=dt.UTC),
            open=4000.0,
            close=4010.0,
            previous_close=3990.0,
            high=4020.0,
            low=3980.0,
            trade_status=1,
            isST=0,
            close_hfq=4010.0,
            open_hfq=4000.0,
            high_hfq=4020.0,
            low_hfq=3980.0,
        ),
    )
    factor_model = fake_model(
        SimpleNamespace(
            stock_code="sh600000",
            date=dt.datetime(2026, 9, 15, tzinfo=dt.UTC),
            ma_10=10.0,
            ma_20=9.9,
            ma_30=9.8,
            ma_60=9.5,
            ma_120=9.0,
        )
    )
    signal_model = fake_model(
        SimpleNamespace(
            stock_code="sh600000",
            date=dt.datetime(2026, 9, 15, tzinfo=dt.UTC),
            signal_name="ma_cross",
            direction="BULLISH",
            strength=0.75,
        )
    )
    metric_model = fake_model(
        SimpleNamespace(
            industry_code="J66",
            industry_name="金融",
            date=dt.datetime(2026, 9, 14, tzinfo=dt.UTC),
            horizon=20,
            model_version="ranked-v1",
            stock_count=10,
            avg_score=50.0,
            max_score=80.0,
            min_score=20.0,
            std_dev_score=10.0,
            avg_percentile=0.5,
            avg_rank=5.5,
            buy_count=1,
            watch_count=2,
            avoid_count=1,
        )
    )
    config = {"20": {"buy_threshold": 70.5}}
    registry_model = fake_model(
        SimpleNamespace(
            model_version="ranked-v1",
            status="ACTIVE",
            scoring_mode="ranked",
            config=config,
            config_hash=model_config_hash(config),
        )
    )
    return PitInputEvidenceCapture(
        stock_model=stock_model,
        quote_model=quote_model,
        factor_model=factor_model,
        signal_model=signal_model,
        industry_model=industry_model,
        industry_metrics_model=metric_model,
        model_version_model=registry_model,
        market_model=market_model,
        now_fn=now_fn or (lambda: now),
        build_revision="abc123",
    )


def test_two_phase_capture_freezes_universe_and_complete_ranked_inputs():
    universe = _capture(PRE_OPEN).capture_universe(SESSION)

    assert [row["code"] for row in universe["payload"]["sources"]["universe"]] == [
        "sh600000",
        "sh600001",
    ]
    assert universe["as_of"] == "2026-09-15T00:30:00Z"

    inputs = _capture(POST_CLOSE).capture_inputs(
        universe, model_version="ranked-v1", horizons=[20]
    )

    assert inputs["as_of"] == "2026-09-15T07:00:00Z"
    assert (
        inputs["input_snapshot"]["universe_artifact"]["artifact_id"]
        == universe["artifact_id"]
    )
    assert inputs["payload"]["universe_artifact_hash"] == universe["artifact_hash"]
    assert {row["code"] for row in inputs["payload"]["sources"]["quotes"]} == {
        "sh600000"
    }
    assert inputs["payload"]["sources"]["quotes"][0]["close"] == {
        "scaled_1e8": 1_025_000_000
    }
    assert inputs["field_manifest"]["fields"]["payload/sources/quotes/*/close"][
        "inputs"
    ] == ["quotes"]
    assert (
        inputs["field_manifest"]["numeric_units"][
            "payload/sources/quotes/*/close/scaled_1e8"
        ]
        == "1e-8 source unit"
    )
    validate_artifact(inputs)

    tampered = copy.deepcopy(inputs)
    tampered["payload"]["sources"]["quotes"][0]["unexpected"] = "field"
    tampered["input_snapshot"]["sources"]["quotes"]["content_hash"] = content_hash(
        tampered["payload"]["sources"]["quotes"]
    )
    _rehash(tampered)
    with pytest.raises(ValueError, match="source row schema mismatch"):
        validate_artifact(tampered)

    misclassified = copy.deepcopy(inputs)
    misclassified["field_manifest"]["fields"]["payload"]["kind"] = "lifecycle"
    _rehash(misclassified)
    with pytest.raises(ValueError, match="payload classification mismatch"):
        validate_artifact(misclassified)

    future_row = copy.deepcopy(inputs)
    future_row["payload"]["sources"]["quotes"][0]["date"] = "2026-09-16T00:00:00Z"
    future_row["input_snapshot"]["sources"]["quotes"]["content_hash"] = content_hash(
        future_row["payload"]["sources"]["quotes"]
    )
    _rehash(future_row)
    with pytest.raises(ValueError, match="after capture cutoff"):
        validate_artifact(future_row)

    bad_dependencies = copy.deepcopy(inputs)
    bad_dependencies["field_manifest"]["fields"]["payload/sources"]["inputs"] = [
        "bogus"
    ]
    _rehash(bad_dependencies)
    with pytest.raises(ValueError, match="input dependencies mismatch"):
        validate_artifact(bad_dependencies)

    partial_model = copy.deepcopy(inputs)
    del partial_model["input_snapshot"]["model"]["scoring_mode"]
    _rehash(partial_model)
    with pytest.raises(ValueError, match="model pins are incomplete"):
        validate_artifact(partial_model)

    noncanonical_universe_pin = copy.deepcopy(inputs)
    noncanonical_universe_pin["input_snapshot"]["universe_artifact"]["as_of"] = (
        "2026-09-15T08:30:00+08:00"
    )
    _rehash(noncanonical_universe_pin)
    with pytest.raises(ValueError, match="canonical UTC Z"):
        validate_artifact(noncanonical_universe_pin)


def test_universe_capture_rejects_duplicate_member_and_future_classification():
    duplicate = _capture(PRE_OPEN)
    duplicate.stock_model.records.append(
        SimpleNamespace(code="sh600000", name="重复代码", active_status=0)
    )
    with pytest.raises(ValueError, match="duplicate universe business key"):
        duplicate.capture_universe(SESSION)

    future = _capture(PRE_OPEN)
    future.industry_model.records[0].assigned_at = dt.datetime(
        2026, 9, 15, 1, tzinfo=dt.UTC
    )
    with pytest.raises(ValueError, match="assigned_at is in the future"):
        future.capture_universe(SESSION)


def test_input_capture_rejects_tampered_universe_before_market_reads():
    universe = _capture(PRE_OPEN).capture_universe(SESSION)
    universe["payload"]["sources"]["universe"][0]["code"] = "sh600002"

    with pytest.raises(ValueError, match="artifact_hash mismatch"):
        _capture(POST_CLOSE).capture_inputs(
            universe, model_version="ranked-v1", horizons=[20]
        )


@pytest.mark.parametrize(("field", "value"), [("code", None), ("name", "")])
def test_universe_validator_rejects_empty_members_and_identifiers(field, value):
    universe = _capture(PRE_OPEN).capture_universe(SESSION)
    rows = universe["payload"]["sources"]["universe"]
    rows[0][field] = value
    universe["input_snapshot"]["sources"]["universe"]["content_hash"] = content_hash(
        rows
    )
    _rehash(universe)
    with pytest.raises(ValueError, match="source (business key|text field)"):
        validate_artifact(universe)

    rows.clear()
    universe["input_snapshot"]["sources"]["universe"].update(
        content_hash=content_hash(rows), row_count=0
    )
    _rehash(universe)
    with pytest.raises(ValueError, match="captured universe is empty"):
        validate_artifact(universe)


def test_capture_rechecks_window_before_publication():
    universe_clock = iter([PRE_OPEN, dt.datetime(2026, 9, 15, 1, 30, tzinfo=dt.UTC)])
    with pytest.raises(ValueError, match="before open"):
        _capture(PRE_OPEN, now_fn=lambda: next(universe_clock)).capture_universe(
            SESSION
        )

    universe = _capture(PRE_OPEN).capture_universe(SESSION)
    inputs_clock = iter([POST_CLOSE, dt.datetime(2026, 9, 16, 1, 30, tzinfo=dt.UTC)])
    with pytest.raises(ValueError, match="before next open"):
        _capture(POST_CLOSE, now_fn=lambda: next(inputs_clock)).capture_inputs(
            universe, model_version="ranked-v1", horizons=[20]
        )


def test_universe_write_rechecks_actual_publication_window(tmp_path):
    universe = _capture(PRE_OPEN).capture_universe(SESSION)
    output = tmp_path / "universe.json"

    with pytest.raises(ValueError, match="before open"):
        write_artifact_exclusive(output, universe, now_fn=lambda: POST_CLOSE)

    assert not output.exists()
