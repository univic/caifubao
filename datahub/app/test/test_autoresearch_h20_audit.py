import json

import pandas as pd
import pytest

from app.jobs.autoresearch_h20_audit import audit, compare_quotes, main


def sample():
    return pd.DataFrame(
        {
            "date": ["2026-01-01", "2026-01-02", "2026-02-01"],
            "stock_code": ["a"] * 3,
            "open_hfq": [100.0, 100.0, 99.0],
            "close_hfq": [100.0, 100.0, 99.0],
            "actual_entry_date": ["2026-01-02", None, None],
            "actual_exit_date": ["2026-02-01", None, None],
            "actual_entry_open_hfq": [100.0, None, None],
            "actual_exit_open_hfq": [99.0, None, None],
            "eligibility": [True, False, False],
        }
    )


def test_holding_period_units_and_coverage():
    result = audit(sample(), rate_cost=0)
    assert result["returns"]["mean_holding_period_cohort_return"] == pytest.approx(
        -0.01
    )
    assert result["returns"]["signal_dates"] == 1
    assert result["labels"]["entry"]["matched"] == 1
    assert result["labels"]["entry"]["missing_label_date"] == 2
    assert "nav" not in result


def test_corruption_and_missing_are_separate():
    frame = sample()
    frame.loc[0, "actual_entry_open_hfq"] = 101
    assert audit(frame)["labels"]["entry"]["price_mismatches"] == 1
    frame.loc[0, "actual_entry_date"] = "2025-12-01"
    result = audit(frame)
    assert result["eligible_invalid_chronology"] == 1
    assert result["labels"]["entry"]["unmatched"] == 1
    frame.loc[0, "actual_exit_open_hfq"] = float("inf")
    assert audit(frame)["eligible_unusable_execution_prices"] == 1
    assert audit(frame)["returns"]["rows"] == 0


def test_duplicate_keys_do_not_multiply_joins():
    frame = pd.concat([sample(), sample().iloc[[0]]], ignore_index=True)
    result = audit(frame)
    assert result["duplicate_key_rows"] == 2
    assert result["labels"] is None
    assert result["returns"] is None


def test_reference_missing_invalid_and_timezone():
    reference = sample()[["date", "stock_code", "open_hfq", "close_hfq"]]
    reference["date"] = pd.to_datetime(reference.date, utc=True)
    reference.loc[1, "open_hfq"] = float("nan")
    reference.loc[2, "stock_code"] = "missing"
    result = compare_quotes(sample(), reference)
    assert result["matched"] == 2
    assert result["unmatched"] == 1
    assert result["fields"]["open_hfq"]["unusable_pairs"] == 1
    assert result["fields"]["close_hfq"]["price_mismatches"] == 0
    with pytest.raises(ValueError, match="duplicate"):
        compare_quotes(sample(), pd.concat([reference, reference]))


def test_cli_preserves_inputs_and_serializes_empty_window(tmp_path):
    path = tmp_path / "snapshot.parquet"
    sample().to_parquet(path)
    before = path.read_bytes()
    with pytest.raises(ValueError, match="exists"):
        main(["--snapshot", str(path), "--output", str(path)])
    output = tmp_path / "audit.json"
    main(
        ["--snapshot", str(path), "--from-date", "2030-01-01", "--output", str(output)]
    )
    assert (
        json.loads(output.read_text())["returns"]["mean_holding_period_cohort_return"]
        is None
    )
    assert path.read_bytes() == before


def test_snapshot_overlap_distinguishes_missing_and_changed_fields():
    from app.jobs.autoresearch_h20_audit import compare_snapshots

    other = sample().iloc[:2].copy()
    other.loc[0, "eligibility"] = False
    result = compare_snapshots(sample(), other)
    assert result["matched"] == 2
    assert result["snapshot_only"] == 1
    assert result["exact_field_mismatches"]["eligibility"] == 1


def test_cohorts_have_equal_weight_despite_different_stock_counts():
    first = sample()
    second = sample()
    second["stock_code"] = "b"
    second.loc[0, "actual_exit_open_hfq"] = 103
    second.loc[2, "open_hfq"] = 103
    third = sample()
    third["stock_code"] = "c"
    third.loc[0, "date"] = "2025-12-31"
    third.loc[0, "actual_exit_open_hfq"] = 110
    third.loc[2, "open_hfq"] = 110
    result = audit(pd.concat([first, second, third]), rate_cost=0)
    assert result["returns"]["signal_dates"] == 2
    assert result["returns"]["mean_holding_period_cohort_return"] == pytest.approx(
        0.055
    )
