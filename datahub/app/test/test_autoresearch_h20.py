import json
import math

import pandas as pd
import pytest

from app.jobs.autoresearch_h20_runner import main
from app.lib.autoresearch.h20_excess_alpha import (
    apply_friction,
    equal_weight_benchmark,
    information_ratio,
    profitability_score,
    rank_components,
    validate_candidate,
    validate_snapshot,
)

COMPONENTS = (
    "signal_strength",
    "momentum",
    "trend_alignment",
    "breakout_or_position",
    "industry_momentum",
    "relative_strength",
    "real_relative_strength",
    "risk_penalty",
)


def candidate():
    return {
        "name": "test",
        "component_directions": {
            name: (-1 if name == "risk_penalty" else 1) for name in COMPONENTS
        },
        "weights": {name: 12.5 for name in COMPONENTS},
        "selection": {
            "mode": "top_percentile",
            "lower": 0.5,
            "upper": 1.0,
            "portfolio_size": 2,
        },
        "regime_filter": {"mode": "none"},
    }


def profile(tmp_path):
    return {
        "experiment": {
            "train_range": ["2024-01-01", "2024-12-31"],
            "validation_range": ["2025-01-01", "2025-06-30"],
            "test_range": ["2025-07-01", "2026-07-31"],
        },
        "execution": {
            "commission_rate": 0.00025,
            "minimum_commission_cny": 5.0,
            "sell_stamp_duty_rate": 0.001,
            "slippage_per_side": 0.001,
        },
        "metric": {
            "annualized_net_excess_return_weight": 0.1,
            "drawdown_free_allowance": 0.1,
            "drawdown_penalty_weight": 2.0,
            "turnover_free_allowance": 6.0,
            "turnover_penalty_weight": 0.02,
            "concentration_free_allowance": 0.25,
            "concentration_penalty_weight": 1.0,
            "minimum_completed_trades": 5,
            "minimum_eligible_trading_days": 120,
            "maximum_profit_concentration": 0.4,
            "maximum_walk_forward_decay": 0.2,
            "hard_failure_score": -999.0,
        },
        "artifacts": {
            "snapshot_path": str(tmp_path / "snapshot.parquet"),
            "snapshot_manifest_path": str(tmp_path / "manifest.json"),
            "latest_report_path": str(tmp_path / "report.json"),
            "results_tsv_path": str(tmp_path / "results.tsv"),
            "ledger_path": str(tmp_path / "ledger.jsonl"),
        },
        "logging": {"run_log_path": str(tmp_path / "baseline.log")},
    }


def snapshot_frame():
    rows = []
    for date in pd.date_range("2025-01-02", periods=130, freq="B", tz="UTC"):
        for index, code in enumerate(("sh600001", "sh600002", "sh600003")):
            row = {
                "date": date,
                "stock_code": code,
                "is_bse": False,
                "is_st": False,
                "listing_days": 100,
                "trade_status": 1,
                "requested_entry_date": date + pd.offsets.BDay(1),
                "actual_entry_date": date + pd.offsets.BDay(1),
                "actual_entry_open_hfq": 10.0 + index,
                "requested_exit_date": date + pd.offsets.BDay(21),
                "actual_exit_date": date + pd.offsets.BDay(21),
                "actual_exit_open_hfq": 10.5 + index,
                "eligibility": True,
                "eligibility_reason": "eligible",
            }
            row.update({name: float(index) for name in COMPONENTS})
            rows.append(row)
    return pd.DataFrame(rows)


def test_candidate_validation_and_ranking():
    config = candidate()
    validate_candidate(config)
    bad = candidate()
    bad["weights"]["momentum"] = 50
    with pytest.raises(ValueError, match="sum to 100"):
        validate_candidate(bad)
    ranked = rank_components(snapshot_frame().head(3), config)
    assert (
        ranked.loc[ranked.stock_code == "sh600003", "momentum_percentile"].iloc[0]
        == 1.0
    )
    assert ranked.loc[ranked.stock_code == "sh600003", "candidate_score"].iloc[0] < 1.0


def test_friction_and_information_ratio(tmp_path):
    cfg = profile(tmp_path)
    price, commission, duty, slip = apply_friction(10.0, 1000, "SELL", cfg)
    assert price == pytest.approx(9.99)
    assert commission == 5.0
    assert duty == pytest.approx(9.99)
    assert slip == pytest.approx(10.0)
    values = pd.Series([0.01, 0.02, -0.01])
    assert information_ratio(values) == pytest.approx(
        values.mean() / values.std(ddof=1) * math.sqrt(252 / 20)
    )


def test_returns_exclusively_use_actual_execution_labels(tmp_path):
    frame = snapshot_frame().head(3)
    expected = (
        (frame.actual_exit_open_hfq / frame.actual_entry_open_hfq - 1)
        .groupby(frame.date)
        .mean()
    )
    pd.testing.assert_series_equal(equal_weight_benchmark(frame), expected)
    legacy = frame.assign(next_open_hfq=1.0)
    with pytest.raises(ValueError, match="legacy execution labels"):
        validate_snapshot(legacy, profile(tmp_path))


def test_hard_gate(tmp_path):
    metrics = {
        "information_ratio": 1.0,
        "annualized_net_excess_return": 0.1,
        "excess_max_drawdown": -0.05,
        "annual_turnover": 2.0,
        "profit_concentration": 0.2,
        "completed_trades": 4,
        "eligible_trading_days": 130,
        "walk_forward_decay": 0.0,
    }
    score, flags = profitability_score(metrics, profile(tmp_path))
    assert score == -999.0
    assert flags == ["low_sample"]


def test_prepare_and_metric_cli(tmp_path, capsys):
    cfg = profile(tmp_path)
    profile_path = tmp_path / "profile.yaml"
    profile_path.write_text(json.dumps(cfg))
    source = tmp_path / "source.parquet"
    snapshot_frame().to_parquet(source)
    assert (
        main(
            ["prepare", "--profile", str(profile_path), "--source-parquet", str(source)]
        )
        == 0
    )
    manifest = json.loads((tmp_path / "manifest.json").read_text())
    assert manifest["schema_version"] == "h20-excess-alpha-v1"
    report = tmp_path / "report.json"
    report.write_text(json.dumps({"research_profitability_score": 1.25}))
    assert main(["metric", "--report", str(report)]) == 0
    assert capsys.readouterr().out == "1.25\n"
