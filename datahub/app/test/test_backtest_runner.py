import datetime
import json
from types import SimpleNamespace

import pytest

from app.jobs import backtest_runner


@pytest.mark.parametrize(
    "strategies",
    [
        ("SCORE_THRESHOLD",),
        ("SCORE_MOMENTUM",),
        ("MULTI_HORIZON_CONSENSUS",),
        ("TOP_N_ROTATION",),
        ("BUY_HOLD", "SCORE_THRESHOLD"),
    ],
)
def test_score_driven_cli_requires_model_version(strategies):
    args = SimpleNamespace(model_version=None)

    with pytest.raises(ValueError, match="model_version is required"):
        backtest_runner._require_score_model_version(args, *strategies)


def test_non_score_cli_does_not_require_model_version():
    args = SimpleNamespace(model_version=None)

    assert backtest_runner._require_score_model_version(args, "BUY_HOLD") is None


def test_score_driven_cli_accepts_explicit_model_version():
    args = SimpleNamespace(model_version="score_v2_202605b")

    assert (
        backtest_runner._require_score_model_version(args, "SCORE_THRESHOLD")
        == "score_v2_202605b"
    )


def test_timing_pool_requires_explicit_cohort_and_forwards_paired_results(
    tmp_path, capsys
):
    input_path = tmp_path / "input.json"
    output_path = tmp_path / "report.json"
    input_path.write_text(
        """{
          "cohort": ["sh600001"],
          "cohort_as_of": "2026-09-01T07:00:00Z",
          "cohort_source": "frozen-test",
          "model_version": "ranked-v1",
          "config": {"entry_percentile": 0.9, "exit_percentile": 0.3},
          "window": {"from": "2026-01-01", "to": "2026-06-30"},
          "delisted_completeness": "VERIFIED",
          "results": {
            "sh600001": {"timing": {"total_return_pct": 3}, "buy_hold": {"total_return_pct": 2}}
          }
        }""",
        encoding="utf-8",
    )
    captured = {}

    def evaluator(cohort, runner, **kwargs):
        captured.update(kwargs)
        captured["cohort"] = cohort
        captured["pair"] = runner("sh600001", save_result=False)
        return {"research_only": True, "validation_status": "UNVALIDATED"}

    result = backtest_runner.run_timing_pool(
        SimpleNamespace(input_json=str(input_path), output=str(output_path)),
        evaluator=evaluator,
    )

    assert result["research_only"] is True
    assert captured["cohort"] == ["sh600001"]
    assert captured["cohort_source"] == "frozen-test"
    assert set(captured["pair"]) == {"timing", "buy_hold"}
    assert "UNVALIDATED" in output_path.read_text(encoding="utf-8")
    assert "research_only" in capsys.readouterr().out


def test_timing_pool_parser_registers_research_command():
    args = backtest_runner.build_parser().parse_args(
        ["timing-pool", "cohort.json", "--output", "report.json"]
    )

    assert args.command == "timing-pool"
    assert args.input_json == "cohort.json"
    assert args.output == "report.json"


def test_timing_pool_runs_real_pure_evaluator_without_database(tmp_path, capsys):
    dates = [
        {
            "date": (
                datetime.date(2026, 1, 1) + datetime.timedelta(days=i)
            ).isoformat(),
            "equity": 100000.0 + i,
        }
        for i in range(120)
    ]
    assumptions = {
        "initial_cash": 100000.0,
        "window": {"from": "2026-01-01", "to": "2026-06-30"},
        "execution_timing": "next_trading_day_open",
        "valuation_timing": "last_close",
        "board_lot": 100,
        "friction": {
            "commission_rate": 0.00025,
            "minimum_commission": 5.0,
            "stamp_duty_rate": 0.001,
            "slippage_rate": 0.001,
        },
    }
    payload = {
        "cohort": ["sh600001"],
        "cohort_as_of": "2026-09-01T07:00:00Z",
        "cohort_source": "frozen-test",
        "model_version": "ranked-v1",
        "config": {"entry_percentile": 0.9, "exit_percentile": 0.3},
        "window": {"from": "2026-01-01", "to": "2026-06-30"},
        "delisted_completeness": "VERIFIED",
        "results": {
            "sh600001": {
                "timing": {
                    "stock_code": "sh600001",
                    "assumptions": assumptions,
                    "return_pct": 3.0,
                    "max_drawdown": -2.0,
                    "sharpe_ratio": 0.5,
                    "trades": [
                        {
                            "date": "2026-03-02",
                            "side": "SELL",
                            "status": "FILLED",
                            "quantity": 100,
                            "exec_price": 10.0,
                        }
                    ]
                    * 5,
                    "daily_values": dates,
                },
                "buy_hold": {
                    "stock_code": "sh600001",
                    "assumptions": assumptions,
                    "return_pct": 2.0,
                    "daily_values": dates,
                },
            }
        },
    }
    input_path = tmp_path / "input.json"
    input_path.write_text(json.dumps(payload), encoding="utf-8")

    report = backtest_runner.run_timing_pool(
        SimpleNamespace(input_json=str(input_path), output=None)
    )

    assert report["results"][0]["status"] == "SUCCESS"
    assert report["results"][0]["buy_hold_return_pct"] == 2.0
    assert report["validation_status"] == "UNVALIDATED"
    assert "UNVALIDATED" in capsys.readouterr().out
