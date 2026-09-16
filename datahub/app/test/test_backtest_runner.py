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


def _replay_manifest():
    sha_a = "a" * 64
    sha_b = "b" * 64
    return {
        "schema_version": "timing-replay-p1",
        "cohort": {
            "as_of": "2026-01-04T07:00:00Z",
            "source": "fixture:pit-members-v1",
            "membership_basis": "point_in_time",
            "delisted_completeness": "VERIFIED",
            "provenance": {
                "artifact_uri": "s3://fixture/members.json",
                "artifact_sha256": sha_a,
                "member_count": 1,
                "subsequently_delisted_count": 0,
                "suspended_count": 0,
                "includes_subsequently_delisted": True,
                "includes_suspended": True,
            },
            "members": [
                {
                    "stock_code": "sh600001",
                    "listed_on": "2010-01-01",
                    "delisted_on": None,
                    "suspended_at_as_of": False,
                    "evidence_at": "2026-01-04T06:00:00Z",
                }
            ],
        },
        "prediction_cohorts": {
            "2026-01-05": {
                "artifact_uri": "s3://fixture/ranks/2026-01-05.json",
                "artifact_sha256": sha_b,
                "prediction_root_sha256": sha_a,
                "cohort_fingerprint": "333d83aefe911ca97a02afb247e5e648779d65c66a4b56cccf2cef0f7ad0170f",
                "member_codes": ["sh600001", "sh600002"],
                "member_count": 2,
                "data_as_of": "2026-01-05T07:00:00Z",
            }
        },
        "model": {
            "model_version": "ranked-pit-v1",
            "config_hash": "c" * 64,
            "horizon": 20,
        },
        "strategy": {"entry_percentile": 0.9, "exit_percentile": 0.3},
        "window": {"from": "2026-01-05", "to": "2026-01-05"},
        "trading_calendar": ["2026-01-05"],
        "initial_cash": 50000,
        "board_lot": 100,
        "friction": {
            "commission_rate": 0.00025,
            "minimum_commission": 5.0,
            "stamp_duty_rate": 0.001,
            "slippage_rate": 0.001,
        },
    }


def test_timing_replay_parser_registers_real_research_command():
    args = backtest_runner.build_parser().parse_args(
        ["timing-replay", "manifest.json", "--output", "report.json"]
    )

    assert args.command == "timing-replay"
    assert args.manifest_json == "manifest.json"
    assert args.output == "report.json"


def test_timing_replay_loads_read_only_evidence_and_invokes_p0(tmp_path, capsys):
    manifest_path = tmp_path / "manifest.json"
    report_path = tmp_path / "report.json"
    manifest_path.write_text(json.dumps(_replay_manifest()), encoding="utf-8")
    calls = []

    def evaluator(cohort, runner, **kwargs):
        calls.append(("evaluate", cohort, kwargs))
        pair = runner("sh600001", save_result=False)
        assert pair == {"timing": {}, "buy_hold": {}}
        return {"research_only": True, "validation_status": "UNVALIDATED"}

    def evidence_loader(stock_code, manifest):
        calls.append(("read", stock_code, manifest["model_version"]))
        return ["quote"], ["prediction"]

    result = backtest_runner.run_timing_replay(
        SimpleNamespace(manifest_json=str(manifest_path), output=str(report_path)),
        evaluator=evaluator,
        db_initializer=lambda: calls.append(("db",)),
        model_loader=lambda version: {
            "model_version": version,
            "config_hash": "c" * 64,
            "scoring_mode": "ranked",
            "status": "ACTIVE",
        },
        evidence_loader=evidence_loader,
        pair_builder=lambda code, quotes, predictions, manifest: {
            "timing": {},
            "buy_hold": {},
        },
    )

    assert result["research_only"] is True
    assert ("read", "sh600001", "ranked-pit-v1") in calls
    assert calls[0] == ("db",)
    assert "UNVALIDATED" in report_path.read_text(encoding="utf-8")
    assert "research_only" in capsys.readouterr().out


def test_timing_replay_rejects_model_before_loading_stock_evidence(tmp_path):
    manifest_path = tmp_path / "manifest.json"
    manifest_path.write_text(json.dumps(_replay_manifest()), encoding="utf-8")
    evidence_calls = []

    with pytest.raises(ValueError, match="status"):
        backtest_runner.run_timing_replay(
            SimpleNamespace(manifest_json=str(manifest_path), output=None),
            evaluator=lambda *_args, **_kwargs: {},
            db_initializer=lambda: None,
            model_loader=lambda _version: {
                "model_version": "ranked-pit-v1",
                "config_hash": "c" * 64,
                "scoring_mode": "ranked",
                "status": "RETIRED",
            },
            evidence_loader=lambda *_args: evidence_calls.append(True),
        )

    assert evidence_calls == []


def test_timing_replay_pair_is_accepted_by_the_real_p0_contract(tmp_path):
    manifest_path = tmp_path / "manifest.json"
    manifest_path.write_text(json.dumps(_replay_manifest()), encoding="utf-8")
    quote = {
        "date": "2026-01-05",
        "open_hfq": 10.0,
        "close_hfq": 10.5,
        "trade_status": 1,
        "change_rate": 1.0,
    }

    report = backtest_runner.run_timing_replay(
        SimpleNamespace(manifest_json=str(manifest_path), output=None),
        db_initializer=lambda: None,
        model_loader=lambda version: {
            "model_version": version,
            "config_hash": "c" * 64,
            "scoring_mode": "ranked",
            "status": "ACTIVE",
        },
        evidence_loader=lambda _code, _manifest: ([quote], []),
    )

    assert report["results"][0]["status"] == "SUCCESS"
    assert report["results"][0]["stock_code"] == "sh600001"
    assert report["results"][0]["observed_sessions"] == 1
    assert report["research_only"] is True
    assert report["validation_status"] == "UNVALIDATED"


def test_timing_replay_reports_stable_manifest_and_output_file_errors(tmp_path):
    missing = tmp_path / "missing.json"
    with pytest.raises(ValueError, match="cannot read timing replay manifest"):
        backtest_runner.run_timing_replay(
            SimpleNamespace(manifest_json=str(missing), output=None)
        )

    manifest_path = tmp_path / "manifest.json"
    manifest_path.write_text(json.dumps(_replay_manifest()), encoding="utf-8")
    missing_parent_output = tmp_path / "missing-directory" / "report.json"
    with pytest.raises(ValueError, match="cannot write timing replay report"):
        backtest_runner.run_timing_replay(
            SimpleNamespace(
                manifest_json=str(manifest_path), output=str(missing_parent_output)
            ),
            evaluator=lambda *_args, **_kwargs: {
                "research_only": True,
                "validation_status": "UNVALIDATED",
            },
            db_initializer=lambda: None,
            model_loader=lambda version: {
                "model_version": version,
                "config_hash": "c" * 64,
                "scoring_mode": "ranked",
                "status": "ACTIVE",
            },
        )
