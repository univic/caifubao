# -*- coding: utf-8 -*-
"""Halt / kill switch (roadmap 2.4).

Order generation must check the persisted, default-OFF halt flag FIRST and, when
halted, refuse loudly (no orders, no plan, no forward evidence). The flag must
record who/when/why, survive a process boundary, and carry no silent default
path. All fixtures are synthetic and file-backed (tmp_path); no network, no DB.
"""

import datetime
import json
import os
import subprocess
import sys

import pytest

from app.lib.strategy_engine.halt import (
    ENV_HALT_FILE,
    HaltError,
    assert_not_halted,
    engage_halt,
    is_halted,
    read_halt_state,
    resolve_halt_path,
    resume_halt,
)

# datahub/ (the directory `python -m app...` runs from)
REPO = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


@pytest.fixture
def halt_file(tmp_path, monkeypatch):
    """A per-test halt path with no flag file yet (default OFF)."""
    path = str(tmp_path / "halt.json")
    monkeypatch.setenv(ENV_HALT_FILE, path)
    return path


def _fake_runner_env(monkeypatch, *, fail_if_orders_generated=False):
    """Minimal model stubs so run_strategy can reach the plan step.

    With ``fail_if_orders_generated`` the prediction query raises, proving that
    order generation never started (the halt check runs first).
    """
    import app.jobs.strategy_runner as runner
    import app.model.scoring as model_scoring
    import app.model.strategy as model_strategy

    class FakeRegistered:
        def __init__(self, config):
            self.config = config

    class FakeQS:
        def __init__(self, items):
            self.items = items

        def first(self):
            return self.items[0] if self.items else None

        def limit(self, count):
            return FakeQS(self.items[:count])

        def order_by(self, *fields):
            return self

        def __iter__(self):
            return iter(self.items)

        def delete(self):
            self.items = []

    class FakeRegistry:
        @classmethod
        def objects(cls, **query):
            return FakeQS([FakeRegistered({"20": {"directions": {"momentum": -1}}})])

    class FakeRunModel:
        @classmethod
        def objects(cls, **query):
            return FakeQS([])

    class FakeWindowModel:
        @classmethod
        def objects(cls, **query):
            return FakeQS([])

    monkeypatch.setattr(model_scoring, "ScoreModelVersion", FakeRegistry)
    monkeypatch.setattr(model_strategy, "StrategyPaperRun", FakeRunModel)
    monkeypatch.setattr(model_strategy, "StrategyForwardWindow", FakeWindowModel)
    monkeypatch.setattr(runner, "_execution_date", lambda date: date)
    if fail_if_orders_generated:

        def _fail(*_args, **_kwargs):
            raise AssertionError("order generation started despite the halt")

        monkeypatch.setattr(runner, "_query_usable_predictions", _fail)
    else:
        monkeypatch.setattr(runner, "_query_usable_predictions", lambda mv, d, h: [])
    monkeypatch.setattr(runner, "_query_flags", lambda date, h, model_version=None: {})
    return runner


def test_halt_path_has_no_silent_default(monkeypatch):
    monkeypatch.delenv(ENV_HALT_FILE, raising=False)
    with pytest.raises(ValueError, match="not configured"):
        resolve_halt_path(None)
    with pytest.raises(ValueError, match="not configured"):
        is_halted(None)


def test_missing_file_reads_as_default_off(halt_file):
    assert not os.path.exists(halt_file)
    state = read_halt_state(halt_file)
    assert state["halted"] is False
    assert assert_not_halted(halt_file) is None


def test_engage_records_who_when_and_why(halt_file):
    state = engage_halt(
        changed_by="operator-a", reason="manual emergency stop", halt_path=halt_file
    )
    assert state["halted"] is True
    assert state["changed_by"] == "operator-a"
    assert state["reason"] == "manual emergency stop"
    assert state["changed_at"]
    assert state["history"][-1]["action"] == "halt"
    # Persisted, not just returned in memory.
    on_disk = json.loads(open(halt_file, encoding="utf-8").read())
    assert on_disk["halted"] is True
    assert on_disk["changed_by"] == "operator-a"
    assert is_halted(halt_file) is True
    with pytest.raises(HaltError, match="HALTED"):
        assert_not_halted(halt_file)


def test_resume_restores_order_generation(halt_file):
    engage_halt(changed_by="op", reason="stop", halt_path=halt_file)
    state = resume_halt(changed_by="op", reason="all clear", halt_path=halt_file)
    assert state["halted"] is False
    assert state["history"][-1]["action"] == "resume"
    assert is_halted(halt_file) is False
    assert assert_not_halted(halt_file) is None


def test_engage_requires_an_operator_and_reason(halt_file):
    with pytest.raises(ValueError, match="changed_by"):
        engage_halt(changed_by="  ", reason="stop", halt_path=halt_file)
    with pytest.raises(ValueError, match="reason"):
        engage_halt(changed_by="op", reason="  ", halt_path=halt_file)


def test_corrupt_flag_file_fails_closed(tmp_path, monkeypatch):
    path = tmp_path / "halt.json"
    path.write_text("{not json", encoding="utf-8")
    monkeypatch.setenv(ENV_HALT_FILE, str(path))
    with pytest.raises(ValueError, match="cannot read halt flag file"):
        is_halted(str(path))


def test_deleting_the_flag_file_returns_to_un_halted(halt_file):
    """Documented stickiness boundary: only an ABSENT PATH fails closed. Losing
    (deleting) the flag file returns the runner to normal order generation."""
    engage_halt(changed_by="op", reason="stop", halt_path=halt_file)
    assert is_halted(halt_file) is True
    os.remove(halt_file)
    assert is_halted(halt_file) is False
    assert assert_not_halted(halt_file) is None


def test_halt_state_persists_across_processes(halt_file, tmp_path):
    """A separate process engages the flag; this process must see HALTED."""
    script = (
        "from app.lib.strategy_engine.halt import engage_halt;"
        "engage_halt(changed_by='sub-process', reason='cross-process stop');"
        "print('engaged')"
    )
    env = dict(os.environ)
    env[ENV_HALT_FILE] = halt_file
    env["PYTHONPATH"] = REPO + os.pathsep + env.get("PYTHONPATH", "")
    completed = subprocess.run(
        [sys.executable, "-c", script],
        cwd=REPO,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )
    assert completed.returncode == 0, completed.stderr
    assert "engaged" in completed.stdout
    state = read_halt_state(halt_file)
    assert state["halted"] is True
    assert state["changed_by"] == "sub-process"


def test_run_strategy_is_halted_before_any_order_generation(halt_file, monkeypatch):
    runner = _fake_runner_env(monkeypatch)
    engage_halt(changed_by="op", reason="emergency", halt_path=halt_file)
    with pytest.raises(HaltError, match="no orders were produced"):
        runner.run_strategy(
            date=datetime.datetime(2026, 4, 10, tzinfo=datetime.UTC),
            config={"score_model_version": "flip_wide_shadow_v1", "horizon": 20},
            dry_run=True,
            halt_path=halt_file,
        )


def test_run_strategy_resumes_after_the_halt_is_lifted(halt_file, monkeypatch):
    runner = _fake_runner_env(monkeypatch)
    engage_halt(changed_by="op", reason="emergency", halt_path=halt_file)
    with pytest.raises(HaltError):
        runner.run_strategy(
            date=datetime.datetime(2026, 4, 10, tzinfo=datetime.UTC),
            config={"score_model_version": "flip_wide_shadow_v1", "horizon": 20},
            dry_run=True,
            halt_path=halt_file,
        )
    resume_halt(changed_by="op", reason="resolved", halt_path=halt_file)
    result = runner.run_strategy(
        date=datetime.datetime(2026, 4, 10, tzinfo=datetime.UTC),
        config={"score_model_version": "flip_wide_shadow_v1", "horizon": 20},
        dry_run=True,
        halt_path=halt_file,
    )
    assert result["dry_run"] is True


def test_unconfigured_halt_store_fails_order_generation_closed(monkeypatch):
    runner = _fake_runner_env(monkeypatch)
    monkeypatch.delenv(ENV_HALT_FILE, raising=False)
    with pytest.raises(ValueError, match="not configured"):
        runner.run_strategy(
            date=datetime.datetime(2026, 4, 10, tzinfo=datetime.UTC),
            config={"score_model_version": "flip_wide_shadow_v1", "horizon": 20},
            dry_run=True,
            halt_path=None,
        )


def test_run_command_reports_halted_fails_the_job_and_persists_nothing(
    halt_file, monkeypatch, capsys
):
    """The operator surface (not just `run_strategy`): `run --dry-run` while
    halted must print {"status": "HALTED"}, exit 2, record the job FAILED, and
    never start order generation (no plan, nothing persisted)."""
    from app.lib.utilities import job_run_helper

    runner = _fake_runner_env(monkeypatch, fail_if_orders_generated=True)
    engage_halt(changed_by="operator-a", reason="emergency", halt_path=halt_file)
    finished = {}

    monkeypatch.setattr(
        job_run_helper, "compute_daily_schedule_at", lambda hour, minute: None
    )
    monkeypatch.setattr(job_run_helper, "create_job_run", lambda context: object())

    def _finish(job_run, *, status=None, summary=None, error_message=None):
        finished["status"] = status
        finished["error_message"] = error_message

    monkeypatch.setattr(job_run_helper, "finish_job_run", _finish)

    args = runner.build_parser().parse_args(
        ["run", "--date", "2026-04-10", "--dry-run", "--halt-file", halt_file]
    )
    with pytest.raises(SystemExit) as excinfo:
        runner._run_with_tracking(
            args, {"score_model_version": "flip_wide_shadow_v1", "horizon": 20}
        )
    assert excinfo.value.code == 2
    payload = json.loads(capsys.readouterr().out)
    assert payload["status"] == "HALTED"
    assert payload["halted"] is True
    assert "operator-a" in payload["error"]
    assert finished["status"] == job_run_helper.STATUS_FAILED
    assert "HALTED" in finished["error_message"]


def test_halt_cli_subcommands_parse_and_require_attribution():
    from app.jobs.strategy_runner import build_parser

    parser = build_parser()
    engage = parser.parse_args(
        ["halt", "engage", "--by", "op", "--reason", "stop", "--halt-file", "/tmp/h"]
    )
    assert engage.halt_command == "engage"
    assert engage.by == "op"
    resume = parser.parse_args(["halt", "resume", "--by", "op", "--reason", "clear"])
    assert resume.halt_command == "resume"
    status = parser.parse_args(["halt", "status"])
    assert status.halt_command == "status"
    with pytest.raises(SystemExit):
        parser.parse_args(["halt", "engage", "--reason", "stop"])
    # The run surface can pin the same flag file.
    run = parser.parse_args(["run", "--date", "2026-09-04", "--halt-file", "/tmp/h"])
    assert run.halt_file == "/tmp/h"
