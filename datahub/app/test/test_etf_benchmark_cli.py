"""Exercise local CLI, output safety and halt boundaries end to end."""

import copy
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys

import pytest

from app.lib.strategy_engine import etf_benchmark as benchmark

ROOT = Path(__file__).resolve().parents[3]
EXAMPLE = ROOT / "datahub" / "examples" / "etf-benchmark-100k.json"


@pytest.fixture
def paths(tmp_path):
    source, output, halt = [
        tmp_path / name for name in ("input.json", "output.json", "halt.json")
    ]
    source.write_text(EXAMPLE.read_text())
    return source, output, halt


def argv(paths):
    source, output, halt = paths
    return ["--input", str(source), "--output", str(output), "--halt-file", str(halt)]


def test_cli_atomic_output_matches_stdout_and_repeated_run(paths, capsys):
    assert benchmark.main(argv(paths)) == 0
    first = paths[1].read_bytes()
    assert benchmark.main(argv(paths)) == 0
    assert paths[1].read_bytes() == first
    source, _, halt = paths
    assert benchmark.main(["--input", str(source), "--halt-file", str(halt)]) == 0
    assert capsys.readouterr().out.encode() == first
    result = json.loads(first)
    assert result["evidence_kind"] == "REPLAY"
    assert result["final_account"]["cash"] == "375.10"
    assert result["curve"][-1]["nav"] == "104955.10"


@pytest.mark.parametrize("kind", ["same", "symlink", "hardlink", "halt"])
def test_cli_refuses_output_aliases(paths, kind):
    source, output, halt = paths
    if kind == "same":
        output = source
    elif kind == "symlink":
        output.symlink_to(source)
    elif kind == "hardlink":
        os.link(source, output)
    else:
        halt.write_text('{"halted": false}')
        output = halt
    before = output.read_bytes()
    assert benchmark.main(argv((source, output, halt))) == 1
    assert output.read_bytes() == before


def test_cli_invalid_input_preserves_old_output(paths):
    paths[0].write_text('{"schema_version": "wrong"}')
    paths[1].write_text("old result")
    assert benchmark.main(argv(paths)) == 1
    assert paths[1].read_text() == "old result"


def test_cli_atomic_replace_failure_preserves_old_output(paths, monkeypatch):
    paths[1].write_text("old result")
    before = set(paths[0].parent.iterdir())

    def fail(*_):
        raise OSError("simulated disk failure")

    monkeypatch.setattr(benchmark.os, "replace", fail)
    assert benchmark.main(argv(paths)) == 1
    assert paths[1].read_text() == "old result"
    assert set(paths[0].parent.iterdir()) == before


def test_cli_halt_at_entry_and_before_output(paths, monkeypatch):
    paths[2].write_text('{"halted": true}')
    assert benchmark.main(argv(paths)) == 2
    assert not paths[1].exists()
    paths[2].write_text('{"halted": false}')
    replay = benchmark.replay_etf_benchmark

    def engage_after_replay(*args, **kwargs):
        result = replay(*args, **kwargs)
        paths[2].write_text('{"halted": true}')
        return result

    monkeypatch.setattr(benchmark, "replay_etf_benchmark", engage_after_replay)
    assert benchmark.main(argv(paths)) == 2
    assert not paths[1].exists()


def test_cli_requires_configured_halt_and_rejects_corrupt_store(paths, monkeypatch):
    monkeypatch.delenv("CAIFUBAO_STRATEGY_HALT_FILE", raising=False)
    assert benchmark.main(["--input", str(paths[0])]) == 1
    paths[2].write_text("corrupt")
    assert benchmark.main(argv(paths)) == 1
    assert not paths[1].exists()


@pytest.mark.parametrize("value", ["NaN", "Infinity", "-Infinity"])
def test_cli_rejects_nonfinite_json(paths, value):
    payload = EXAMPLE.read_text().replace('"100000.00"', value)
    paths[0].write_text(payload)
    assert benchmark.main(argv(paths)) == 1
    assert not paths[1].exists()


def test_cli_rejects_duplicate_json_keys(paths):
    payload = EXAMPLE.read_text().replace(
        '"price_basis": "raw",', '"price_basis": "raw", "price_basis": "hfq",'
    )
    paths[0].write_text(payload)
    assert benchmark.main(argv(paths)) == 1
    assert not paths[1].exists()


def test_default_cash_and_explicit_cash_normalise_to_identical_result(paths):
    data = json.loads(EXAMPLE.read_text())
    explicit = benchmark.replay_etf_benchmark(data, halt_path=str(paths[2]))
    omitted = copy.deepcopy(data)
    del omitted["initial_cash"]
    assert benchmark.replay_etf_benchmark(omitted, halt_path=str(paths[2])) == explicit


@pytest.mark.skipif(shutil.which("bash") is None, reason="bash required")
def test_unified_cli_is_local_and_passes_paths_with_spaces(paths, tmp_path):
    source = tmp_path / "input with spaces.json"
    output = tmp_path / "output with spaces.json"
    source.write_text(EXAMPLE.read_text())
    stub = tmp_path / "kubectl"
    stub.write_text("#!/bin/sh\nexit 99\n")
    stub.chmod(0o755)
    env = dict(
        os.environ,
        CFB_LOCAL_PYTHON=sys.executable,
        PATH=f"{tmp_path}{os.pathsep}{os.environ.get('PATH', '')}",
    )
    result = subprocess.run(
        [
            "bash",
            str(ROOT / "scripts" / "caifubao"),
            "strategy",
            "benchmark",
            *argv((source, output, paths[2])),
        ],
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    assert json.loads(output.read_text())["final_account"]["cash"] == "375.10"
