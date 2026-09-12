# -*- coding: utf-8 -*-
"""Contract test: the `strategy` CLI domain must emit vectors the runner accepts.

Roadmap 0.1's daily capture is operated through `scripts/caifubao strategy ...`.
A flag the wrapper silently drops (or forwards to a subcommand that rejects it)
only shows up at pod-exec time, and nothing else in CI exercises `scripts/`.
So: source the CLI with a stubbed pod exec, then feed each emitted vector into
the runner's real `argparse` parser.

This lives in the datahub suite only because that is where CI runs; it reads the
repo-level operator script and asserts no datahub behaviour.
"""

import pathlib
import shutil
import subprocess

import pytest

from app.jobs.strategy_runner import build_parser

REPO_ROOT = pathlib.Path(__file__).resolve().parents[3]
CLI_PATH = REPO_ROOT / "scripts" / "caifubao"

pytestmark = pytest.mark.skipif(
    shutil.which("bash") is None or not CLI_PATH.exists(),
    reason="bash and scripts/caifubao are required",
)


def _run_cli(call: str) -> subprocess.CompletedProcess:
    """Source the CLI, stub the cluster hop, and run one `cmd_strategy_*` call."""
    script = "\n".join(
        [
            f'source "{CLI_PATH}" >/dev/null 2>&1 || true',
            'emit() { printf "%s\\n" "$*"; }',
            '_pod_exec() { emit "$@"; }',
            "info() { :; }",
            "warn() { :; }",
            call,
        ]
    )
    return subprocess.run(
        ["bash", "-c", script], capture_output=True, text=True, check=False
    )


def _runner_argv(call: str) -> list[str]:
    """The argv the CLI handed to `python -m app.jobs.strategy_runner`."""
    result = _run_cli(call)
    assert result.returncode == 0, f"{call!r} failed: {result.stderr}"
    lines = [line for line in result.stdout.strip().splitlines() if line]
    assert lines, f"{call!r} emitted no command"
    tokens = lines[-1].split(" ")
    assert tokens[:3] == ["python", "-m", "app.jobs.strategy_runner"], tokens[:3]
    return tokens[3:]


@pytest.mark.parametrize(
    "call",
    [
        # The regression that mattered: a dropped --config-json would leave the
        # daily run on the default config, whose hash never matches the
        # certified window, so every run would classify REPLAY.
        "cmd_strategy_run 2026-09-11 --config-json '{\"initial_nav\":20000000}'",
        "cmd_strategy_run 2026-09-11 --config-json '{\"initial_nav\":20000000}' --dry-run",
        "cmd_strategy_run 2026-09-11",
        "cmd_strategy_report 2026-09-11",
        "cmd_strategy_report 2026-09-11 --model-version flip_wide_shadow_v1",
        "cmd_strategy_nav --from 2026-03-01 --to 2026-08-31",
        "cmd_strategy_nav --from 2026-03-01 --to 2026-08-31 --config-json '{\"horizon\":20}' --horizon 20",
        "cmd_strategy_forward progress --model-version flip_wide_shadow_v1 --horizon 20",
        "cmd_strategy_forward close --model-version flip_wide_shadow_v1 --horizon 20",
        "cmd_strategy_forward certify --model-version flip_wide_shadow_v1 --horizon 20 --yes",
        "cmd_strategy_forward certify --model-version flip_wide_shadow_v1 --horizon 20 --config-json '{\"horizon\":20}' --yes",
    ],
)
def test_emitted_vector_is_accepted_by_the_runner(call):
    argv = _runner_argv(call)
    # Raises SystemExit(2) on an unknown/missing argument, which is the failure
    # mode a stub-only check cannot see.
    build_parser().parse_args(argv)


def test_run_config_json_survives_the_wrapper():
    argv = _runner_argv(
        "cmd_strategy_run 2026-09-11 --config-json '{\"initial_nav\":20000000}'"
    )
    assert "--config-json" in argv
    assert argv[argv.index("--config-json") + 1] == '{"initial_nav":20000000}'


@pytest.mark.parametrize(
    "call",
    [
        # certify is the promotion gate: it must refuse without explicit consent.
        "cmd_strategy_forward certify --model-version v1 --horizon 20",
        # close/progress do not take a config; forwarding one would exit 2.
        "cmd_strategy_forward close --model-version v1 --horizon 20 --config-json '{}'",
        "cmd_strategy_forward progress --model-version v1 --horizon 20 --config-json '{}'",
        # run takes its score source from the config JSON, never as a flag.
        "cmd_strategy_run 2026-09-11 --model-version v1",
        "cmd_strategy_run 2026-09-11 --horizon 20",
        # unknown subcommand
        "cmd_strategy_forward bogus --model-version v1 --horizon 20",
        # missing required value
        "cmd_strategy_forward progress --model-version v1",
        "cmd_strategy_nav --from",
        # A misspelled option must NOT be silently swallowed: on the
        # promotion-gate path a typo would otherwise reproduce the original
        # defect (default config -> hash mismatch -> every run REPLAY -> a
        # permanent gap in the evidence window).
        "cmd_strategy_run 2026-09-11 --config_json '{\"a\":1}'",
        "cmd_strategy_run 2026-09-11 --horizion 20",
        "cmd_strategy_run 2026-09-11 --bogus",
        "cmd_strategy_report 2026-09-11 --bogus",
        "cmd_strategy_nav --from 2026-03-01 --to 2026-08-31 --bogus",
        "cmd_strategy_forward progress --model-version v1 --horizon 20 --bogus",
        # --yes is certify-only
        "cmd_strategy_forward close --model-version v1 --horizon 20 --yes",
    ],
)
def test_misuse_fails_closed(call):
    result = _run_cli(call)
    assert result.returncode != 0, f"{call!r} unexpectedly succeeded"
    assert "unbound variable" not in result.stderr, (
        f"{call!r} died with a bash error instead of usage: {result.stderr}"
    )


@pytest.mark.parametrize(
    "call",
    [
        # Route through main() so a mis-wired dispatcher case is caught too.
        "main strategy run 2026-09-11 --config-json '{\"a\":1}'",
        "main strategy report 2026-09-11",
        "main strategy nav --from 2026-03-01 --to 2026-08-31",
        "main strategy forward progress --model-version v1 --horizon 20",
        "main strategy forward close --model-version v1 --horizon 20",
        "main strategy forward certify --model-version v1 --horizon 20 --yes",
    ],
)
def test_main_dispatch_reaches_the_runner(call):
    build_parser().parse_args(_runner_argv(call))


def test_main_rejects_an_unknown_strategy_subcommand():
    result = _run_cli("main strategy bogus")
    assert result.returncode != 0


def test_usage_documents_the_run_config_flag():
    result = subprocess.run(
        ["bash", str(CLI_PATH), "--help"], capture_output=True, text=True, check=False
    )
    assert result.returncode == 0
    assert "strategy run [DATE] [--config-json JSON]" in result.stdout
    assert "strategy forward certify" in result.stdout
