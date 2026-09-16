# -*- coding: utf-8 -*-
"""Point-in-time 50k paper simulation for 2026-08-01..2026-09-11 (research).

Drives the *existing* scoring and paper-strategy runners one decision date at a
time, in ascending order, so every decision uses only data with date <= D:

* `scoring_runner backfill --from D --to D` recomputes that day's scores. The
  scoring engine's inputs are bounded by the evaluation date by construction
  (history strictly `< D`; day quote/factor/signal `== D`), so no future bar can
  reach a decision.
* `strategy_runner run --date D` selects holdings from those scores and executes
  at the next session's open (T+1), sized from the execution-day open.
* `strategy_runner nav` marks the book to market over the window.

The three variants separate the two things that differ between the deployed
production model and the research candidate:

| variant | model_version | scoring mode | construction |
|:--|:--|:--|:--|
| A production-as-deployed | pit_baseline_h20_v1 | raw | built-in weights, no flip |
| B flip candidate | pit_flip_ranked_h20_v1 | ranked | cross-sectional ranks, 7 components direction -1 |
| C ranked control | pit_baseline_ranked_h20_v1 | ranked | cross-sectional ranks, no flip |

B vs C isolates the direction flip; C vs A isolates the rank construction.

NOTE: the *raw* path ignores `config["directions"]` entirely (only
`score_all_stocks_ranked` applies them), so a flipped model_version silently
scores identically to the baseline under DATAHUB_SCORING_MODE=raw. That is why B
must run with DATAHUB_SCORING_MODE=ranked.
"""

import json
import os
import subprocess
import sys


def _assert_research_db() -> None:
    """These scripts WRITE predictions/paper runs: refuse non-research targets."""
    name = os.getenv("MONGODB_NAME", "").strip()
    if name != "caifubao-research" and os.getenv("PIT_SIM_ALLOW_DB") != name:
        raise SystemExit(
            f"refusing to run: MONGODB_NAME={name!r} is not the research database "
            "(set PIT_SIM_ALLOW_DB to that exact name to override)"
        )


# 5-session rebalance cadence (the configured `rebalance.cadence_days`).
DECISION_DATES = [
    "2026-08-03",
    "2026-08-10",
    "2026-08-17",
    "2026-08-24",
    "2026-08-31",
    "2026-09-07",
]

VARIANTS = [
    ("A_production_raw", "pit_baseline_h20_v1", "raw"),
    ("B_flip_ranked", "pit_flip_ranked_h20_v1", "ranked"),
    ("C_ranked_control", "pit_baseline_ranked_h20_v1", "ranked"),
]

HORIZON = 20
WINDOW_FROM = "2026-08-01"
WINDOW_TO = "2026-09-11"

BASE_CONFIG = {
    "timing_version": "paper_causal_v1",
    "horizon": HORIZON,
    "initial_nav": 50000,
    "selection": {
        "mode": "top_percentile",
        "lower": 0.2,
        "upper": 1.0,
        "portfolio_size": 10,
    },
    "constraints": {
        "exclude_st": True,
        "exclude_bse": True,
        "exclude_suspended": True,
        "max_single_stock_pct": 0.15,
        "min_trade_amount_cny": 0,
    },
    "rebalance": {"cadence_days": 5},
    "weighting": "equal",
    "cash_reserve_pct": 0,
}


def config_json(model_version: str) -> str:
    cfg = dict(BASE_CONFIG)
    cfg["score_model_version"] = model_version
    return json.dumps(cfg, separators=(",", ":"))


def run(cmd, label, env=None):
    proc = subprocess.run(cmd, capture_output=True, text=True, env=env)
    tail = (proc.stdout or "").strip().splitlines()
    print(
        "  %-46s rc=%d %s" % (label, proc.returncode, tail[-1][:120] if tail else ""),
        flush=True,
    )
    if proc.returncode != 0:
        print(proc.stderr[-600:], flush=True)
    return proc.returncode


def main() -> int:
    _assert_research_db()
    failures = 0
    for label, model_version, mode in VARIANTS:
        env = dict(os.environ)
        env["DATAHUB_SCORING_MODE"] = mode
        print("=" * 78, flush=True)
        print(
            "VARIANT %s  (model=%s mode=%s)" % (label, model_version, mode), flush=True
        )
        print("=" * 78, flush=True)
        for decision_date in DECISION_DATES:
            failures += run(
                [
                    sys.executable,
                    "-m",
                    "app.jobs.scoring_runner",
                    "backfill",
                    "--from",
                    decision_date,
                    "--to",
                    decision_date,
                    "--horizon",
                    str(HORIZON),
                    "--model-version",
                    model_version,
                    "--replace",
                ],
                "score %s" % decision_date,
                env=env,
            )
            failures += run(
                [
                    sys.executable,
                    "-m",
                    "app.jobs.strategy_runner",
                    "run",
                    "--date",
                    decision_date,
                    "--config-json",
                    config_json(model_version),
                    "--replace",
                ],
                "strategy run %s" % decision_date,
                env=env,
            )
        proc = subprocess.run(
            [
                sys.executable,
                "-m",
                "app.jobs.strategy_runner",
                "nav",
                "--from",
                WINDOW_FROM,
                "--to",
                WINDOW_TO,
                "--config-json",
                config_json(model_version),
            ],
            capture_output=True,
            text=True,
            env=env,
        )
        print("--- NAV %s (rc=%d) ---" % (label, proc.returncode), flush=True)
        print(proc.stdout[-2500:], flush=True)
        if proc.returncode != 0:
            print(proc.stderr[-1200:], flush=True)
            failures += 1
    print("FAILURES=%d" % failures, flush=True)
    return 0 if failures == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
