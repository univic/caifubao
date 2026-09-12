# -*- coding: utf-8 -*-
"""Point-in-time re-derivation drift check (research).

Re-derives h20 scores for dates where a genuine same-day production run already
exists under `score_v2_202605b`, using a scratch model_version registered with an
*empty* h20 override (i.e. byte-identical to the built-in production config), then
diffs the two by (stock_code, date).

What it measures:

* 2026-08-26 and 2026-08-27 were scored by production *before* the 2026-08-27
  quote-stock run that rewrote 9,619,954 historical quote/factor rows (the FQ
  fix). Re-deriving them today therefore measures how much the FQ rewrite moved
  the model's own decisions.
* 2026-08-28 and 2026-09-11 were scored after that rewrite, so they bound the
  drift from everything else (ordinary daily data revisions, signal ordering).

If the drift on the post-rewrite dates is ~0, re-derivation is a faithful
substitute for a decision that production never got to make (the pipeline only
scored 12 of the 30 window sessions).
"""

import datetime
import os
import subprocess
import sys


def _assert_research_db() -> None:
    """These scripts WRITE predictions: refuse non-research targets."""
    name = os.getenv("MONGODB_NAME", "").strip()
    if name != "caifubao-research" and os.getenv("PIT_SIM_ALLOW_DB") != name:
        raise SystemExit(
            f"refusing to run: MONGODB_NAME={name!r} is not the research database "
            "(set PIT_SIM_ALLOW_DB to that exact name to override)"
        )


SCRATCH_VERSION = "pit_baseline_h20_v1"
PRODUCTION_VERSION = "score_v2_202605b"
DATES = ["2026-08-26", "2026-08-27", "2026-08-28", "2026-09-11"]
HORIZON = 20


def main() -> int:
    from app.lib.db_watcher.mongoengine_tool import mongo_watcher

    _assert_research_db()
    # This audit compares against the built-in production configuration, which is
    # only what the *raw* path scores; pin it instead of inheriting ambient env.
    os.environ["DATAHUB_SCORING_MODE"] = "raw"
    mongo_watcher.get_db_connection()
    from app.model.scoring import StockScorePrediction

    print("=" * 78)
    print("Re-deriving %s h%d for %s" % (SCRATCH_VERSION, HORIZON, DATES))
    print("=" * 78, flush=True)
    for date in DATES:
        proc = subprocess.run(
            [
                sys.executable,
                "-m",
                "app.jobs.scoring_runner",
                "backfill",
                "--from",
                date,
                "--to",
                date,
                "--horizon",
                str(HORIZON),
                "--model-version",
                SCRATCH_VERSION,
                "--replace",
            ],
            capture_output=True,
            text=True,
        )
        print(
            "  scored %s rc=%d" % (date, proc.returncode),
            flush=True,
        )

    print()
    print(
        "%-12s %7s %7s %7s %8s %8s %8s %8s"
        % ("date", "prod", "rederiv", "matched", "d_score", "d_rec", "d_rank", "d_pct")
    )
    for date in DATES:
        day = datetime.datetime.strptime(date, "%Y-%m-%d")
        prod = {
            p.stock_code: p
            for p in StockScorePrediction.objects(
                model_version=PRODUCTION_VERSION, date=day, horizon=HORIZON
            ).only("stock_code", "score", "rank", "percentile", "recommendation")
        }
        red = {
            p.stock_code: p
            for p in StockScorePrediction.objects(
                model_version=SCRATCH_VERSION, date=day, horizon=HORIZON
            ).only("stock_code", "score", "rank", "percentile", "recommendation")
        }
        common = sorted(set(prod) & set(red))
        d_score = sum(
            1
            for c in common
            if abs((prod[c].score or 0.0) - (red[c].score or 0.0)) > 0.005
        )
        d_rec = sum(
            1 for c in common if prod[c].recommendation != red[c].recommendation
        )
        d_rank = sum(1 for c in common if prod[c].rank != red[c].rank)
        d_pct = sum(
            1
            for c in common
            if abs((prod[c].percentile or 0.0) - (red[c].percentile or 0.0)) > 1e-6
        )
        print(
            "%-12s %7d %7d %7d %8d %8d %8d %8d"
            % (date, len(prod), len(red), len(common), d_score, d_rec, d_rank, d_pct),
            flush=True,
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
