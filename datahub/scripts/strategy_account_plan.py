# -*- coding: utf-8 -*-
"""Read-only CLI for the small-account execution plan (C3).

Usage::

    python datahub/scripts/strategy_account_plan.py --input plan_input.json
    python datahub/scripts/strategy_account_plan.py --input plan_input.json \
        --indent 2 --output plan.json

The input document is JSON (every field except ``aum`` is required)::

    {
      "cash": 50000,
      "positions": {"sh600000": 100},
      "target_weights": {"sh600000": 0.2, "sh688001": 0.1},
      "prices": {
        "sh600000": {"open": 10.0, "trade_status": 1, "previous_close": 10.0},
        "sh688001": {"open": 12.0, "trade_status": 1, "previous_close": 12.0}
      },
      "config": {"score_model_version": "flip_wide_shadow_v1",
                 "cash_reserve_pct": 0.1},
      "as_of": "2026-01-05",
      "aum": 51000
    }

``prices`` rows are QuoteView fields; a row without ``trade_status: 1`` is not
tradable (fail-closed). ``aum`` is an optional cross-check of ``cash + marked
holdings``. ``--cash-reserve-pct`` overrides the config value and is injected
into the hashed config, so a floor change changes ``config_hash`` too.

The plan JSON goes to stdout (or ``--output``). This tool is read-only: no
Mongo, no broker and no order placement — blocked/skipped rows are reported
inside the plan. A failed build (unvaluable holding, negative cash, invalid
config/input) exits 1 with an ``error`` JSON object on stderr.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

INPUT_HELP = """\
input JSON: {cash, positions, target_weights, prices, config, as_of, aum?}
  prices row fields: open, close, trade_status, limit_up, limit_down,
  previous_close, stock_code, is_st. A row without trade_status=1 is not
  tradable; the plan reports the refusal instead of filling it.
"""


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Build a read-only small-account execution plan from a JSON input "
            "(sell-first, board-legal, fee-aware; no Mongo, no broker)"
        ),
        epilog=INPUT_HELP,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--input",
        required=True,
        help="Plan input JSON file (see the input schema below)",
    )
    parser.add_argument(
        "--output",
        default=None,
        help="Write the plan JSON to this path instead of stdout",
    )
    parser.add_argument(
        "--indent",
        type=int,
        default=None,
        help="Pretty-print with this indent (default: canonical compact JSON)",
    )
    parser.add_argument(
        "--cash-reserve-pct",
        type=float,
        default=None,
        help=(
            "Override config.cash_reserve_pct (cash floor fraction of AUM in "
            "[0, 1)); injected into the validated config and its hash"
        ),
    )
    return parser


def _load(path: str) -> dict:
    with open(path, encoding="utf-8") as handle:
        document = json.load(handle)
    if not isinstance(document, dict):
        raise ValueError("plan input must be a JSON object")
    return document


def _build(document: dict, cash_reserve_pct: float | None) -> dict:
    """Build the plan; importing ``app`` is deferred so ``main`` can first keep
    stdout reserved for the JSON document (the package logs and may print a
    dotenv notice on import)."""
    from app.lib.strategy_engine.account_plan import build_plan

    config = document.get("config")
    if not isinstance(config, dict):
        raise ValueError(
            "plan input requires a config object (a validated strategy config; "
            "score_model_version is required)"
        )
    if cash_reserve_pct is not None:
        config = dict(config)
        config["cash_reserve_pct"] = cash_reserve_pct
    if "as_of" not in document:
        raise ValueError("plan input requires as_of (the signal/execution date)")
    return build_plan(
        cash=document.get("cash"),
        positions=document.get("positions") or {},
        target_weights=document.get("target_weights") or {},
        prices=document.get("prices") or {},
        config=config,
        as_of=document.get("as_of"),
        aum=document.get("aum"),
    )


def _error(message: str) -> None:
    print(json.dumps({"error": message}, ensure_ascii=False), file=sys.stderr)


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    # Importing the datahub ``app`` package installs a stdout log handler and
    # may print a dotenv notice. Keep stdout reserved for the plan JSON: redirect
    # it while the app is imported and the plan is built, then restore it for the
    # single payload write.
    real_stdout = sys.stdout
    sys.stdout = sys.stderr
    try:
        from app.lib.strategy_engine.account_plan import plan_json

        try:
            plan = _build(_load(args.input), args.cash_reserve_pct)
        except json.JSONDecodeError as exc:
            _error(f"invalid JSON input: {exc}")
            return 1
        except OSError as exc:
            _error(f"cannot read plan input: {exc}")
            return 1
        except ValueError as exc:
            _error(str(exc))
            return 1
    finally:
        sys.stdout = real_stdout
    text = plan_json(plan, indent=args.indent)
    if args.output:
        Path(args.output).write_text(text + "\n", encoding="utf-8")
    else:
        sys.stdout.write(text + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
