"""Read-only paper-trading readiness runner (research environment).

Usage:
    python -m app.jobs.paper_readiness --model-version flip_wide_shadow_v1 --horizon 20

Prints one JSON document and exits 0 when no check FAILED (WARN is reported,
not fatal) or 1 when the chain is not ready. It performs no writes: it neither
opens a forward window nor records a run.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys

logger = logging.getLogger(__name__)


def _init_db_connection() -> None:
    from app.lib.db_watcher.mongoengine_tool import mongo_watcher

    mongo_watcher.get_db_connection()


def _database():
    from mongoengine import get_connection

    from app.conf import app_config as cfg

    return get_connection()[cfg.MONGODB_DB]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Read-only paper-trading readiness assessment"
    )
    parser.add_argument(
        "--model-version",
        default=None,
        help="Score model version to assess (default: any)",
    )
    parser.add_argument(
        "--horizon",
        type=int,
        default=None,
        choices=[5, 20, 60],
        help="Forward-window horizon to assess (default: any)",
    )
    parser.add_argument(
        "--halt-file",
        default=None,
        help="Halt-switch file (default: CAIFUBAO_STRATEGY_HALT_FILE)",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO)
    args = build_parser().parse_args(argv)
    _init_db_connection()
    from app.lib import paper_readiness

    inputs = paper_readiness.load_inputs(
        _database(),
        model_version=args.model_version,
        horizon=args.horizon,
        halt_path=args.halt_file,
    )
    report = paper_readiness.evaluate(inputs)
    print(json.dumps(report, default=str, ensure_ascii=False))
    return 1 if report["overall"] == "NOT_READY" else 0


if __name__ == "__main__":
    sys.exit(main())
