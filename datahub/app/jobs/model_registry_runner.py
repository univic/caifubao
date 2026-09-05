"""Immutable scoring model-version registry management.

Registers a named model version with its per-horizon scoring override so that
any scoring run naming the version reproduces the exact same configuration
(weights / thresholds / component directions). Registration pins a config_hash
and versions are immutable: change = register a new version name, then retire
the old one. Unregistered versions fall back to the built-in SCORING_CONFIG
(backward compatible) — DEFAULT_MODEL_VERSION never needs registration.

Usage:
    python -m app.jobs.model_registry_runner register \
        --model-version flip_wide_v1 --description "..." \
        --config-json '{"20": {"directions": {"signal_strength": -1, ...}}}'
    python -m app.jobs.model_registry_runner list
    python -m app.jobs.model_registry_runner retire --model-version flip_wide_v1
"""

from __future__ import annotations

import argparse
import json
import logging

from app.lib.db_watcher.mongoengine_tool import mongo_watcher
from app.lib.scoring_engine.config import (
    SCORING_CONFIG,
    get_effective_horizon_config,
    model_config_hash,
)

logger = logging.getLogger(__name__)


def _init_db() -> None:
    mongo_watcher.get_db_connection()


def _validate_config(config: dict) -> None:
    """Validate per-horizon override keys/values via the resolution path."""
    if not isinstance(config, dict):
        raise ValueError("config must be a dict keyed by horizon")
    known_horizons = {str(h) for h in SCORING_CONFIG}
    unknown = {str(h) for h in config} - known_horizons
    if unknown:
        raise ValueError(
            f"config horizon keys must be in {sorted(known_horizons)}; "
            f"got {sorted(unknown)}"
        )
    for raw_horizon, override in config.items():
        if not isinstance(override, dict):
            raise ValueError(f"horizon {raw_horizon} override must be a dict")
        horizon = int(raw_horizon)
        known_weights = set(SCORING_CONFIG[horizon]["weights"])
        # Explicit weights override must be a dict with keys that are REAL
        # scored components. Without this, a typo'd weight key (e.g.
        # "momemtum") would be absorbed into the resolved weights by
        # get_effective_horizon_config and pinned immutably - the override
        # would silently do nothing at scoring time. Directions keys are
        # validated against the same known set by the resolution path.
        weights_override = override.get("weights")
        if weights_override is not None:
            if not isinstance(weights_override, dict):
                raise ValueError(
                    f"horizon {raw_horizon} weights override must be a dict"
                )
            unknown_weights = set(map(str, weights_override)) - known_weights
            if unknown_weights:
                raise ValueError(
                    "weights override keys must be real scored components; "
                    f"got {sorted(unknown_weights)}"
                )
        directions_override = override.get("directions")
        if directions_override is not None:
            if not isinstance(directions_override, dict):
                raise ValueError(
                    f"horizon {raw_horizon} directions override must be a dict"
                )
            unknown_directions = set(map(str, directions_override)) - known_weights
            if unknown_directions:
                raise ValueError(
                    "direction override keys must be real scored components; "
                    f"got {sorted(unknown_directions)}"
                )
        # Resolve to force remaining directions value errors to surface at
        # registration time rather than at scoring time.
        get_effective_horizon_config(horizon, {horizon: override})


def register(
    model_version: str,
    config: dict,
    *,
    description: str = "",
    scoring_mode: str | None = None,
    force: bool = False,
) -> dict:
    from app.model.scoring import ScoreModelVersion

    _validate_config(config)
    existing = ScoreModelVersion.objects(model_version=model_version).first()
    if existing is not None:
        if not force:
            raise ValueError(
                f"model_version {model_version!r} already registered "
                "(immutable: register a new name or pass --force to overwrite)"
            )
        logger.warning("overwriting existing registration %r", model_version)
        existing.delete()
    config_hash = model_config_hash(config)
    doc = ScoreModelVersion(
        model_version=model_version,
        description=description,
        scoring_mode=scoring_mode,
        config=config,
        config_hash=config_hash,
        status="ACTIVE",
    )
    doc.save()
    logger.info(
        "registered model_version=%s config_hash=%s", model_version, config_hash
    )
    return {
        "model_version": model_version,
        "config_hash": config_hash,
        "status": "ACTIVE",
    }


def list_versions() -> list[dict]:
    from app.model.scoring import ScoreModelVersion

    docs = ScoreModelVersion.objects().order_by("-activated_at")
    return [
        {
            "model_version": d.model_version,
            "status": d.status,
            "config_hash": d.config_hash,
            "scoring_mode": d.scoring_mode,
            "description": d.description,
            "activated_at": (d.activated_at.isoformat() if d.activated_at else None),
        }
        for d in docs
    ]


def retire(model_version: str) -> dict:
    import datetime

    from app.model.scoring import ScoreModelVersion

    doc = ScoreModelVersion.objects(
        model_version=model_version, status="ACTIVE"
    ).first()
    if doc is None:
        raise ValueError(f"no ACTIVE registration for {model_version!r}")
    doc.status = "RETIRED"
    doc.retired_at = datetime.datetime.now(datetime.UTC)
    doc.save()
    return {"model_version": model_version, "status": "RETIRED"}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Scoring model-version registry")
    sub = parser.add_subparsers(dest="command", required=True)

    p_reg = sub.add_parser("register")
    p_reg.add_argument("--model-version", required=True)
    p_reg.add_argument("--config-json", required=True)
    p_reg.add_argument("--description", default="")
    p_reg.add_argument("--scoring-mode", choices=["ranked", "raw"], default=None)
    p_reg.add_argument("--force", action="store_true")

    sub.add_parser("list")

    p_ret = sub.add_parser("retire")
    p_ret.add_argument("--model-version", required=True)
    return parser


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO)
    args = build_parser().parse_args(argv)
    _init_db()
    if args.command == "register":
        config = json.loads(args.config_json)
        result = register(
            args.model_version,
            config,
            description=args.description,
            scoring_mode=args.scoring_mode,
            force=args.force,
        )
    elif args.command == "list":
        result = {"versions": list_versions()}
    elif args.command == "retire":
        result = retire(args.model_version)
    else:
        raise SystemExit(f"unknown command: {args.command}")
    print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
