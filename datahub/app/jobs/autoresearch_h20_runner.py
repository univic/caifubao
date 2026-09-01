"""Research-only H20 autoresearch compatibility CLI."""

from __future__ import annotations

import argparse
import json
import os
import sys
import tempfile
from pathlib import Path

import pandas as pd

from app.lib.autoresearch.h20_excess_alpha import (
    append_run_artifacts,
    evaluate_candidate,
    file_sha256,
    load_json_yaml,
    metric_from_report,
    validate_snapshot,
)


def _write_json_atomic(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        "w", dir=path.parent, delete=False, encoding="utf-8"
    ) as handle:
        json.dump(payload, handle, indent=2, sort_keys=True)
        temp_name = handle.name
    os.replace(temp_name, path)


def prepare(profile_path: str, source_parquet: str | None = None) -> dict:
    profile = load_json_yaml(profile_path)
    snapshot = Path(profile["artifacts"]["snapshot_path"])
    manifest_path = Path(profile["artifacts"]["snapshot_manifest_path"])
    if source_parquet:
        frame = pd.read_parquet(source_parquet)
    elif snapshot.exists():
        frame = pd.read_parquet(snapshot)
    else:
        raise RuntimeError(
            "immutable snapshot is absent; provide --source-parquet or configure "
            "the approved dev snapshot export before bootstrap"
        )
    summary = validate_snapshot(frame, profile)
    snapshot.parent.mkdir(parents=True, exist_ok=True)
    if not snapshot.exists() or source_parquet:
        with tempfile.NamedTemporaryFile(
            dir=snapshot.parent, suffix=".parquet", delete=False
        ) as handle:
            temp_name = handle.name
        try:
            frame.to_parquet(temp_name, index=False)
            os.replace(temp_name, snapshot)
        finally:
            if os.path.exists(temp_name):
                os.unlink(temp_name)
    dates = pd.to_datetime(frame["date"], utc=True).dt.strftime("%Y-%m-%d")
    manifest = {
        "schema_version": "h20-excess-alpha-v1",
        "snapshot_path": str(snapshot.resolve()),
        "snapshot_sha256": file_sha256(snapshot),
        **summary,
        "counts_by_date": dates.value_counts().sort_index().to_dict(),
        "eligible_counts_by_date": (
            frame.loc[frame["eligibility"].astype(bool), "date"]
            .pipe(pd.to_datetime, utc=True)
            .dt.strftime("%Y-%m-%d")
            .value_counts()
            .sort_index()
            .to_dict()
        ),
        "missingness_by_column": {
            column: int(frame[column].isna().sum()) for column in frame.columns
        },
        "columns": list(frame.columns),
        "source_model_version": sorted(
            str(value)
            for value in frame.get("source_model_version", pd.Series())
            .dropna()
            .unique()
        ),
        "factor_version": sorted(
            str(value)
            for value in frame.get("factor_version", pd.Series()).dropna().unique()
        ),
        "signal_version": sorted(
            str(value)
            for value in frame.get("signal_version", pd.Series()).dropna().unique()
        ),
        "generation_command": " ".join(sys.argv),
    }
    _write_json_atomic(manifest_path, manifest)
    return manifest


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command", required=True)
    prepare_parser = subparsers.add_parser("prepare")
    prepare_parser.add_argument("--profile", required=True)
    prepare_parser.add_argument("--source-parquet")
    run_parser = subparsers.add_parser("run")
    run_parser.add_argument("--profile", required=True)
    run_parser.add_argument("--candidate", required=True)
    run_parser.add_argument(
        "--split", choices=("train", "validation", "test"), default="validation"
    )
    run_parser.add_argument("--allow-test", action="store_true")
    metric_parser = subparsers.add_parser("metric")
    metric_parser.add_argument("--report", required=True)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.command == "prepare":
        prepare(args.profile, args.source_parquet)
        return 0
    if args.command == "run":
        profile = load_json_yaml(args.profile)
        report = evaluate_candidate(
            profile["artifacts"]["snapshot_path"],
            args.candidate,
            args.profile,
            split=args.split,
            allow_test=args.allow_test,
        )
        append_run_artifacts(report, profile)
        print(f"research_profitability_score: {report['research_profitability_score']}")
        return 0
    value = metric_from_report(args.report)
    print(f"{value:g}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
