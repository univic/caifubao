from __future__ import annotations

import argparse
import datetime as dt
import json
import logging
import os
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

DATASET_ALL = "all"


@dataclass(frozen=True)
class DatasetConfig:
    name: str
    collection: str
    path: str
    date_field: str
    exclude_fields: tuple[str, ...] = ("_id", "stock", "_cls")


DATASETS: dict[str, DatasetConfig] = {
    "daily_quotes": DatasetConfig(
        name="daily_quotes",
        collection="stock_daily_quote",
        path="china-a/daily_quotes",
        date_field="date",
    ),
    "factors": DatasetConfig(
        name="factors",
        collection="stock_factor_daily",
        path="china-a/factors",
        date_field="date",
    ),
    "signals": DatasetConfig(
        name="signals",
        collection="stock_signal_daily",
        path="china-a/signals",
        date_field="date",
    ),
}


def _init_db_connection():
    from mongoengine.connection import get_db

    from app.lib.db_watcher.mongoengine_tool import mongo_watcher

    mongo_watcher.get_db_connection()
    return get_db()


def _parse_date(value: str | None) -> dt.datetime | None:
    if not value:
        return None
    parsed = dt.datetime.strptime(value, "%Y-%m-%d")
    return parsed.replace(hour=0, minute=0, second=0, microsecond=0)


def _date_from_lookback(days: int | None, today: dt.date | None = None) -> dt.datetime | None:
    if days is None:
        return None
    base = today or dt.datetime.now(dt.timezone.utc).date()
    return dt.datetime.combine(base - dt.timedelta(days=days), dt.time.min)


def _build_query(
    date_field: str,
    from_date: dt.datetime | None,
    to_date: dt.datetime | None,
) -> dict[str, Any]:
    date_filter: dict[str, Any] = {}
    if from_date:
        date_filter["$gte"] = from_date
    if to_date:
        date_filter["$lte"] = to_date
    if not date_filter:
        return {}
    return {date_field: date_filter}


def _normalize_value(value: Any) -> Any:
    if isinstance(value, dt.datetime):
        return value.replace(tzinfo=None)
    if isinstance(value, dt.date):
        return value.isoformat()
    if isinstance(value, dict):
        return {key: _normalize_value(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_normalize_value(item) for item in value]
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    return str(value)


def _normalize_doc(doc: dict[str, Any], config: DatasetConfig) -> dict[str, Any]:
    normalized = {
        key: _normalize_value(value)
        for key, value in doc.items()
        if key not in config.exclude_fields
    }
    date_value = doc.get(config.date_field)
    if isinstance(date_value, dt.datetime):
        normalized["trade_date"] = date_value.strftime("%Y-%m-%d")
    elif isinstance(date_value, dt.date):
        normalized["trade_date"] = date_value.isoformat()
    elif date_value:
        normalized["trade_date"] = str(date_value)[:10]
    else:
        normalized["trade_date"] = "unknown"
    return normalized


def _object_key(prefix: str, dataset_path: str, trade_date: str) -> str:
    safe_prefix = prefix.strip("/")
    return (
        f"{safe_prefix}/{dataset_path}/trade_date={trade_date}/part-{trade_date}.parquet"
    )


def _write_parquet(rows: list[dict[str, Any]], destination: Path) -> None:
    import pandas as pd

    destination.parent.mkdir(parents=True, exist_ok=True)
    frame = pd.DataFrame(rows)
    frame.to_parquet(destination, index=False, engine="pyarrow")


def _upload_file(
    source: Path,
    *,
    bucket: str,
    object_key: str,
    endpoint_url: str | None,
    region_name: str | None,
) -> None:
    import boto3
    from botocore.config import Config

    client = boto3.client(
        "s3",
        endpoint_url=endpoint_url or None,
        region_name=region_name or None,
        config=Config(s3={"addressing_style": "virtual"}),
    )
    client.upload_file(str(source), bucket, object_key)


def _load_rows(
    collection,
    config: DatasetConfig,
    query: dict[str, Any],
    limit: int | None,
) -> list[dict[str, Any]]:
    projection = {field: False for field in config.exclude_fields}
    cursor = collection.find(query, projection=projection).sort(config.date_field, 1)
    if limit:
        cursor = cursor.limit(limit)
    return [_normalize_doc(doc, config) for doc in cursor]


def export_dataset(
    db,
    config: DatasetConfig,
    *,
    bucket: str,
    prefix: str,
    endpoint_url: str | None,
    region_name: str | None,
    from_date: dt.datetime | None,
    to_date: dt.datetime | None,
    limit: int | None,
    dry_run: bool,
) -> dict[str, Any]:
    query = _build_query(config.date_field, from_date, to_date)
    rows = _load_rows(db[config.collection], config, query, limit)
    partitions: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        partitions.setdefault(row["trade_date"], []).append(row)

    result = {
        "dataset": config.name,
        "collection": config.collection,
        "query": query,
        "row_count": len(rows),
        "partition_count": len(partitions),
        "uploaded": [],
        "dry_run": dry_run,
    }
    if dry_run or not rows:
        return result

    with tempfile.TemporaryDirectory(prefix="caifubao-parquet-") as tmp:
        tmp_dir = Path(tmp)
        for trade_date, partition_rows in sorted(partitions.items()):
            object_key = _object_key(prefix, config.path, trade_date)
            parquet_path = tmp_dir / config.name / f"{trade_date}.parquet"
            _write_parquet(partition_rows, parquet_path)
            _upload_file(
                parquet_path,
                bucket=bucket,
                object_key=object_key,
                endpoint_url=endpoint_url,
                region_name=region_name,
            )
            result["uploaded"].append(
                {
                    "object_key": object_key,
                    "row_count": len(partition_rows),
                }
            )
    return result


def run_export(args: argparse.Namespace) -> dict[str, Any]:
    db = _init_db_connection()
    dataset_names = list(DATASETS) if args.dataset == DATASET_ALL else [args.dataset]
    bucket = args.bucket or os.getenv("DATA_LAKE_BUCKET") or os.getenv("S3_BUCKET")
    if not bucket:
        raise ValueError("DATA_LAKE_BUCKET or S3_BUCKET is required")

    from_date = _parse_date(args.from_date)
    if from_date is None:
        from_date = _date_from_lookback(args.lookback_days)
    to_date = _parse_date(args.to_date)
    prefix = args.prefix or os.getenv("DATA_LAKE_PREFIX", "data-lake")
    endpoint_url = args.endpoint_url or os.getenv("DATA_LAKE_ENDPOINT_URL") or os.getenv(
        "AWS_ENDPOINT_URL"
    )
    region_name = args.region or os.getenv("DATA_LAKE_REGION") or os.getenv(
        "AWS_DEFAULT_REGION"
    )

    results = [
        export_dataset(
            db,
            DATASETS[name],
            bucket=bucket,
            prefix=prefix,
            endpoint_url=endpoint_url,
            region_name=region_name,
            from_date=from_date,
            to_date=to_date,
            limit=args.limit,
            dry_run=args.dry_run,
        )
        for name in dataset_names
    ]
    return {
        "status": "DRY_RUN" if args.dry_run else "SUCCESS",
        "bucket": bucket,
        "prefix": prefix,
        "datasets": results,
    }


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Export MongoDB datasets to Parquet")
    subparsers = parser.add_subparsers(dest="command")
    export_parser = subparsers.add_parser("export")
    export_parser.add_argument(
        "--dataset",
        choices=[DATASET_ALL, *DATASETS.keys()],
        default=DATASET_ALL,
    )
    export_parser.add_argument("--from-date", default=None)
    export_parser.add_argument("--to-date", default=None)
    export_parser.add_argument("--lookback-days", type=int, default=None)
    export_parser.add_argument("--limit", type=int, default=None)
    export_parser.add_argument("--bucket", default=None)
    export_parser.add_argument("--prefix", default=None)
    export_parser.add_argument("--endpoint-url", default=None)
    export_parser.add_argument("--region", default=None)
    export_parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    if args.command == "export":
        result = run_export(args)
        print(json.dumps(result, default=str, ensure_ascii=False, indent=2))
    else:
        raise SystemExit("Command is required. Use: export")


if __name__ == "__main__":
    main()
