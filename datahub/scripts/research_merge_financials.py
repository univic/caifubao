# -*- coding: utf-8 -*-
"""Point-in-time financial/quality merge into a frozen factor-lab panel.

For each panel row on date D, the merge uses the latest financial indicator
vintage with ``ann_date <= D`` (and the greatest ``end_date`` on ties). A report
announced after D is never visible; when no report exists the quality fields
stay null (no forward fill). The current CSRC level-1 industry is attached for
industry-neutral ranking, with its point-in-time limitation documented.
"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

import pandas as pd

PANEL_COLUMNS = (
    "date",
    "stock_code",
    "close",
    "close_hfq",
    "open",
    "open_hfq",
    "high_hfq",
    "low_hfq",
    "volume",
    "trade_amount",
    "previous_close",
    "is_bse",
    "is_st",
    "trade_status",
    "limit_up",
    "limit_down",
    "pe_ttm",
    "pb",
    "ps_ttm",
    "dv_ttm",
    "total_mv",
    "turnover_rate",
    "fwd_h1",
    "blocked_h1",
    "fwd_h5",
    "blocked_h5",
    "fwd_h20",
    "blocked_h20",
    "fwd_h60",
    "blocked_h60",
)

FIELDS = (
    "roe",
    "roe_waa",
    "roa",
    "grossprofit_margin",
    "netprofit_margin",
    "assets_turn",
    "debt_to_assets",
    "netprofit_yoy",
    "or_yoy",
    "tr_yoy",
    "ocf_yoy",
    "q_ocf_to_sales",
)


def merge_financials(
    panel: pd.DataFrame,
    financial: pd.DataFrame,
    industry: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """As-of merge by announcement date; never use a later report."""
    if panel.empty:
        raise ValueError("panel is empty")
    if financial.empty:
        raise ValueError("financial frame is empty")
    if "roe" in panel.columns:
        raise ValueError(
            "panel already carries financial columns; merge the raw panel, not "
            "an already-merged one"
        )
    panel = panel.copy()
    panel["date"] = pd.to_datetime(panel["date"]).dt.normalize()
    financial = financial.copy()
    financial["stock_code"] = financial["code"]
    financial["ann_date"] = pd.to_datetime(financial["ann_date"]).dt.normalize()
    financial["end_date"] = pd.to_datetime(financial["end_date"]).dt.normalize()
    financial = financial.drop_duplicates(
        ["stock_code", "ann_date", "end_date", "report_type"]
    )
    # merge_asof requires the ``on`` key to be globally sorted; the ``by`` key
    # only partitions that order. Sorting by stock first would interleave dates
    # and is rejected by pandas.
    financial = financial.sort_values(
        ["ann_date", "end_date", "stock_code"], kind="stable"
    )
    # Prefer the consolidated statement (report_type=1) when Tushare returns
    # several report types for the same announcement/period; otherwise keep the
    # last available type deterministically.
    financial["__report_priority"] = (financial["report_type"] == 1).astype(int)
    financial = financial.sort_values(
        ["ann_date", "end_date", "stock_code", "__report_priority", "report_type"],
        kind="stable",
    )
    financial = financial.drop_duplicates(
        ["stock_code", "ann_date", "end_date"], keep="last"
    )
    panel = panel.sort_values(["date", "stock_code"], kind="stable")
    merged = pd.merge_asof(
        panel,
        financial[
            ["stock_code", "ann_date", "end_date"]
            + [field for field in FIELDS if field in financial.columns]
        ],
        left_on="date",
        right_on="ann_date",
        by="stock_code",
        direction="backward",
        allow_exact_matches=True,
    )
    merged = merged.rename(
        columns={"ann_date": "financial_ann_date", "end_date": "financial_end_date"}
    )
    if industry is not None and not industry.empty:
        industry = industry.copy()
        industry["industry"] = industry["industry"].replace("", pd.NA).fillna("UNKNOWN")
        merged = merged.merge(
            industry[["code", "industry"]],
            left_on="stock_code",
            right_on="code",
            how="left",
        ).drop(columns=["code"])
        merged["industry"] = merged["industry"].fillna("UNKNOWN")
    else:
        merged["industry"] = "UNKNOWN"
    if len(merged) != len(panel):
        raise ValueError("financial merge changed the panel row count")
    return merged.sort_values(["date", "stock_code"], kind="stable").reset_index(
        drop=True
    )


def load_financials() -> pd.DataFrame:
    from pymongo import MongoClient

    client = MongoClient(
        host=os.environ["MONGODB_HOST"],
        port=int(os.environ["MONGODB_PORT"]),
        username=os.environ.get("MONGODB_USER"),
        password=os.environ.get("MONGODB_PASS"),
        authSource="admin",
    )
    db = client[os.environ["MONGODB_NAME"]]
    docs = list(
        db.stock_financial_indicator.find(
            {},
            {
                "_id": 0,
                "code": 1,
                "ann_date": 1,
                "end_date": 1,
                "report_type": 1,
                **{field: 1 for field in FIELDS},
            },
        )
    )
    if not docs:
        raise ValueError("stock_financial_indicator is empty")
    return pd.DataFrame(docs)


def load_industry() -> pd.DataFrame:
    from pymongo import MongoClient

    client = MongoClient(
        host=os.environ["MONGODB_HOST"],
        port=int(os.environ["MONGODB_PORT"]),
        username=os.environ.get("MONGODB_USER"),
        password=os.environ.get("MONGODB_PASS"),
        authSource="admin",
    )
    db = client[os.environ["MONGODB_NAME"]]
    docs = list(
        db.stock_industry.find(
            {}, {"_id": 0, "stock_code": 1, "industry_code_sw_l1": 1}
        )
    )
    return pd.DataFrame(
        [
            {"code": row["stock_code"], "industry": row.get("industry_code_sw_l1")}
            for row in docs
        ]
    )


def merge_to_parquet(
    panel_path: str,
    output_path: str,
    financial: pd.DataFrame,
    industry: pd.DataFrame,
    *,
    sessions_per_chunk: int = 250,
) -> dict:
    """Chunk the as-of merge so the 7.7 M-row panel never needs a full copy.

    The output keeps only the columns the replay actually reads, which keeps the
    peak well below the 16 Gi the full-frame merge needed.
    """
    import numpy as np
    import pyarrow as pa
    import pyarrow.parquet as pq

    available = set(pq.ParquetFile(panel_path).schema.names)
    columns = [name for name in PANEL_COLUMNS if name in available]
    panel = pd.read_parquet(panel_path, columns=columns)
    panel["date"] = pd.to_datetime(panel["date"]).dt.normalize()
    panel = panel.sort_values(["date", "stock_code"], kind="stable").reset_index(
        drop=True
    )
    dates = np.sort(panel["date"].drop_duplicates().to_numpy())
    writer = None
    merged_rows = 0
    rows_with_report = 0
    known_industry = 0
    temporary = output_path + ".tmp"
    try:
        for start in range(0, len(dates), sessions_per_chunk):
            window = dates[start : start + sessions_per_chunk]
            left = int(np.searchsorted(panel["date"].to_numpy(), window[0], "left"))
            right = int(np.searchsorted(panel["date"].to_numpy(), window[-1], "right"))
            chunk = panel.iloc[left:right]
            merged = merge_financials(chunk, financial, industry)
            merged_rows += len(merged)
            rows_with_report += int(merged["financial_ann_date"].notna().sum())
            known_industry += int((merged["industry"] != "UNKNOWN").sum())
            table = pa.Table.from_pandas(merged, preserve_index=False)
            if writer is None:
                writer = pq.ParquetWriter(temporary, table.schema)
            writer.write_table(table)
    except Exception:
        if writer is not None:
            writer.close()
            writer = None
        Path(temporary).unlink(missing_ok=True)
        raise
    finally:
        if writer is not None:
            writer.close()
    os.replace(temporary, output_path)
    return {
        "panel_rows": int(len(panel)),
        "merged_rows": merged_rows,
        "rows_with_report": rows_with_report,
        "industry_coverage": round(known_industry / merged_rows, 4)
        if merged_rows
        else 0.0,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--panel", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args(argv)
    financial = load_financials()
    industry = load_industry()
    result = merge_to_parquet(args.panel, args.output, financial, industry)
    summary = {
        **result,
        "financial_rows": int(len(financial)),
        "coverage": round(result["rows_with_report"] / result["merged_rows"], 4)
        if result["merged_rows"]
        else 0.0,
        "financial_max_ann_date": str(financial["ann_date"].max()),
        "output": args.output,
    }
    print(json.dumps(summary, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
