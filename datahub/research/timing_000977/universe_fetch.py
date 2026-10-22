# -*- coding: utf-8 -*-
"""Fetch a cross-sectional A-share sample for the rule-generalisation study.

Data source: Tencent hfq daily bars (total-return series; Tencent qfq can go
negative for high-dividend stocks, so it is unusable here) (the vendor whose series passed the
limit-move check on 000977).  The API returns at most 640 rows per call, so the
fetch walks backwards from the end date.

The universe is a deterministic random sample of currently listed A-shares
(BSE excluded, ST names excluded).  It is survivorship-biased by construction
and the report says so; the point of the sample is a controlled *paired* test of
each rule against a matched constant-exposure benchmark, not a market-wide
return estimate.
"""

from __future__ import annotations

import argparse
import time
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd
import requests

HERE = Path(__file__).resolve().parent
DATA = HERE / "data"
UNI = DATA / "universe"
UNI.mkdir(parents=True, exist_ok=True)
UA = {"User-Agent": "Mozilla/5.0"}
START = "2014-01-01"
END = "2026-10-21"


def list_universe() -> pd.DataFrame:
    """Current A-share list from Sina (Eastmoney's list endpoint is rate-limited)."""
    rows = []
    for page in range(1, 80):
        url = (
            "https://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php/"
            f"Market_Center.getHQNodeData?page={page}&num=100&sort=symbol&asc=1&node=hs_a"
        )
        try:
            block = requests.get(url, headers=UA, timeout=20).json()
        except Exception:  # noqa: BLE001
            break
        if not block:
            break
        rows += block
        time.sleep(0.1)
    df = pd.DataFrame(rows)
    df["code"] = df["code"].astype(str).str.zfill(6)
    df = df[~df["symbol"].str.startswith("bj")]
    df = df[~df["name"].str.contains("ST", na=False)]
    df = df[~df["code"].str.startswith(("4", "8", "9"))]
    return df[["code", "name"]].drop_duplicates("code").reset_index(drop=True)


def symbol(code: str) -> str:
    return ("sh" if code.startswith(("6", "9")) else "sz") + code


def fetch_one(code: str) -> pd.DataFrame:
    sym = symbol(code)
    rows: list[list] = []
    end = END
    for _ in range(12):
        url = (
            "https://web.ifzq.gtimg.cn/appstock/app/fqkline/get"
            f"?param={sym},day,{START},{end},640,hfq"
        )
        payload = None
        for attempt in range(3):
            try:
                payload = requests.get(url, headers=UA, timeout=20).json()
                break
            except Exception:  # noqa: BLE001
                time.sleep(1.0 + attempt)
        if payload is None:
            break
        block = payload.get("data", {}).get(sym, {})
        key = "hfqday" if "hfqday" in block else "day"
        arr = block.get(key) or []
        if not arr:
            break
        arr = [r[:6] for r in arr]
        rows = arr + rows
        first = arr[0][0]
        if first <= START:
            break
        end = (datetime.fromisoformat(first) - pd.Timedelta(days=1)).date().isoformat()
        time.sleep(0.15)
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(
        rows, columns=["date", "open", "close", "high", "low", "volume"]
    ).drop_duplicates("date")
    df["date"] = pd.to_datetime(df["date"])
    for c in ("open", "close", "high", "low", "volume"):
        df[c] = pd.to_numeric(df[c], errors="coerce")
    return df.sort_values("date").reset_index(drop=True)


def quality(df: pd.DataFrame, code: str) -> dict:
    if df.empty:
        return {"rows": 0, "violations": 999, "ok": False}
    limit = 0.21 if code.startswith(("30", "68")) else 0.11
    rets = df["close"].pct_change().dropna()
    violations = int((rets.abs() > limit).sum())
    return {
        "rows": int(len(df)),
        "start": str(df.date.min().date()),
        "end": str(df.date.max().date()),
        "violations": violations,
        "ok": bool(len(df) > 1200 and violations <= 5),
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--n", type=int, default=200)
    parser.add_argument("--seed", type=int, default=20261021)
    parser.add_argument("--refresh", action="store_true")
    args = parser.parse_args()

    meta_path = UNI / "universe.csv"
    if meta_path.exists() and not args.refresh:
        meta = pd.read_csv(meta_path, dtype={"code": str})
    else:
        pool = list_universe()
        rng = np.random.default_rng(args.seed)
        pick = rng.choice(len(pool), size=min(args.n * 2, len(pool)), replace=False)
        meta = pool.iloc[sorted(pick)].reset_index(drop=True)
        meta.to_csv(meta_path, index=False)
    rows = []
    for i, row in meta.iterrows():
        if i >= args.n * 3:
            break
        path = UNI / f"{row.code}.parquet"
        if path.exists() and not args.refresh:
            df = pd.read_parquet(path)
        else:
            try:
                df = fetch_one(row.code)
                if not df.empty:
                    df.to_parquet(path, index=False)
            except Exception as exc:  # noqa: BLE001
                print(f"{row.code} failed: {type(exc).__name__}")
                continue
            time.sleep(0.2)
        q = quality(df, row.code)
        rows.append({"code": row.code, "name": row.name, **q})
        print(
            f"{len(rows):3d} {row.code} {str(row.name):8s} rows={q.get('rows', 0)} viol={q.get('violations', '-')} ok={q.get('ok', '-')}"
        )
        if sum(1 for r in rows if r.get("ok")) >= args.n:
            break
    summary = pd.DataFrame(rows)
    summary.to_csv(UNI / "universe_quality.csv", index=False)
    print(f"\nusable: {int(summary.ok.sum())} / {len(summary)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
