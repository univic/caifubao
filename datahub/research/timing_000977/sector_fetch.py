# -*- coding: utf-8 -*-
"""Fetch the 10 CSI sector indices and the industry label for each stock.

Sector indices: 000928..000937 (中证能源/材料/工业/可选/主要消费/医药/金融/
信息/电信/公用), Eastmoney kline, 后复权 (indices have no dividends).
Industry labels: Eastmoney clist field f100 for the cross-section universe.
"""

from __future__ import annotations

import json
import time
from pathlib import Path

import pandas as pd
import requests

HERE = Path(__file__).resolve().parent
DATA = HERE / "data"
SEC = DATA / "sectors"
SEC.mkdir(parents=True, exist_ok=True)
UA = {"User-Agent": "Mozilla/5.0"}
SECTORS = {
    "000928": "能源",
    "000929": "材料",
    "000930": "工业",
    "000931": "可选消费",
    "000932": "主要消费",
    "000933": "医药",
    "000934": "金融",
    "000935": "信息",
    "000936": "电信",
    "000937": "公用",
}


def fetch_index(code: str) -> pd.DataFrame:
    url = (
        "https://push2his.eastmoney.com/api/qt/stock/kline/get"
        f"?secid=1.{code}&fields1=f1,f2&fields2=f51,f52,f53,f54,f55,f56"
        "&klt=101&fqt=2&beg=20050101&end=20500101"
    )
    for attempt in range(4):
        try:
            payload = requests.get(url, headers=UA, timeout=20).json()
            klines = (payload.get("data") or {}).get("klines") or []
            if klines:
                break
        except Exception:  # noqa: BLE001
            klines = []
        time.sleep(1.5 + attempt)
    if not klines:
        raise RuntimeError(f"no data for {code}")
    df = pd.DataFrame(
        [r.split(",") for r in klines],
        columns=["date", "open", "close", "high", "low", "volume"],
    )
    df["date"] = pd.to_datetime(df["date"])
    for c in ("open", "close", "high", "low", "volume"):
        df[c] = pd.to_numeric(df[c], errors="coerce")
    return df.sort_values("date").reset_index(drop=True)


def main() -> int:
    for code, name in SECTORS.items():
        path = SEC / f"{code}.parquet"
        if path.exists():
            print(f"{code} {name}: cached")
            continue
        df = fetch_index(code)
        df.to_parquet(path, index=False)
        print(
            f"{code} {name}: {len(df)} rows {df.date.min().date()}..{df.date.max().date()}"
        )
        time.sleep(0.6)

    # industry labels for the cross-section universe
    url = (
        "https://push2.eastmoney.com/api/qt/clist/get"
        "?pn=1&pz=6000&po=1&np=1&fltt=2&invt=2&fid=f3"
        "&fs=m:0+t:6,m:0+t:80,m:1+t:2,m:1+t:23&fields=f12,f14,f100"
    )
    for attempt in range(4):
        try:
            diff = requests.get(url, headers=UA, timeout=30).json()["data"]["diff"]
            break
        except Exception:  # noqa: BLE001
            diff = []
            time.sleep(2 + attempt)
    industry = pd.DataFrame(diff).rename(
        columns={"f12": "code", "f14": "name", "f100": "industry"}
    )
    industry["code"] = industry["code"].astype(str).str.zfill(6)
    industry.to_csv(DATA / "universe" / "industry_map.csv", index=False)
    codes = [p.stem for p in (DATA / "universe").glob("*.parquet")]
    sub = industry[industry.code.isin(codes)]
    print(f"industry map rows={len(industry)} matched={len(sub)}/{len(codes)}")
    (DATA / "sectors" / "sector_names.json").write_text(
        json.dumps(SECTORS, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
