# -*- coding: utf-8 -*-
"""Fetch daily OHLCV for 000977 (浪潮信息) and market indices from Eastmoney.

Caches one parquet per target under ``data/`` with a ``fetched_at`` column so a
research artefact can state exactly which snapshot it used.  Research tooling
only: it never writes Mongo and never places an order.

Adjustment choice: ``fqt=1`` (前复权).  ``fqt=2`` (后复权) is also downloaded but
only as audit evidence: for this stock the vendor's 后复权 series violates the
±10% daily limit on 51 days (e.g. -26.84% on 2006-03-07), while 前复权 has zero
violations and matches Tencent's independently computed series.  See
``REPORT.md`` §2.0.  Because 前复权 is anchored to the latest price, a snapshot
must be kept together with its ``fetched_at`` manifest for reproducibility.
"""

from __future__ import annotations

import argparse
import json
import time
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import requests

DATA_DIR = Path(__file__).resolve().parent / "data"
UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
)
KLINES_URL = "https://push2his.eastmoney.com/api/qt/stock/kline/get"
FIELDS2 = "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61"
COLUMNS = [
    "date",
    "open",
    "close",
    "high",
    "low",
    "volume",
    "amount",
    "amplitude",
    "pct_chg",
    "change",
    "turnover_rate",
]
# (secid, cache name, fqt, role)
TARGETS = [
    ("0.000977", "000977_qfq", 1, "primary"),
    ("0.000977", "000977_raw", 0, "audit"),
    ("0.000977", "000977_hfq", 2, "audit-broken"),
    ("1.000300", "csi300", 1, "market regime"),
    ("1.000001", "sse", 1, "reference"),
    ("0.399006", "chinext", 1, "reference"),
    ("1.000905", "csi500", 1, "reference"),
]


def fetch_kline(secid: str, fqt: int, beg: str = "20050101") -> pd.DataFrame:
    params = {
        "secid": secid,
        "fields1": "f1,f2,f3,f4,f5,f6",
        "fields2": FIELDS2,
        "klt": "101",
        "fqt": str(fqt),
        "beg": beg,
        "end": "20500101",
    }
    resp = requests.get(
        KLINES_URL, params=params, headers={"User-Agent": UA}, timeout=20
    )
    resp.raise_for_status()
    payload = resp.json()
    data = payload.get("data") or {}
    klines = data.get("klines") or []
    if not klines:
        raise RuntimeError(f"empty kline payload for {secid} fqt={fqt}")
    rows = [line.split(",") for line in klines]
    frame = pd.DataFrame(rows, columns=COLUMNS)
    for col in COLUMNS[1:]:
        frame[col] = pd.to_numeric(frame[col], errors="coerce")
    frame["date"] = pd.to_datetime(frame["date"])
    frame = frame.sort_values("date").drop_duplicates("date").reset_index(drop=True)
    frame["fetched_at"] = datetime.now(timezone.utc).isoformat(timespec="seconds")
    return frame


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--refresh", action="store_true")
    args = parser.parse_args()
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    manifest: dict[str, dict] = {}
    for secid, name, fqt, role in TARGETS:
        path = DATA_DIR / f"{name}.parquet"
        if path.exists() and not args.refresh:
            frame = pd.read_parquet(path)
        else:
            frame = fetch_kline(secid, fqt)
            frame.to_parquet(path, index=False)
            time.sleep(0.6)
        manifest[name] = {
            "secid": secid,
            "fqt": fqt,
            "role": role,
            "rows": int(len(frame)),
            "start": str(frame["date"].min().date()),
            "end": str(frame["date"].max().date()),
            "fetched_at": str(frame["fetched_at"].iloc[-1]),
            "path": str(path.relative_to(Path(__file__).resolve().parent)),
        }
        print(
            f"{name:14s} fqt={fqt} rows={len(frame):5d} {manifest[name]['start']} -> {manifest[name]['end']}"
        )
    (DATA_DIR / "manifest.json").write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
