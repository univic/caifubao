# -*- coding: utf-8 -*-
"""ETF research pipeline for a small-account (5-10万) rotation strategy.

Three stages, each independently runnable and read-only against the outside
world except for the parquet files it writes:

``pool``
    Build the **tradable theme pool**. The ETF listing is ~3k "funds" but only
    ~300 distinct benchmarks: many issuers wrap the same index. This stage
    ranks by recent turnover, drops non-equity structures, and keeps the single
    most liquid wrapper per benchmark, so the downstream selection is a
    *theme* ranking over a fixed, auditable universe rather than a search over
    2k near-duplicates.

``panel``
    Freeze an ETF price panel with the factor lab's label semantics: labels are
    positional trading-session offsets from the T+1 open to the open h sessions
    later, and a name whose entry or exit is untradable is dropped with a reason
    (never rolled forward). Prices are HFQ (raw × ``fund_adj``) so distributions
    cannot masquerade as reversal.

``measure``
    Cross-sectional information coefficients and quantile spreads on the panel,
    net of the paper execution model, so a factor's edge on the ETF pool is
    visible before any portfolio construction.

The ETF instrument is deliberately distinct from ``app.lib.factor_lab`` (which
is stock-only): no board-aware limit bands are needed because the panel drops
only on suspension / missing price, and ETF round trips are exempt from stamp
duty, which the CLI passes through as a zero ``sell_stamp_duty_rate``.
"""

from __future__ import annotations

import argparse
import json
import math
import re
import time
from pathlib import Path

import pandas as pd

#: ETF daily price limit is 10 % (cross-border/leveraged products differ, which
#: is one reason they are excluded from the pool by structure).
ETF_LIMIT = 10.0
LIMIT_EPSILON = 0.1

#: Round-trip friction for ETFs: no stamp duty, same commission floor and
#: slippage as the paper model.
ETF_SLIPPAGE_PER_SIDE = 0.001
ETF_COMMISSION_RATE = 0.00025
ETF_MIN_COMMISSION_CNY = 5.0
ETF_SELL_STAMP_DUTY = 0.0

#: Fund types kept in the pool: on-exchange equity products only.
EQUITY_TYPES = {"股票型", "混合型", "指数型"}

_ISSUER = re.compile(
    r"^(易方达|华夏|广发|南方|天弘|国联|华泰柏瑞|嘉实|招商|汇添富|博时|富国|鹏华|工银"
    r"|建信|银华|中欧|平安|国泰|景顺长城|华宝|摩根|西部利得|兴银|中金|海富通|万家|融通"
    r"|长城|国投|中信保诚|浙商|华安|大成|中银|永赢|鑫元|前海|德邦|民生|方正|东财|财通"
    r"|华商|金鹰|诺安|长盛|申万菱信|国海富兰克林|浦银安盛|农银|交银|光大|信达澳亚|国寿"
    r"安保|泰康|人保|太平|新华|中邮|安信|宝盈|红土|创金合信|金信|国融|中航|华润元大|兴证"
    r"全球|睿远|泉果|东吴|东海|恒越|惠升|同泰|百嘉|易米|瑞达|华宸未来|明亚|尚正|京管泰富"
    r"|苏新|兴合|安联|贝莱德|路博迈|施罗德|联博|摩根士丹利|宏利|富达|惠理|中泰|国新|华泰"
    r"证券|招商资管|广发资管|中信建投|中信|东方|财达|国金|华福|华鑫|华创|华西|华龙|华林"
    r"|红塔|江海|开源|联储|麦高|世纪|首创|太平洋|天风|万和|五矿|湘财|信达|兴业|野村|银河"
    r"|银泰|英大|粤开|长江|中德|中山|中天国富|中银国际|中原|众惠|朱雀|中庚|博道|睿郡|慎知"
    r")(?:基金|资管|证券)?"
)
_SUFFIX = re.compile(r"(ETF|交易型开放式指数证券投资基金|指数基金|联接|LOF|基金)$")
_NOISE = re.compile(r"(发起式|增强策略|指数型|指数)$")


def theme_of(name: str) -> str:
    """Best-effort theme label from an ETF name (fallback when benchmark is
    unparsable). Only used for reporting and for de-dup when the benchmark
    string is identical anyway."""
    value = str(name or "")
    value = _ISSUER.sub("", value)
    value = _SUFFIX.sub("", value)
    value = _NOISE.sub("", value)
    return value.strip() or str(name or "")


def benchmark_key(benchmark: str | None, name: str) -> str:
    """Normalise a ``fund_basic.benchmark`` string into a de-dup key.

    The raw value looks like ``创业板算力基础设施指数收益率×100%``; the parts
    that vary between two wrappers of the same index are the issuer prefix in
    the *name* and the ``收益率×100%`` suffix in the benchmark, so both are
    stripped.
    """
    text = str(benchmark or "").strip()
    text = re.sub(r"收益率.*$", "", text)
    text = re.sub(r"[×xX]\s*100\s*%?", "", text)
    text = re.sub(r"(指数|全收益|净收益|价格)$", "", text)
    text = text.strip()
    return text or theme_of(name)


def _amount_column(frame: pd.DataFrame) -> pd.Series:
    """Turnover in 千元 (tushare's unit) coerced to float."""
    return pd.to_numeric(frame["amount"], errors="coerce")


def build_pool(
    basic: pd.DataFrame,
    daily: pd.DataFrame,
    *,
    min_amount_kyuan: float,
    top_per_theme: int = 1,
    exclude_patterns=(
        "货币",
        "债",
        "国债",
        "短融",
        "同业存单",
        "杠杆",
        "反向",
        "两倍",
        "三倍",
    ),
) -> pd.DataFrame:
    """Rank liquid equity ETFs and keep the best wrapper per benchmark.

    ``daily`` is one recent ``fund_daily`` snapshot (all funds, one trade date)
    used only for the liquidity screen; nothing in the pool depends on it
    beyond membership.
    """
    quotes = daily.copy()
    quotes["amount"] = _amount_column(quotes)
    merged = quotes.merge(
        basic[["ts_code", "name", "fund_type", "benchmark", "m_fee", "list_date"]],
        on="ts_code",
        how="left",
    )
    merged = merged.loc[merged["amount"].fillna(0) >= float(min_amount_kyuan)]
    if "fund_type" in merged:
        merged = merged.loc[merged["fund_type"].isin(EQUITY_TYPES)]
    if exclude_patterns:
        pattern = "|".join(map(re.escape, exclude_patterns))
        merged = merged.loc[
            ~merged["name"].fillna("").str.contains(pattern, regex=True)
        ]
    merged = merged.assign(
        theme=merged.apply(
            lambda row: benchmark_key(row.get("benchmark"), row.get("name")), axis=1
        )
    )
    merged = merged.sort_values(["theme", "amount"], ascending=[True, False])
    pool = merged.groupby("theme", sort=False).head(int(top_per_theme)).copy()
    pool = pool.sort_values("amount", ascending=False, ignore_index=True)
    return pool[
        ["ts_code", "name", "theme", "amount", "m_fee", "fund_type", "list_date"]
    ]


def fetch_basic(pro) -> pd.DataFrame:
    return pro.fund_basic(market="E")


def fetch_liquidity(pro, trade_date: str) -> pd.DataFrame:
    return pro.fund_daily(trade_date=trade_date)


def fetch_prices(pro, codes, start_date: str, end_date: str, pause: float = 0.0):
    """HFQ OHLC + turnover for ``codes``; one call per code (tushare has no
    multi-code daily endpoint for funds)."""
    frames = []
    for index, code in enumerate(codes, start=1):
        daily = pro.fund_daily(ts_code=code, start_date=start_date, end_date=end_date)
        if daily is None or daily.empty:
            continue
        adj = pro.fund_adj(ts_code=code, start_date=start_date, end_date=end_date)
        frame = daily.merge(adj, on=["ts_code", "trade_date"], how="left")
        frame["adj_factor"] = (
            pd.to_numeric(frame["adj_factor"], errors="coerce").ffill().bfill()
        )
        frames.append(frame)
        if pause:
            time.sleep(pause)
        if index % 20 == 0:
            print(f"  prices {index}/{len(codes)}", flush=True)
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def build_etf_panel(prices: pd.DataFrame, horizons) -> pd.DataFrame:
    """Panel with the factor lab's session-offset labels.

    Columns mirror ``factor_lab.panel`` where they overlap (raw + HFQ OHLC,
    volume, amount, change_rate, previous_close, trade_status) and add
    ``fwd_h{h}`` / ``blocked_h{h}`` per horizon. Unlike the stock lab there is
    no board-aware limit band: an ETF entry is dropped only when the session is
    missing or the price is unusable, which is the correct rule for a pool whose
    limit bands differ by product type and which the pool stage already filters.
    """
    frame = prices.copy()
    frame["date"] = pd.to_datetime(frame["trade_date"], format="%Y%m%d")
    frame = frame.rename(columns={"ts_code": "stock_code"})
    numeric = (
        "open",
        "high",
        "low",
        "close",
        "pre_close",
        "vol",
        "amount",
        "adj_factor",
        "pct_chg",
    )
    for column in numeric:
        if column in frame:
            frame[column] = pd.to_numeric(frame[column], errors="coerce")
    for column in ("open", "high", "low", "close", "pre_close"):
        frame[f"{column}_hfq"] = frame[column] * frame["adj_factor"]
    frame["volume"] = frame["vol"]
    frame["trade_amount"] = frame["amount"]
    frame["change_rate"] = frame["pct_chg"]
    frame["previous_close"] = frame["pre_close"]
    frame["trade_status"] = 1
    frame = frame.sort_values(["stock_code", "date"], kind="stable")
    frame = frame.drop_duplicates(["stock_code", "date"], keep="first").reset_index(
        drop=True
    )

    grouped = frame.groupby("stock_code", sort=False)
    entry_open = grouped["open_hfq"].shift(-1)
    entry_exists = grouped["date"].shift(-1).notna()
    entry_close = grouped["close_hfq"].shift(-1)
    entry_change = grouped["change_rate"].shift(-1)
    # A next session exists but the stock's own next row may be a gap; without a
    # market calendar the best available guard is a price sanity check, and a
    # gap larger than the ETF limit is treated as a data gap rather than a move.
    entry_valid = (
        entry_exists
        & entry_open.notna()
        & (entry_open > 0)
        & (entry_close > 0)
        & (entry_change.abs() <= ETF_LIMIT)
    )

    for horizon in sorted({int(h) for h in horizons}):
        exit_open = grouped["open_hfq"].shift(-(1 + horizon))
        exit_close = grouped["close_hfq"].shift(-(1 + horizon))
        exit_exists = grouped["date"].shift(-(1 + horizon)).notna()
        exit_change = grouped["change_rate"].shift(-(1 + horizon))
        exit_valid = (
            exit_exists
            & exit_open.notna()
            & (exit_open > 0)
            & (exit_close > 0)
            & (exit_change.abs() <= ETF_LIMIT)
        )
        gross = (exit_open - entry_open) / entry_open
        blocked = pd.Series(pd.NA, index=frame.index, dtype="object")
        blocked = blocked.mask(~entry_exists, "no_next_session")
        blocked = blocked.mask(blocked.isna() & ~entry_valid, "missing_price")
        blocked = blocked.mask(blocked.isna() & ~exit_exists, "no_exit_yet")
        blocked = blocked.mask(blocked.isna() & ~exit_valid, "missing_price")
        frame[f"fwd_h{horizon}"] = gross.where(blocked.isna())
        frame[f"blocked_h{horizon}"] = blocked

    keep = [
        "date",
        "stock_code",
        "open",
        "close",
        "open_hfq",
        "close_hfq",
        "high_hfq",
        "low_hfq",
        "volume",
        "trade_amount",
        "change_rate",
        "previous_close",
        "trade_status",
    ]
    keep += [
        column
        for horizon in sorted({int(h) for h in horizons})
        for column in (f"fwd_h{horizon}", f"blocked_h{horizon}")
    ]
    return frame.loc[:, [c for c in keep if c in frame.columns]]


def etf_round_trip_cost() -> float:
    return 2 * ETF_SLIPPAGE_PER_SIDE + 2 * ETF_COMMISSION_RATE + ETF_SELL_STAMP_DUTY


def _price(frame: pd.DataFrame) -> pd.Series:
    hfq = pd.to_numeric(frame["close_hfq"], errors="coerce")
    raw = pd.to_numeric(frame["close"], errors="coerce")
    return hfq.where(hfq > 0, raw)


def _by_code(frame: pd.DataFrame, series: pd.Series) -> pd.Series:
    return series.groupby(frame["stock_code"], sort=False, observed=True)


def factor_values(panel: pd.DataFrame, name: str) -> pd.Series:
    """Small factor set that only needs the panel's own price/volume columns."""
    price = _price(panel)
    grouped = _by_code(panel, price)

    def momentum(window: int) -> pd.Series:
        return price / grouped.shift(window) - 1.0

    def rolling(series: pd.Series, window: int, how: str) -> pd.Series:
        return (
            series.groupby(panel["stock_code"], sort=False, observed=True)
            .rolling(window)
            .agg(how)
            .reset_index(level=0, drop=True)
        )

    if name.startswith("momentum_"):
        return momentum(int(name.split("_")[1]))
    if name.startswith("reversal_"):
        return -momentum(int(name.split("_")[1]))
    if name.startswith("trend_"):
        window = int(name.split("_")[1])
        average = rolling(price, window, "mean")
        return price / average.where(average > 0) - 1.0
    if name.startswith("volatility_"):
        window = int(name.split("_")[1])
        returns = grouped.pct_change(fill_method=None)
        return rolling(returns, window, "std") * math.sqrt(252)
    raise KeyError(f"unknown factor {name!r}")


def ic_report(panel: pd.DataFrame, factor: str, horizon: int) -> dict:
    label = f"fwd_h{horizon}"
    values = factor_values(panel, factor)
    work = pd.DataFrame(
        {
            "date": panel["date"],
            "factor": values,
            label: panel[label],
        }
    ).dropna()
    if work.empty:
        return {"horizon": horizon, "ic_mean": None, "n_dates": 0}
    ics = []
    for _, session in work.groupby("date", sort=True):
        if len(session) < 8:
            continue
        if session["factor"].nunique() < 2 or session[label].nunique() < 2:
            continue
        ics.append(float(session["factor"].rank().corr(session[label].rank())))
    if not ics:
        return {"horizon": horizon, "ic_mean": None, "n_dates": 0}
    series = pd.Series(ics)
    mean, std = (
        float(series.mean()),
        float(series.std(ddof=1)) if len(series) > 1 else 0.0,
    )
    return {
        "horizon": horizon,
        "ic_mean": round(mean, 5),
        "icir": round(mean / std, 4) if std > 0 else None,
        "t_stat": round(mean / std * math.sqrt(len(series)), 3) if std > 0 else None,
        "positive_share": round(float((series > 0).mean()), 4),
        "n_dates": int(len(series)),
        "n_observations": int(len(work)),
    }


def quantile_spread(
    panel: pd.DataFrame, factor: str, horizon: int, quantiles: int = 5
) -> dict:
    label = f"fwd_h{horizon}"
    values = factor_values(panel, factor)
    work = pd.DataFrame(
        {
            "date": panel["date"],
            "factor": values,
            label: panel[label],
        }
    ).dropna()
    if work.empty:
        return {"horizon": horizon, "top_minus_bottom": None}
    ranks = work.groupby("date")["factor"].rank(method="first")
    counts = work.groupby("date")["factor"].transform("count")
    work["bucket"] = (
        ((ranks - 1) / counts * quantiles).clip(upper=quantiles - 1e-9).astype(int)
    )
    work = work.loc[counts >= quantiles]
    if work.empty:
        return {"horizon": horizon, "top_minus_bottom": None}
    means = work.groupby("bucket")[label].mean()
    cost = etf_round_trip_cost()
    return {
        "horizon": horizon,
        "quantiles": [round(float(v), 6) for v in means.tolist()],
        "gross_top_minus_bottom": round(float(means.iloc[-1] - means.iloc[0]), 6),
        "top_minus_bottom": round(float(means.iloc[-1] - means.iloc[0]) - 2 * cost, 6),
        "top_avg_net": round(float(means.iloc[-1]) - cost, 6),
        "cost_per_leg": round(cost, 6),
    }


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="ETF small-account research pipeline")
    commands = parser.add_subparsers(dest="command", required=True)

    pool = commands.add_parser("pool", help="Build the tradable theme pool")
    pool.add_argument("--trade-date", required=True)
    pool.add_argument("--min-amount-kyuan", type=float, default=20000.0)
    pool.add_argument("--output", required=True)

    panel = commands.add_parser("panel", help="Freeze an ETF price panel")
    panel.add_argument("--pool", required=True)
    panel.add_argument("--from-date", required=True)
    panel.add_argument("--to-date", required=True)
    panel.add_argument("--horizons", default="5,20,60")
    panel.add_argument("--output", required=True)

    measure = commands.add_parser("measure", help="IC and quantile spread per factor")
    measure.add_argument("--panel", required=True)
    measure.add_argument(
        "--factors", default="reversal_5,reversal_20,momentum_20,trend_20,volatility_20"
    )
    measure.add_argument("--horizons", default="5,20,60")
    measure.add_argument("--output", default=None)

    args = parser.parse_args(argv)
    if not args.command:
        parser.print_help()
        return 2

    import os

    import tushare as ts

    pro = ts.pro_api(os.environ["TUSHARE_TOKEN"])

    if args.command == "pool":
        basic = fetch_basic(pro)
        daily = fetch_liquidity(pro, args.trade_date)
        pool = build_pool(basic, daily, min_amount_kyuan=args.min_amount_kyuan)
        Path(args.output).parent.mkdir(parents=True, exist_ok=True)
        pool.to_parquet(args.output, index=False)
        summary = {
            "trade_date": args.trade_date,
            "min_amount_kyuan": args.min_amount_kyuan,
            "pool_size": int(len(pool)),
            "themes_with_data": int(pool["theme"].nunique()),
            "output": args.output,
        }
        print(json.dumps(summary, ensure_ascii=False))
        return 0

    if args.command == "panel":
        pool = pd.read_parquet(args.pool)
        prices = fetch_prices(
            pro, pool["ts_code"].tolist(), args.from_date, args.to_date
        )
        horizons = [int(h) for h in args.horizons.split(",") if h.strip()]
        panel = build_etf_panel(prices, horizons)
        Path(args.output).parent.mkdir(parents=True, exist_ok=True)
        panel.to_parquet(args.output, index=False)
        coverage = {
            str(h): round(float(panel[f"fwd_h{h}"].notna().mean()), 4) for h in horizons
        }
        print(
            json.dumps(
                {
                    "rows": int(len(panel)),
                    "codes": int(panel["stock_code"].nunique()),
                    "from": str(panel["date"].min().date()),
                    "to": str(panel["date"].max().date()),
                    "label_coverage": coverage,
                    "output": args.output,
                },
                ensure_ascii=False,
            )
        )
        return 0

    panel = pd.read_parquet(args.panel)
    horizons = [int(h) for h in args.horizons.split(",") if h.strip()]
    factors = [f for f in args.factors.split(",") if f.strip()]
    report = {
        "panel": args.panel,
        "rows": int(len(panel)),
        "codes": int(panel["stock_code"].nunique()),
        "from": str(pd.to_datetime(panel["date"]).min().date()),
        "to": str(pd.to_datetime(panel["date"]).max().date()),
        "round_trip_cost": round(etf_round_trip_cost(), 6),
        "factors": {},
    }
    for factor in factors:
        report["factors"][factor] = {
            "ic": {str(h): ic_report(panel, factor, h) for h in horizons},
            "spread": {str(h): quantile_spread(panel, factor, h) for h in horizons},
        }
    if args.output:
        with open(args.output, "w", encoding="utf-8") as handle:
            json.dump(report, handle, ensure_ascii=False, indent=2, default=str)
    for factor, entry in report["factors"].items():
        for horizon, ic in entry["ic"].items():
            spread = entry["spread"][horizon]
            print(
                "%-14s h%-3s ic=%+.4f icir=%s t=%s n=%-4d | spread=%s top_net=%s"
                % (
                    factor,
                    horizon,
                    ic["ic_mean"] if ic["ic_mean"] is not None else float("nan"),
                    ic.get("icir"),
                    ic.get("t_stat"),
                    ic.get("n_dates", 0),
                    spread.get("top_minus_bottom"),
                    spread.get("top_avg_net"),
                )
            )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
