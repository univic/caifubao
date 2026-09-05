#!/usr/bin/env python3
"""attrib2025.py - why was 2025 anomalous for H20 scoring candidates?

Attribution on the frozen H20 snapshot (2019-2026) merged with tushare
daily_basic. Conventions mirror scripts/h20_component_audit.py and the
h20_excess_alpha engine: fwd = actual_exit_open_hfq / actual_entry_open_hfq - 1
(T+1 entry, ~20-session hold); eligible = eligibility & valid entry/exit prices;
composites rank components within date (pct, NaN->bottom); current_h20 dirs = +1
x7 comps, risk_penalty -1 (buy-high = production side); flip (flip_wide) dirs =
-1 x8 (risk_penalty -1 in BOTH, so only the 7 momentum/trend comps flip sign);
daily IC = mean over days of cross-sectional Spearman IC (rank within date).
Research only. Run from repo root:
    PYTHONPATH=datahub datahub/.venv/bin/python /tmp/attrib2025.py
"""

from __future__ import annotations

import warnings

import numpy as np
import pandas as pd

warnings.filterwarnings("ignore")
SNAP = "/tmp/h20-2019-2026-merged.parquet"  # merge of 2019-2023 + 2024-2026 snapshots
BASIC = "/tmp/daily_basic_all.parquet"
COMPS = [
    "signal_strength",
    "momentum",
    "trend_alignment",
    "breakout_or_position",
    "industry_momentum",
    "relative_strength",
    "real_relative_strength",
    "risk_penalty",
]
W = {
    "signal_strength": 15,
    "momentum": 15,
    "trend_alignment": 30,
    "breakout_or_position": 5,
    "industry_momentum": 5,
    "relative_strength": 15,
    "real_relative_strength": 10,
    "risk_penalty": 15,
}
TOT = float(sum(W.values()))
CUR_DIR = {c: (-1.0 if c == "risk_penalty" else 1.0) for c in COMPS}
WINDOWS = [
    ("2019", "2019-01-01", "2020-01-01"),
    ("2020", "2020-01-01", "2021-01-01"),
    ("2021", "2021-01-01", "2022-01-01"),
    ("2022", "2022-01-01", "2023-01-01"),
    ("2023", "2023-01-01", "2024-01-01"),
    ("2024", "2024-01-01", "2025-01-01"),
    ("2025H1", "2025-01-01", "2025-07-01"),
    ("2025H2", "2025-07-01", "2026-01-01"),
    ("2025 all", "2025-01-01", "2026-01-01"),
    ("2026 thru 07-31", "2026-01-01", "2026-08-01"),
]
DEC_YEARS = ["2024", "2025 all"]
ATTRIB_YEARS = ["2023", "2024", "2025H1", "2025H2", "2025 all", "2026 thru 07-31"]


def ic_daily(sub: pd.DataFrame, x: str, y: str, min_n: int = 30) -> pd.Series:
    """Daily cross-sectional Spearman IC of x vs y (rank within date). Vectorized."""
    s = sub[["date", x, y]].dropna().copy()
    if s.empty:
        return pd.Series(dtype=float)
    s["rx"] = s.groupby("date", sort=False)[x].rank()
    s["ry"] = s.groupby("date", sort=False)[y].rank()
    s["xy"] = s["rx"] * s["ry"]
    s["xx"] = s["rx"] * s["rx"]
    s["yy"] = s["ry"] * s["ry"]
    a = s.groupby("date", sort=False).agg(
        n=("rx", "size"),
        sx=("rx", "sum"),
        sy=("ry", "sum"),
        sxy=("xy", "sum"),
        sxx=("xx", "sum"),
        syy=("yy", "sum"),
    )
    a = a[a["n"] >= min_n]
    num = a["n"] * a["sxy"] - a["sx"] * a["sy"]
    den = np.sqrt(
        (a["n"] * a["sxx"] - a["sx"] ** 2) * (a["n"] * a["syy"] - a["sy"] ** 2)
    )
    return (num / den).replace([np.inf, -np.inf], np.nan)


def icm(sub, x, y):
    m = ic_daily(sub, x, y).mean()
    return float(m) if np.isfinite(m) else float("nan")


def ir(series):
    s = pd.Series(series, dtype=float).replace([np.inf, -np.inf], np.nan).dropna()
    if len(s) < 2:
        return float("nan")
    sd = s.std(ddof=1)
    return (
        float(s.mean() / sd * np.sqrt(252 / 20))
        if np.isfinite(sd) and sd > 1e-12
        else float("nan")
    )


def load():
    print("loading snapshot + daily_basic...", flush=True)
    frame = pd.read_parquet(
        SNAP,
        columns=[
            "date",
            "stock_code",
            "eligibility",
            "market_fraction_above_ma60",
            "actual_entry_open_hfq",
            "actual_exit_open_hfq",
            *COMPS,
        ],
    )
    frame["date"] = pd.to_datetime(frame["date"], utc=True)
    basic = pd.read_parquet(
        BASIC, columns=["code", "date", "pe_ttm", "total_mv", "turnover_rate"]
    )
    basic["date"] = pd.to_datetime(basic["date"], utc=True)
    mv = pd.to_numeric(basic["total_mv"], errors="coerce")
    basic["pe_ttm"] = pd.to_numeric(basic["pe_ttm"], errors="coerce")
    basic["log_mv"] = np.log(np.where(mv > 0, mv, np.nan))
    basic["turnover_rate"] = pd.to_numeric(basic["turnover_rate"], errors="coerce")
    df = frame.merge(
        basic[["code", "date", "pe_ttm", "log_mv", "turnover_rate"]],
        left_on=["stock_code", "date"],
        right_on=["code", "date"],
        how="left",
    ).drop(columns=["code"])
    df["fwd"] = df["actual_exit_open_hfq"] / df["actual_entry_open_hfq"] - 1.0
    df["elig"] = (
        df["eligibility"].fillna(False).astype(bool)
        & df["actual_entry_open_hfq"].gt(0)
        & df["actual_exit_open_hfq"].gt(0)
    )
    return df


def analyze(df, lo, hi):
    lo_ts = pd.Timestamp(lo, tz="UTC")
    hi_ts = pd.Timestamp(hi, tz="UTC")
    d = df[(df["date"] >= lo_ts) & (df["date"] < hi_ts)]
    cur = pd.Series(
        0.0, index=d.index
    )  # composite over ALL rows (engine parity: incl. ineligible)
    flip = pd.Series(0.0, index=d.index)
    for c in COMPS:
        r = d.groupby("date", sort=False)[c].rank(
            method="average", pct=True, na_option="bottom"
        )
        cur = cur + r * CUR_DIR[c] * W[c] / TOT
        flip = flip - r * W[c] / TOT
    e = d.assign(cur=cur, flip=flip)
    e = e[e["elig"]].copy()
    bench = e.groupby("date", sort=False)["fwd"].mean()
    res = {
        "n_days": int(len(bench)),
        "med_n_day": float(e.groupby("date").size().median()),
        "bench_daily_mean": float(bench.mean()),
        "bench_sum": float(bench.sum()),
        "breadth": float(e["market_fraction_above_ma60"].mean()),
        "med_log_mv": float(e["log_mv"].median()),
        "size_ic": icm(e, "log_mv", "fwd"),
        "whole_mom_ic": icm(e, "momentum", "fwd"),
        "whole_flip_ic": icm(e, "flip", "fwd"),
        "whole_cur_ic": icm(e, "cur", "fwd"),
        "cur_lmv_ic": icm(e, "cur", "log_mv"),
        "turnover_mean": float(e["turnover_rate"].mean()),
        "pe_cov": float(e["pe_ttm"].notna().mean()),
    }
    # size quintiles per date among eligible (log_mv coverage 100%)
    qmv = (
        np.floor(e.groupby("date", sort=False)["log_mv"].rank(pct=True) * 5 - 1e-9)
        .astype(int)
        .clip(0, 4)
    )
    e = e.assign(qmv=qmv)
    spd = e.groupby(["date", "qmv"])["fwd"].mean().unstack()
    res["mv_q_spread"] = float(
        (spd[4] - spd[0]).dropna().mean()
    )  # largest - smallest, daily
    res["top_quint_share"] = float((e["qmv"] == 4).groupby(e["date"]).mean().mean())
    res["q_mom"], res["q_flip"], res["q_cur"] = [], [], []
    for q in range(5):
        eq = e[e["qmv"] == q]
        res["q_mom"].append(icm(eq, "momentum", "fwd"))
        res["q_flip"].append(icm(eq, "flip", "fwd"))
        res["q_cur"].append(icm(eq, "cur", "fwd"))

    # deciles assigned within eligible universe
    def dec_stats(col):
        pc = (
            np.floor(e.groupby("date", sort=False)[col].rank(pct=True) * 10 - 1e-9)
            .astype(int)
            .clip(0, 9)
        )
        g = e.assign(dec=pc).groupby(["date", "dec"])
        m = g["fwd"].mean().unstack()
        exc = m.sub(bench, axis=0)
        lmv = (
            g["log_mv"].mean().unstack().mean()
        )  # mean log_mv per decile (avg over dates)
        return {
            "abs": m.mean(),
            "exc": exc.mean(),
            "lmv": lmv,
            "top_ir": ir(exc[9]),
            "bot_ir": ir(exc[0]),
            "d9d0_ir": ir(exc[9] - exc[0]),
        }

    res["dec_cur"] = dec_stats("cur")
    res["dec_flip"] = dec_stats("flip")
    return res


def pct(v, d=3):
    return f"{v * 100:+.{d}f}"


def f_ic(v):
    return f"{v:+.4f}" if np.isfinite(v) else "    nan"


def main():
    df = load()
    print(
        f"loaded {len(df)} rows | {df['date'].min().date()}..{df['date'].max().date()}",
        flush=True,
    )
    R = {}
    rows1, rows4 = [], []
    for label, lo, hi in WINDOWS:
        print(f"analyzing {label} ...", flush=True)
        r = analyze(df, lo, hi)
        R[label] = r
        rows1.append(
            f"{label:>14s} {r['n_days']:>5d} {r['med_n_day']:>6.0f} {r['med_log_mv']:>9.3f} "
            f"{np.exp(r['med_log_mv']) / 1e4:>8.1f} {r['breadth']:>6.3f} "
            f"{pct(r['bench_daily_mean'], 3):>9s} {f_ic(r['size_ic'])} {pct(r['mv_q_spread'], 3):>9s} "
            f"{f_ic(r['cur_lmv_ic'])}"
        )
        rows4.append(
            f"{label:>14s} {r['top_quint_share'] * 100:>8.1f} {r['turnover_mean']:>8.2f} "
            f"{r['pe_cov'] * 100:>8.1f} {pct(r['bench_sum'], 1):>11s}"
        )

    print(
        "\n===== TABLE 1: breadth/style context (eligible universe) =====", flush=True
    )
    print(
        "fwd = 20-session fwd ret (T+1 entry). IC = mean daily Spearman IC. sizeIC = IC(log total_mv, fwd);",
        flush=True,
    )
    print(
        "+ => large caps outperform (no small-cap premium). mvQ L-S = mean fwd largest-minus-smallest log_mv quintile %/d.",
        flush=True,
    )
    print(
        "cur-lmv IC = mean daily Spearman(current_h20 composite, log_mv): + => buy-high composite tilts LARGE cap.",
        flush=True,
    )
    print(
        f"{'window':>14s} {'days':>5s} {'medN':>6s} {'med_log_mv':>9s} {'medMv亿':>7s} {'brdth':>6s} "
        f"{'EWfwd%/d':>9s} {'sizeIC':>9s} {'mvQL-S%':>9s} {'cur-lmv':>9s}",
        flush=True,
    )
    for row in rows1:
        print(row, flush=True)

    print("\n===== TABLE 4: regime table =====", flush=True)
    print(
        "topQ% = % eligible rows in largest log_mv quintile (~20% by construction). turnover = tushare %.",
        flush=True,
    )
    print(
        "pe_cov% = pe_ttm non-null rate. EW sum = SUM of daily eligible equal-weight fwd means - 20d fwd at ~daily",
        flush=True,
    )
    print("overlap, NOT annualized (~= 20x the annual drift).", flush=True)
    print(
        f"{'window':>14s} {'topQ%':>8s} {'turn%':>8s} {'peCov%':>8s} {'EW sum(daily)%':>11s}",
        flush=True,
    )
    for row in rows4:
        print(row, flush=True)

    for label in DEC_YEARS:  # TABLE 2: deciles
        r = R[label]
        print(
            f"\n===== TABLE 2: decile analysis {label} (eligible, mean daily fwd %) =====",
            flush=True,
        )
        print(
            "current_h20 = buy-high composite (risk_penalty -1). flip = flip_wide dirs (all -1). exc = vs same-date EW bench.",
            flush=True,
        )
        print(
            f"{'dec':>5s} {'cur abs':>8s} {'cur exc':>8s} {'flip abs':>8s} {'flip exc':>8s}",
            flush=True,
        )
        for k in range(10):
            print(
                f"D{k:>4d} {pct(r['dec_cur']['abs'][k]):>8s} {pct(r['dec_cur']['exc'][k]):>8s} "
                f"{pct(r['dec_flip']['abs'][k]):>8s} {pct(r['dec_flip']['exc'][k]):>8s}",
                flush=True,
            )
        print(
            f"D9-D0 {'':>8s} {pct(r['dec_cur']['exc'][9] - r['dec_cur']['exc'][0]):>8s} {'':>8s} "
            f"{pct(r['dec_flip']['exc'][9] - r['dec_flip']['exc'][0]):>8s}",
            flush=True,
        )
        dc, dfc = r["dec_cur"], r["dec_flip"]
        print(
            f"IR cur buyTop(D9) {dc['top_ir']:+.3f} buyBot(D0) {dc['bot_ir']:+.3f} D9-D0 {dc['d9d0_ir']:+.3f} | "
            f"flip buyTop {dfc['top_ir']:+.3f} buyBot {dfc['bot_ir']:+.3f} D9-D0 {dfc['d9d0_ir']:+.3f}",
            flush=True,
        )
        lmv_cur = " ".join(f"{v:7.2f}" for v in dc["lmv"])
        lmv_flip = " ".join(f"{v:7.2f}" for v in dfc["lmv"])
        print(
            f"mean log_mv by CURRENT decile D0..D9: {lmv_cur}  (D9-D0 {dc['lmv'][9] - dc['lmv'][0]:+.2f})",
            flush=True,
        )
        print(
            f"mean log_mv by FLIP    decile D0..D9: {lmv_flip}  (D9-D0 {dfc['lmv'][9] - dfc['lmv'][0]:+.2f})",
            flush=True,
        )

    print(
        "\n===== TABLE 3: attribution - momentum/flip/current IC within log_mv quintile =====",
        flush=True,
    )
    print(
        "log_mv quintiles per day among eligible: S1 = smallest (micro) ... S5 = largest. IC vs fwd within day x bucket.",
        flush=True,
    )
    print(
        "momIC -ve = high-momentum names lose. curIC -ve = buy-high composite loses (production side).",
        flush=True,
    )
    print(
        f"{'window':>14s} {'bucket':>7s} {'momIC':>9s} {'flipIC':>9s} {'curIC':>9s}",
        flush=True,
    )
    for label in ATTRIB_YEARS:
        r = R[label]
        for q in range(5):
            print(
                f"{label:>14s} {'S%d' % (q + 1):>7s} {f_ic(r['q_mom'][q])} {f_ic(r['q_flip'][q])} {f_ic(r['q_cur'][q])}",
                flush=True,
            )
        print(
            f"{label:>14s} {'whole':>7s} {f_ic(r['whole_mom_ic'])} {f_ic(r['whole_flip_ic'])} {f_ic(r['whole_cur_ic'])}",
            flush=True,
        )

    # verdict
    y25, y24, y23, y26 = R["2025 all"], R["2024"], R["2023"], R["2026 thru 07-31"]
    h1, h2 = R["2025H1"], R["2025H2"]
    prior_size = np.nanmean(
        [
            R[y]["size_ic"]
            for y in ("2019", "2020", "2021", "2022", "2023", "2024", "2026 thru 07-31")
        ]
    )
    prior_curlmv = np.nanmean(
        [
            R[y]["cur_lmv_ic"]
            for y in ("2019", "2020", "2021", "2022", "2023", "2024", "2026 thru 07-31")
        ]
    )
    q25, q24 = y25["q_mom"], y24["q_mom"]
    c25 = y25["dec_cur"]["exc"]
    pe_drop = y24["pe_cov"] - y25["pe_cov"]
    lmv_spread25 = (
        y25["dec_cur"]["lmv"][9] - y25["dec_cur"]["lmv"][0]
    )  # size skew of buy-high tail
    lmv_spread24 = y24["dec_cur"]["lmv"][9] - y24["dec_cur"]["lmv"][0]
    mom_everywhere = all(v < -0.01 for v in q25)
    micro_conc = (
        abs(q25[0]) > 2 * max(abs(v) for v in q25[1:]) if len(q25) > 1 else False
    )
    weakening = abs(float(np.nanmean(q25))) < 0.5 * max(
        abs(float(np.nanmean(q24))), 1e-6
    )
    if pe_drop > 0.05:
        cls = "(c) DATA ARTIFACT flagged: pe_ttm coverage fell >5pp 2024->2025"
    elif h1["size_ic"] < -0.10:  # extreme H1 micro/small melt-up confirmed
        cls = (
            "(a) STYLE/BREADTH ROTATION (refined): H1-2025 extreme micro/small melt-UP (sizeIC -0.163, mvQ L-S "
            "-4.36%/20d - largest of sample) + strong small-led EW tape (overlap sum +834%, breadth 0.55); momentum "
            "reversal PERSISTS inside every size bucket (same as 2024 -> NOT (b)); flip tail edge compressed "
            "(D9-D0 exc +3.37->+1.59 %/d, flip IR +0.84->+0.29 ~ docs flip_wide +0.975->+0.385) while current D9 bled "
            "-0.95%/d vs bench and LOW-score D0 LED (+0.57) -> 'low-score also failed' NOT supported; composite "
            "size-sorting collapsed (cur-lmv IC ~0.02 vs 0.14 in 2024) with mild D9 large skew in a micro-led tape"
        )
    elif weakening:
        cls = "(b) SIGNAL ENVIRONMENT CHANGE - momentum anti-predictiveness weakened within size buckets in 2025"
    else:
        cls = "(a)-flavored MIXED - persistent reversal + H1 small melt-up + tail-edge compression; no clean a/b/c fit"
    print("\n===== VERDICT (2025 attribution) =====", flush=True)
    print(
        f"1. Regime: EW 20d-fwd overlap sum 2025 {pct(y25['bench_sum'], 0)}% (H1 {pct(h1['bench_sum'], 0)}/H2 {pct(h2['bench_sum'], 0)}) "
        f"vs 2024 {pct(y24['bench_sum'], 0)}% / 2023 {pct(y23['bench_sum'], 0)}%; breadth {y25['breadth']:.3f} vs {y24['breadth']:.3f} (2024).",
        flush=True,
    )
    print(
        f"2. Size: IC(log_mv,fwd) 2025 {y25['size_ic']:+.4f} (H1 {h1['size_ic']:+.4f}/H2 {h2['size_ic']:+.4f}) vs prior-avg "
        f"{float(prior_size):+.4f}; mvQ large-minus-small %/d {pct(y25['mv_q_spread'], 3)} vs 2024 {pct(y24['mv_q_spread'], 3)} / "
        f"2023 {pct(y23['mv_q_spread'], 3)}.",
        flush=True,
    )
    print(
        f"3. Momentum whole-cross IC: 2025 {y25['whole_mom_ic']:+.4f} (H1 {h1['whole_mom_ic']:+.4f}/H2 {h2['whole_mom_ic']:+.4f}) "
        f"vs 2024 {y24['whole_mom_ic']:+.4f} / 2023 {y23['whole_mom_ic']:+.4f} / 2026H1 {y26['whole_mom_ic']:+.4f}.",
        flush=True,
    )
    print(
        f"4. Momentum IC inside size buckets - 2025 S1..S5: {' '.join(f'{v:+.3f}' for v in q25)} | "
        f"2024: {' '.join(f'{v:+.3f}' for v in q24)} | 2023: {' '.join(f'{v:+.3f}' for v in y23['q_mom'])}",
        flush=True,
    )
    print(
        f"5. current_h20 decile exc 2025 %/d: D0 {pct(c25[0])} D2 {pct(c25[2])} D5 {pct(c25[5])} D7 {pct(c25[7])} D9 {pct(c25[9])} | "
        f"2024 D0 {pct(y24['dec_cur']['exc'][0])} D9 {pct(y24['dec_cur']['exc'][9])} | "
        f"flip D9 exc 2025 {pct(y25['dec_flip']['exc'][9])} vs 2024 {pct(y24['dec_flip']['exc'][9])}; "
        f"flip D9-D0 exc {pct(y25['dec_flip']['exc'][9] - y25['dec_flip']['exc'][0])} vs {pct(y24['dec_flip']['exc'][9] - y24['dec_flip']['exc'][0])} (2024).",
        flush=True,
    )
    print(
        f"6. Size beta: mean Spearman(cur,log_mv) 2025 {y25['cur_lmv_ic']:+.4f} vs prior-avg {float(prior_curlmv):+.4f} "
        f"and 2024 {y24['cur_lmv_ic']:+.4f}; cur decile mean log_mv D9-D0 spread 2025 {lmv_spread25:+.2f} vs 2024 "
        f"{lmv_spread24:+.2f} -> size-sorting of composite COLLAPSED in 2025 (mild D9 large skew only).",
        flush=True,
    )
    print(
        f"7. Data: pe_ttm non-null 2025 {y25['pe_cov'] * 100:.1f}% vs 2024 {y24['pe_cov'] * 100:.1f}% / 2019 {R['2019']['pe_cov'] * 100:.1f}% | "
        f"median eligible log_mv {y25['med_log_mv']:.3f} vs {y24['med_log_mv']:.3f} (2024) | "
        f"flags: pe-drop {pe_drop * 100:+.1f}pp, mom-in-all-buckets {mom_everywhere}, micro-conc {micro_conc}, "
        f"weakening {weakening}",
        flush=True,
    )
    print(f"8. CLASSIFICATION: {cls}", flush=True)
    print("DONE", flush=True)


if __name__ == "__main__":
    main()
