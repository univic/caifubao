# -*- coding: utf-8 -*-
"""Do oversold, trend and market-state signals carry repeatable incremental alpha?

Four tests, all on the corrected 000977 series:

1. full-sample rank IC with a block-bootstrap confidence interval;
2. IC stability across three eras (a signal that only works in one era is not
   repeatable);
3. an event study for the binary versions (mean forward return when on vs off);
4. expanding-window out-of-sample regressions that compare nested models, so
   the *incremental* predictive content of each family is measured rather than
   its standalone correlation;
5. a block-bootstrap Reality Check on the maximum t-stat across all variants,
   because a dozen signals were examined.

No transaction costs are applied here: this measures predictive content, not a
tradable implementation (the economic-value column is a simple sign rule).
"""

from __future__ import annotations

import json
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from scan import load

HERE = Path(__file__).resolve().parent
OUT = HERE / "out"

ERAS = [
    ("2005-2012", "2005-01-01", "2012-12-31"),
    ("2013-2018", "2013-01-01", "2018-12-31"),
    ("2019-2026", "2019-01-01", "2026-12-31"),
]


def features(df: pd.DataFrame, index_frame: pd.DataFrame) -> pd.DataFrame:
    close = df["close"]
    csi = index_frame["close"].reindex(df.index).ffill()
    delta = close.diff()
    gain = delta.clip(lower=0).ewm(alpha=1 / 14, adjust=False).mean()
    loss = (-delta.clip(upper=0)).ewm(alpha=1 / 14, adjust=False).mean()
    f = pd.DataFrame(index=df.index)
    f["oversold"] = -close.pct_change(120)  # higher = more oversold
    f["oversold_bin"] = (close.pct_change(120) < -0.25).astype(float)
    f["rsi_low"] = (100 - 100 / (1 + gain / loss) < 35).astype(float)
    f["trend_ma60"] = (close > df["ma60"]).astype(float)
    f["trend_ma200"] = (close > df["ma200"]).astype(float)
    f["trend_dist200"] = close / df["ma200"] - 1
    f["mom250"] = close.pct_change(250)
    f["csi_ma200"] = (csi > csi.rolling(200).mean()).astype(float)
    f["csi_dd250"] = csi / csi.rolling(250).max() - 1
    f["vol20"] = df["vol20"]
    f["csi_vol60"] = csi.pct_change().rolling(60).std() * np.sqrt(244)
    return f


def forward(df: pd.DataFrame, horizon: int) -> pd.Series:
    return df["close"].shift(-horizon) / df["close"] - 1


def block_bootstrap_ic(
    signal: np.ndarray,
    fwd: np.ndarray,
    block: int = 20,
    reps: int = 1000,
    seed: int = 3,
):
    rng = np.random.default_rng(seed)
    n = len(signal)
    n_blocks = n // block
    obs = []
    for _ in range(reps):
        starts = rng.integers(0, n - block, size=n_blocks)
        idx = np.concatenate([np.arange(s, s + block) for s in starts])[:n]
        s, f = signal[idx], fwd[idx]
        if np.std(s) > 0 and np.std(f) > 0:
            obs.append(np.corrcoef(pd.Series(s).rank(), pd.Series(f).rank())[0, 1])
    return float(np.percentile(obs, 2.5)), float(np.percentile(obs, 97.5))


def ic_table(f: pd.DataFrame, df: pd.DataFrame, horizon: int = 20) -> pd.DataFrame:
    fwd = forward(df, horizon)
    rows = []
    for name in f.columns:
        x = f[name]
        ok = x.notna() & fwd.notna()
        xv, yv = x[ok].to_numpy(), fwd[ok].to_numpy()
        ic = float(np.corrcoef(pd.Series(xv).rank(), pd.Series(yv).rank())[0, 1])
        n_eff = max(len(xv) / horizon, 1.0)
        t = ic * np.sqrt(n_eff - 2) / np.sqrt(max(1 - ic**2, 1e-12))
        lo, hi = block_bootstrap_ic(xv, yv)
        rec = {
            "signal": name,
            "horizon": horizon,
            "n": int(len(xv)),
            "ic": ic,
            "t": t,
            "ci_lo": lo,
            "ci_hi": hi,
        }
        for label, start, end in ERAS:
            mask = (x.index >= start) & (x.index <= end) & ok
            if mask.sum() > 200:
                xr = x[mask]
                yr = fwd[mask]
                rec[f"ic_{label}"] = float(np.corrcoef(xr.rank(), yr.rank())[0, 1])
            else:
                rec[f"ic_{label}"] = np.nan
        eras = [rec[f"ic_{label}"] for label, _, _ in ERAS]
        rec["same_sign_eras"] = int(
            sum(1 for e in eras if np.isfinite(e) and np.sign(e) == np.sign(ic))
        )
        rows.append(rec)
    return pd.DataFrame(rows)


def event_table(
    f: pd.DataFrame,
    df: pd.DataFrame,
    horizon: int = 20,
    block: int = 20,
    reps: int = 1000,
) -> pd.DataFrame:
    fwd = forward(df, horizon)
    rng = np.random.default_rng(7)
    rows = []
    for name in ("oversold_bin", "rsi_low", "trend_ma60", "trend_ma200", "csi_ma200"):
        on = f[name].fillna(0).to_numpy() > 0
        ok = fwd.notna().to_numpy()
        y = fwd.to_numpy()
        a, b = y[on & ok], y[~on & ok]
        diff = a.mean() - b.mean()
        n = len(y)
        boot = []
        for _ in range(reps):
            starts = rng.integers(0, n - block, size=n // block)
            idx = np.concatenate([np.arange(s, s + block) for s in starts])[:n]
            mask = ok[idx]
            boot.append(
                y[idx][on[idx] & mask].mean() - y[idx][~(on[idx] & mask) & mask].mean()
            )
        rows.append(
            {
                "signal": name,
                "horizon": horizon,
                "n_on": int(on.sum()),
                "mean_on": float(a.mean()),
                "mean_off": float(b.mean()),
                "diff": float(diff),
                "ci_lo": float(np.percentile(boot, 2.5)),
                "ci_hi": float(np.percentile(boot, 97.5)),
                "hit_on": float((a > 0).mean()),
                "hit_off": float((b > 0).mean()),
            }
        )
    return pd.DataFrame(rows)


def oos_regression(
    f: pd.DataFrame,
    df: pd.DataFrame,
    horizon: int = 20,
    min_train: int = 600,
    refit: int = 20,
) -> pd.DataFrame:
    fwd = forward(df, horizon)
    models = {
        "baseline_vol": ["vol20"],
        "vol+market": ["vol20", "csi_ma200"],
        "vol+market+oversold": ["vol20", "csi_ma200", "oversold"],
        "vol+market+trend": ["vol20", "csi_ma200", "trend_dist200"],
        "vol+market+oversold+trend": [
            "vol20",
            "csi_ma200",
            "oversold",
            "trend_dist200",
        ],
    }
    data = f.join(fwd.rename("fwd")).dropna()
    rows = []
    for name, cols in models.items():
        preds = pd.Series(np.nan, index=data.index)
        for i in range(min_train + horizon, len(data), refit):
            # purge: only rows whose label has been revealed by the decision date
            train = data.iloc[: i - horizon]
            test = data.iloc[i : i + refit]
            X = train[cols].to_numpy()
            y = train["fwd"].to_numpy()
            mu, sd = X.mean(axis=0), X.std(axis=0)
            sd[sd == 0] = 1.0
            Xs = (X - mu) / sd
            Xt = (test[cols].to_numpy() - mu) / sd
            Xs = np.column_stack([np.ones(len(Xs)), Xs])
            Xt = np.column_stack([np.ones(len(Xt)), Xt])
            beta, *_ = np.linalg.lstsq(Xs, y, rcond=None)
            preds.iloc[i : i + refit] = Xt @ beta
        oos = pd.DataFrame({"pred": preds, "fwd": data["fwd"]}).dropna()
        sse = float(((oos.pred - oos.fwd) ** 2).sum())
        sst = float((oos.fwd**2).sum())  # benchmark forecast = 0, available ex ante
        r2 = 1 - sse / sst if sst > 0 else 0.0
        hit = float((np.sign(oos.pred) == np.sign(oos.fwd)).mean())
        exposure = (oos.pred > 0).astype(float)
        strat = (exposure * oos.fwd).mean()  # cash earns zero
        base = oos.fwd.mean()
        rows.append(
            {
                "model": name,
                "features": "+".join(cols),
                "oos_n": int(len(oos)),
                "oos_r2": float(r2),
                "hit_rate": hit,
                "mean_fwd_when_long": float(strat),
                "mean_fwd_all": float(base),
            }
        )
    return pd.DataFrame(rows)


def reality_check(
    f: pd.DataFrame,
    df: pd.DataFrame,
    horizon: int = 20,
    reps: int = 1000,
    seed: int = 23,
) -> dict:
    """Max-statistic test against a no-association null.

    Each signal is circularly shifted by an independent random offset, which
    preserves its own autocorrelation and destroys its alignment with forward
    returns.  The null distribution of max |t| across signals is built from
    those shifted signals, so a perfect predictor would give p ~ 0 (unlike the
    earlier joint-resampling version, which could not reject anything).
    """
    fwd = forward(df, horizon)
    cols = list(f.columns)
    data = f[cols].join(fwd.rename("fwd")).dropna()
    n = len(data)
    n_eff = max(n / horizon, 1.0)
    y = data["fwd"].to_numpy()
    ranks_y = pd.Series(y).rank().to_numpy()

    def tstats(series: dict[str, np.ndarray]) -> dict[str, float]:
        out = {}
        for c, x in series.items():
            if np.std(x) == 0:
                out[c] = 0.0
                continue
            ic = np.corrcoef(pd.Series(x).rank(), ranks_y)[0, 1]
            out[c] = ic * np.sqrt(n_eff - 2) / np.sqrt(max(1 - ic**2, 1e-12))
        return out

    observed = tstats({c: data[c].to_numpy() for c in cols})
    observed_max = max(abs(v) for v in observed.values())
    rng = np.random.default_rng(seed)
    maxima = []
    base = {c: data[c].to_numpy() for c in cols}
    for _ in range(reps):
        shifted = {c: np.roll(x, int(rng.integers(1, n))) for c, x in base.items()}
        ts = tstats(shifted)
        maxima.append(max(abs(v) for v in ts.values()))
    maxima = np.array(maxima)
    return {
        "method": "independent circular shifts of each signal; max |t| null",
        "signals": len(cols),
        "observed_max_abs_t": float(observed_max),
        "null_p95_max_abs_t": float(np.percentile(maxima, 95)),
        "bootstrap_p": float((maxima >= observed_max).mean()),
        "best_signal": max(observed, key=lambda k: abs(observed[k])),
        "tstats": {k: round(float(v), 3) for k, v in observed.items()},
    }


def main() -> int:
    df, index_frame = load()
    f = features(df, index_frame)
    ic20 = ic_table(f, df, 20)
    ic60 = ic_table(f, df, 60)
    events20 = event_table(f, df, 20)
    events60 = event_table(f, df, 60)
    reg = oos_regression(f, df, 20)
    rc = reality_check(f, df, 20)

    ic20.to_csv(OUT / "alpha_ic20.csv", index=False)
    ic60.to_csv(OUT / "alpha_ic60.csv", index=False)
    events20.to_csv(OUT / "alpha_events20.csv", index=False)
    events60.to_csv(OUT / "alpha_events60.csv", index=False)
    reg.to_csv(OUT / "alpha_oos_regression.csv", index=False)
    (OUT / "alpha_reality_check.json").write_text(
        json.dumps(rc, indent=2, ensure_ascii=False), encoding="utf-8"
    )

    pd.set_option("display.width", 250)
    print("=== IC (20d) ===")
    print(
        ic20[
            [
                "signal",
                "ic",
                "t",
                "ci_lo",
                "ci_hi",
                "ic_2005-2012",
                "ic_2013-2018",
                "ic_2019-2026",
                "same_sign_eras",
            ]
        ].to_string(index=False, float_format=lambda v: f"{v:.3f}")
    )
    print("\n=== Event study (20d) ===")
    print(events20.to_string(index=False, float_format=lambda v: f"{v:.4f}"))
    print("\n=== OOS regression (expanding window, 20d) ===")
    print(reg.to_string(index=False, float_format=lambda v: f"{v:.4f}"))
    print("\n=== Reality check ===")
    print(json.dumps(rc, indent=2, ensure_ascii=False))

    # ---------------------------------------------------------------- figure
    show = ["oversold", "rsi_low", "trend_dist200", "csi_ma200", "vol20"]
    fig, axes = plt.subplots(1, 3, figsize=(13.5, 4.2))
    x = np.arange(len(show))
    width = 0.26
    for k, (label, _, _) in enumerate(ERAS):
        vals = [ic20.loc[ic20.signal == s, f"ic_{label}"].iloc[0] for s in show]
        axes[0].bar(x + (k - 1) * width, vals, width, label=label)
    axes[0].axhline(0, color="black", lw=0.6)
    axes[0].set_xticks(x, show, rotation=25, ha="right", fontsize=7)
    axes[0].set_title("Rank IC by era (20d)")
    axes[0].legend(frameon=False, fontsize=7)

    ev = events20.set_index("signal").loc[
        ["oversold_bin", "rsi_low", "trend_ma60", "trend_ma200", "csi_ma200"]
    ]
    axes[1].errorbar(
        np.arange(len(ev)),
        ev["diff"] * 100,
        yerr=[(ev["diff"] - ev["ci_lo"]) * 100, (ev["ci_hi"] - ev["diff"]) * 100],
        fmt="o",
        color="#c0392b",
        capsize=3,
    )
    axes[1].axhline(0, color="black", lw=0.6)
    axes[1].set_xticks(
        np.arange(len(ev)), ev.index, rotation=25, ha="right", fontsize=7
    )
    axes[1].set_ylabel("forward 20d diff (pp)")
    axes[1].set_title("Event study: on minus off, 95% CI")

    axes[2].barh(reg["model"], reg["oos_r2"] * 100, color="#2980b9")
    axes[2].axvline(0, color="black", lw=0.6)
    axes[2].set_xlabel("OOS R2 (%)")
    axes[2].set_title(
        f"OOS R2 (negative = no value)\nreality-check p = {rc['bootstrap_p']:.2f}"
    )
    axes[2].tick_params(axis="y", labelsize=7)
    fig.suptitle("Does 000977 have repeatable predictive signals?")
    fig.savefig(HERE / "figures" / "fig8_alpha_evidence.png")
    plt.close(fig)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
