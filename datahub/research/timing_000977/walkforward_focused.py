# -*- coding: utf-8 -*-
"""Anchored walk-forward inside the final structural family.

The structure (calm-vol gate x reversal boost x CSI300 bear override x optional
vol target) is fixed before the test.  Each block re-selects the small set of
free parameters on history only, which is the honest test of the design rather
than of one hand-picked configuration.
"""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from engine import simulate
from scan import load
from strategy import Config, ExposureBuilder
from walkforward import curve_metrics, stitch

HERE = Path(__file__).resolve().parent
OUT = HERE / "out"


def family() -> list[Config]:
    out = []
    for thr, scale in ((0.15, 0.5), (0.20, 0.5), (0.20, 0.3)):
        for blend in (0.4, 0.5):
            for rev in (0.25, 0.30):
                for vt in (None, 0.55):
                    out.append(
                        Config(
                            calm_q=0.70,
                            rev_entry=rev,
                            blend=blend,
                            override_thr=thr,
                            override_scale=scale,
                            vol_target=vt,
                        )
                    )
    return out


def blocks(df, first_year=2011):
    out = []
    start = pd.Timestamp(f"{first_year}-01-01")
    while start <= df.index[-1]:
        end = min(start + pd.DateOffset(years=1) - pd.Timedelta(days=1), df.index[-1])
        out.append((start, end))
        start = end + pd.Timedelta(days=1)
    return out


def main() -> int:
    df, index_frame = load()
    builder = ExposureBuilder(df, index_frame)
    fam = family()
    exposures = {c.label: builder.build(c) for c in fam}
    summary = {}
    for top_k in (1, 3, 5):
        pieces, picks, expo_pieces = [], [], []
        for start, end in blocks(df):
            hist = df.loc[: start - pd.Timedelta(days=1)]
            if len(hist) < 900:
                continue
            scored = []
            for cfg in fam:
                m = simulate(
                    hist, exposures[cfg.label].reindex(hist.index), band=cfg.band
                ).metrics
                scored.append((cfg, m["calmar"]))
            scored.sort(key=lambda x: -x[1])
            chosen = [c for c, _ in scored[:top_k]]
            seg = df.loc[start:end]
            expo = pd.concat(
                [exposures[c.label].reindex(seg.index) for c in chosen], axis=1
            ).mean(axis=1)
            res = simulate(seg, expo, band=0.05)
            pieces.append(res.equity)
            expo_pieces.append(expo)
            picks.append(
                {
                    "block": f"{start.date()}..{end.date()}",
                    "chosen": "+".join(c.label for c in chosen),
                }
            )
        curve = stitch(pieces)
        m = curve_metrics(curve)
        bh = simulate(
            df.loc[curve.index[0] :],
            pd.Series(1.0, index=df.loc[curve.index[0] :].index),
            band=0.05,
        ).metrics
        summary[f"top{top_k}"] = {"metrics": m, "buy_hold": bh}
        curve.to_frame("equity").to_csv(OUT / f"wf_final_top{top_k}.csv")
        if top_k == 1:
            pd.concat(expo_pieces).to_frame("exposure").to_csv(
                OUT / "wf_final_exposure.csv"
            )
        print(
            f"top_k={top_k}: CAGR={m['cagr']:.2%} MDD={m['max_drawdown']:.2%} "
            f"Calmar={m['calmar']:.2f} Sharpe={m['sharpe']:.2f} | B&H CAGR={bh['cagr']:.2%} MDD={bh['max_drawdown']:.2%}"
        )
        if top_k == 1:
            pd.DataFrame(picks).to_csv(OUT / "wf_final_choices.csv", index=False)
    (OUT / "wf_final_summary.json").write_text(
        json.dumps(summary, indent=2, default=float)
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
