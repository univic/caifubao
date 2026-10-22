# -*- coding: utf-8 -*-
"""Rule ablation for the 000977 timing strategy.

Each row switches off exactly one module of

    exposure = calm_vol_gate * (base + weight * oversold_boost) * market_override

or adds the optional volatility cap, and reports full / in-sample / out-of-sample
performance on the corrected price series.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from engine import simulate
from scan import IS_END, OOS_START, load
from strategy import Config, ExposureBuilder

HERE = Path(__file__).resolve().parent
OUT = HERE / "out"

BASE = 0.50  # (1 - blend) with blend = 0.5


def variants() -> dict[str, Config]:
    return {
        "Full (calm + oversold + override)": Config(
            calm_q=0.70,
            rev_entry=0.25,
            blend=0.50,
            override_thr=0.15,
            override_scale=0.50,
        ),
        "No oversold boost": Config(
            calm_q=0.70,
            rev_entry=None,
            blend=0.50,
            override_thr=0.15,
            override_scale=0.50,
        ),
        "No market override": Config(
            calm_q=0.70, rev_entry=0.25, blend=0.50, override_thr=None
        ),
        "No calm-vol gate": Config(
            calm_q=None,
            rev_entry=0.25,
            blend=0.50,
            override_thr=0.15,
            override_scale=0.50,
        ),
        "Calm gate only": Config(
            calm_q=0.70, rev_entry=None, blend=0.50, override_thr=None
        ),
        "Oversold only": Config(
            calm_q=None, rev_entry=0.25, blend=1.0, override_thr=None
        ),
        "Override only": Config(
            calm_q=None,
            rev_entry=None,
            blend=0.0,
            override_thr=0.15,
            override_scale=0.50,
        ),
        "Full + vol cap 55%": Config(
            calm_q=0.70,
            rev_entry=0.25,
            blend=0.50,
            override_thr=0.15,
            override_scale=0.50,
            vol_target=0.55,
        ),
    }


def main() -> int:
    df, index_frame = load()
    builder = ExposureBuilder(df, index_frame)
    rows = []
    for name, cfg in variants().items():
        expo = builder.build(cfg)
        rec = {"variant": name, "avg_exposure": float(expo.mean())}
        for label, sl in (
            ("is", df.loc[:IS_END]),
            ("oos", df.loc[OOS_START:]),
            ("full", df),
        ):
            m = simulate(sl, expo.reindex(sl.index), band=0.05).metrics
            rec[f"cagr_{label}"] = m["cagr"]
            rec[f"mdd_{label}"] = m["max_drawdown"]
            rec[f"calmar_{label}"] = m["calmar"]
            rec[f"sharpe_{label}"] = m["sharpe"]
        rows.append(rec)
    table = pd.DataFrame(rows)
    full = table.iloc[0]
    for label in ("full", "is", "oos"):
        table[f"dcagr_{label}"] = table[f"cagr_{label}"] - full[f"cagr_{label}"]
        table[f"dmdd_{label}"] = table[f"mdd_{label}"] - full[f"mdd_{label}"]
    table.to_csv(OUT / "ablation.csv", index=False)

    pd.set_option("display.width", 240)
    cols = [
        "variant",
        "avg_exposure",
        "cagr_full",
        "mdd_full",
        "calmar_full",
        "cagr_is",
        "mdd_is",
        "cagr_oos",
        "mdd_oos",
        "calmar_oos",
    ]
    print(table[cols].to_string(index=False, float_format=lambda v: f"{v:.3f}"))
    print("\nContribution vs full (full = 0):")
    cols2 = ["variant", "dcagr_full", "dmdd_full", "dcagr_is", "dcagr_oos", "dmdd_oos"]
    print(table[cols2].to_string(index=False, float_format=lambda v: f"{v:+.3f}"))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
