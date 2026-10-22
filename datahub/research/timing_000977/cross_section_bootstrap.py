# -*- coding: utf-8 -*-
"""Block-bootstrap confidence intervals for the cross-sectional paired effects."""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd

HERE = Path(__file__).resolve().parent
OUT = HERE / "out"


def main() -> int:
    table = pd.read_csv(OUT / "cross_section.csv")
    rng = np.random.default_rng(7)
    rows = []
    for (rule, period), group in table.groupby(["rule", "period"]):
        x = group.d_calmar.dropna().to_numpy()
        boots = np.array(
            [rng.choice(x, len(x), replace=True).mean() for _ in range(2000)]
        )
        rows.append(
            {
                "rule": rule,
                "period": period,
                "n": len(x),
                "mean_d_calmar": float(x.mean()),
                "lo": float(np.percentile(boots, 2.5)),
                "hi": float(np.percentile(boots, 97.5)),
                "mean_d_cagr": float(group.d_cagr.mean()),
                "mean_d_mdd": float(group.d_mdd.mean()),
                "win_calmar": float((x > 0).mean()),
            }
        )
    pd.DataFrame(rows).to_csv(OUT / "cross_section_bootstrap.csv", index=False)
    print(f"wrote {len(rows)} rows")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
