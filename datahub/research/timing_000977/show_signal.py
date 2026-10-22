# -*- coding: utf-8 -*-
"""Print today's target exposure for the 000977 timing rule.

Reads the cached snapshot (run ``fetch_data.py`` first, or pass ``--refresh``)
and evaluates the four gates on the latest close.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path


from scan import load
from strategy import FINAL as BALANCED
from strategy import ExposureBuilder

HERE = Path(__file__).resolve().parent


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--refresh", action="store_true", help="re-fetch before evaluating"
    )
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()
    if args.refresh:
        import fetch_data

        fetch_data.main()

    df, index_frame = load()
    builder = ExposureBuilder(df, index_frame)
    expo = builder.build(BALANCED)
    last = df.index[-1]
    payload = {
        "date": str(last.date()),
        "close": round(float(df["close"].iloc[-1]), 2),
        "ma200": round(float(df["ma200"].iloc[-1]), 2),
        "ret120": round(float(df["close"].pct_change(120).iloc[-1]), 4),
        "vol60": round(float(df["vol60"].iloc[-1]), 4),
        "calm_gate": int(builder.calm(60, 500, 0.70).iloc[-1]),
        "reversal_gate": int(builder.rev(120, 0.25, 0.05).iloc[-1]),
        "csi_drawdown_250": round(float(builder.csi_dd250.iloc[-1]), 4),
        "market_override": float(builder.override(0.15, 0.50).iloc[-1]),
        "target_exposure": round(float(expo.iloc[-1]), 4),
        "config": BALANCED.label,
    }
    if args.json:
        print(json.dumps(payload, indent=2, ensure_ascii=False))
        return 0
    print(f"000977 浪潮信息  {payload['date']}")
    print(f"  close              {payload['close']:.2f}   MA200 {payload['ma200']:.2f}")
    print(f"  120d return        {payload['ret120']:+.2%}")
    print(
        f"  60d vol            {payload['vol60']:.1%}   calm gate {payload['calm_gate']}"
    )
    print(f"  reversal gate      {payload['reversal_gate']}  (entry -25% / exit +5%)")
    print(
        f"  CSI300 dd from high{payload['csi_drawdown_250']:+.1%}   market override {payload['market_override']:.2f}"
    )
    print(f"  => TARGET EXPOSURE {payload['target_exposure']:.0%}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
