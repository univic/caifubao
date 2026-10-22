# -*- coding: utf-8 -*-
"""Cross-check the frozen results without re-running the full research suite.

Checks:

1. every SHA256 in ``SHA256SUMS`` / ``SHA256SUMS.data``;
2. the final strategy metrics against ``out/headline.json`` (deterministic);
3. the walk-forward exposure path against the benchmark table's timing row;
4. the cross-sectional mean paired effects against ``out/cross_section_bootstrap.csv``.

Exit code 0 only if every check passes.
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

import numpy as np
import pandas as pd

from engine import simulate
from scan import load
from strategy import FINAL, ExposureBuilder

HERE = Path(__file__).resolve().parent
TOL = 1e-6
results: list[tuple[str, bool, str]] = []


def check(label: str, ok: bool, detail: str = "") -> None:
    results.append((label, bool(ok), detail))


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def check_sums(name: str) -> None:
    path = HERE / name
    if not path.exists():
        check(f"checksums:{name}", False, "missing")
        return
    bad = 0
    total = 0
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        digest, rel = line.split("  ", 1)
        total += 1
        target = HERE / rel
        if not target.exists() or sha256(target) != digest:
            bad += 1
    check(f"checksums:{name}", bad == 0, f"{total - bad}/{total} ok")


def main() -> int:
    check_sums("SHA256SUMS")
    check_sums("SHA256SUMS.data")

    headline = json.loads((HERE / "out" / "headline.json").read_text(encoding="utf-8"))
    df, index_frame = load()
    expo = ExposureBuilder(df, index_frame).build(FINAL)
    for key, (start, end) in {
        "full": ("1900-01-01", "2100-12-31"),
        "sd_is": ("1900-01-01", "2016-12-31"),
        "sd_oos": ("2017-01-01", "2100-12-31"),
    }.items():
        sl = df.loc[start:end]
        m = simulate(sl, expo.reindex(sl.index), band=0.05).metrics
        for metric in ("cagr", "max_drawdown", "calmar"):
            expected = headline[key][metric]
            check(
                f"strategy:{key}:{metric}",
                abs(m[metric] - expected) <= TOL,
                f"recomputed={m[metric]:.8f} manifest={expected:.8f}",
            )

    wf = pd.read_csv(
        HERE / "out" / "wf_final_exposure.csv", index_col=0, parse_dates=True
    )["exposure"]
    sub = df.loc[wf.index]
    wfm = simulate(sub, wf.reindex(sub.index).fillna(0.0), band=0.05).metrics
    bench = pd.read_csv(HERE / "out" / "benchmark_oos.csv").set_index("name")
    row = bench.loc["Timing (walk-forward)"]
    for metric in ("cagr", "max_drawdown", "calmar"):
        check(
            f"benchmark:timing:{metric}",
            abs(wfm[metric] - float(row[metric])) <= TOL,
            f"recomputed={wfm[metric]:.8f} table={float(row[metric]):.8f}",
        )

    cs = pd.read_csv(HERE / "out" / "cross_section.csv")
    boot = pd.read_csv(HERE / "out" / "cross_section_bootstrap.csv")
    worst = 0.0
    for _, row in boot.iterrows():
        x = (
            cs[(cs.rule == row.rule) & (cs.period == row.period)]
            .d_calmar.dropna()
            .to_numpy()
        )
        worst = max(worst, abs(float(np.mean(x)) - float(row.mean_d_calmar)))
    check("cross_section:mean_d_calmar", worst <= TOL, f"max abs diff={worst:.2e}")

    print(f"{'check':42s}  result  detail")
    for label, ok, detail in results:
        print(f"{label:42s}  {'PASS' if ok else 'FAIL':6s}  {detail}")
    failed = [r for r in results if not r[1]]
    print(f"\n{len(results) - len(failed)}/{len(results)} checks passed")
    return 0 if not failed else 1


if __name__ == "__main__":
    raise SystemExit(main())
