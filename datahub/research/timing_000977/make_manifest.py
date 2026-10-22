# -*- coding: utf-8 -*-
"""Freeze the current research outputs into a cross-checkable manifest.

Writes:

* ``RESULTS_MANIFEST.md`` — environment, data snapshot, headline numbers, file
  inventory with SHA256, and the exact commands a reviewer should run;
* ``SHA256SUMS`` — machine-readable checksums of documents, code, figures and
  result tables;
* ``SHA256SUMS.data`` — checksums of the cached price data.
"""

from __future__ import annotations

import hashlib
import json
import subprocess
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

HERE = Path(__file__).resolve().parent
ROOT = HERE
EXCLUDE_DIRS = {"__pycache__", ".pytest_cache"}
EXCLUDE_NAMES = {"RESULTS_MANIFEST.md", "SHA256SUMS", "SHA256SUMS.data"}
DATA_SUFFIXES = {".parquet"}


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def collect() -> tuple[list[Path], list[Path]]:
    docs, data = [], []
    for path in sorted(ROOT.rglob("*")):
        if not path.is_file():
            continue
        if any(part in EXCLUDE_DIRS for part in path.parts):
            continue
        if path.name in EXCLUDE_NAMES:
            continue
        if path.suffix in DATA_SUFFIXES:
            data.append(path)
        elif path.suffix in {".py", ".md", ".png", ".csv", ".json"}:
            docs.append(path)
    return docs, data


def fmt_pct(x: float) -> str:
    return f"{x:.2%}"


def table(rows: list[list[str]], header: list[str]) -> str:
    out = [
        "| " + " | ".join(header) + " |",
        "|" + "|".join(["---"] * len(header)) + "|",
    ]
    out += ["| " + " | ".join(str(c) for c in row) + " |" for row in rows]
    return "\n".join(out)


def git(*args: str) -> str:
    try:
        return subprocess.check_output(
            ["git", *args], cwd=ROOT, text=True, stderr=subprocess.DEVNULL
        ).strip()
    except Exception:  # noqa: BLE001
        return "n/a"


def main() -> int:
    docs, data = collect()
    manifest = json.loads((ROOT / "data" / "manifest.json").read_text(encoding="utf-8"))
    headline = json.loads((ROOT / "out" / "headline.json").read_text(encoding="utf-8"))
    wf = json.loads(
        (ROOT / "out" / "wf_final_summary.json").read_text(encoding="utf-8")
    )
    bench = pd.read_csv(ROOT / "out" / "benchmark_oos.csv")
    risk = pd.read_csv(ROOT / "out" / "risk_overlays.csv")
    ablation = pd.read_csv(ROOT / "out" / "ablation.csv")
    cs = pd.read_csv(ROOT / "out" / "cross_section_bootstrap.csv")
    rot = pd.read_csv(ROOT / "out" / "sector_rotation.csv")
    rc = json.loads(
        (ROOT / "out" / "alpha_reality_check.json").read_text(encoding="utf-8")
    )
    ic20 = pd.read_csv(ROOT / "out" / "alpha_ic20.csv")
    oos_reg = pd.read_csv(ROOT / "out" / "alpha_oos_regression.csv")

    lines: list[str] = []
    lines.append("# 浪潮信息择时研究 — 结果快照与交叉检查清单\n")
    lines.append(
        f"生成时间（UTC）：`{datetime.now(timezone.utc).isoformat(timespec='seconds')}`\n"
    )
    lines.append(
        f"节点：`{subprocess.check_output(['hostname'], text=True).strip()}`　｜　目录：`{ROOT}`\n"
    )
    lines.append(
        f"Git HEAD：`{git('rev-parse', 'HEAD')}`　｜　分支：`{git('rev-parse', '--abbrev-ref', 'HEAD')}`\n"
    )
    lines.append(
        f"工作区状态：`{git('status', '--short', '--', str(ROOT)) or '（本目录为未跟踪文件）'}`\n"
    )
    lines.append(
        "\n> 本文件由 `make_manifest.py` 生成，用于冻结已有输出。`verify_results.py` 只校验：全文件哈希、固定策略三段×三指标、已保存 walk-forward 仓位路径、以及跨股票均值；它**不**重跑年度选模、公司行动审计、bootstrap 区间、预测力或行业实验。\n"
    )

    lines.append("\n## 1. 数据快照\n")
    rows = []
    for name, meta in manifest.items():
        rows.append(
            [
                name,
                meta.get("fqt"),
                meta.get("rows"),
                meta.get("start"),
                meta.get("end"),
                meta.get("fetched_at"),
            ]
        )
    lines.append(table(rows, ["数据集", "复权", "行数", "起始", "结束", "抓取时间"]))
    lines.append("\n重建序列 `000977_adj` 的 5 个送转事件：\n")
    ev = manifest.get("000977_adj", {}).get("events", [])
    lines.append(
        table([[e["ex"], e["m"], e["note"]] for e in ev], ["除权日", "倍数", "依据"])
    )

    lines.append("\n## 2. 核心结果\n")
    rows = []
    for key, label in (
        ("full", "全样本 2005-2026"),
        ("sd_is", "样本内 2005-2016"),
        ("sd_oos", "样本外 2017-2026"),
        ("from_2011", "2011 起"),
        ("buy_hold", "买入持有"),
    ):
        m = headline[key]
        rows.append(
            [
                label,
                fmt_pct(m["cagr"]),
                fmt_pct(m["max_drawdown"]),
                f"{m['calmar']:.2f}",
                f"{m['sharpe']:.2f}",
                fmt_pct(m["avg_exposure"]),
            ]
        )
    lines.append(table(rows, ["区间", "CAGR", "MaxDD", "Calmar", "Sharpe", "平均仓位"]))
    lines.append("\n滚动前进（2011-2026，固定结构族内每年重选）：\n")
    rows = []
    for key in ("top1", "top3", "top5"):
        m = wf[key]["metrics"]
        rows.append(
            [
                key,
                fmt_pct(m["cagr"]),
                fmt_pct(m["max_drawdown"]),
                f"{m['calmar']:.2f}",
                f"{m['sharpe']:.2f}",
            ]
        )
    lines.append(table(rows, ["集成档位", "CAGR", "MaxDD", "Calmar", "Sharpe"]))

    lines.append("\n## 3. 统一基准（2011-01-04 起，同一引擎）\n")
    rows = []
    for _, r in bench.iterrows():
        rows.append(
            [
                r["name"],
                fmt_pct(r["cagr"]),
                fmt_pct(r["max_drawdown"]),
                f"{r['calmar']:.3f}",
                f"{r['sharpe']:.3f}",
                fmt_pct(r["avg_exposure"]),
            ]
        )
    lines.append(table(rows, ["组合", "CAGR", "MaxDD", "Calmar", "Sharpe", "平均仓位"]))

    lines.append("\n## 4. 风险覆盖层（节选）\n")
    picks = [
        ("bh", "none"),
        ("bh", "vol_regime70+csi_dd20_x0"),
        ("bh", "combo+vt30"),
        ("core", "none"),
        ("core", "csi_dd20_x0"),
        ("bh", "cppi_75_2"),
        ("bh", "dd_soft_10_60"),
        ("bh", "eqma_150"),
    ]
    rows = []
    for base, name in picks:
        sub = risk[(risk.base == base) & (risk.overlay == name)]
        if sub.empty:
            continue
        r = sub.iloc[0]
        rows.append(
            [
                base,
                name,
                fmt_pct(r["cagr"]),
                fmt_pct(r["max_drawdown"]),
                f"{r['calmar']:.3f}",
                fmt_pct(r["worst_year"]),
                f"{r['ulcer']:.3f}",
            ]
        )
    lines.append(
        table(rows, ["基底", "覆盖层", "CAGR", "MaxDD", "Calmar", "最差年", "Ulcer"])
    )

    lines.append("\n## 5. 消融（全样本）\n")
    rows = []
    for _, r in ablation.iterrows():
        rows.append(
            [
                r["variant"],
                fmt_pct(r["avg_exposure"]),
                fmt_pct(r["cagr_full"]),
                fmt_pct(r["mdd_full"]),
                f"{r['calmar_full']:.3f}",
            ]
        )
    lines.append(table(rows, ["变体", "平均仓位", "CAGR", "MaxDD", "Calmar"]))

    lines.append("\n## 6. 跨股票配对检验（94 只）\n")
    rows = []
    for _, r in cs.sort_values(
        ["period", "mean_d_calmar"], ascending=[True, False]
    ).iterrows():
        rows.append(
            [
                r["period"],
                r["rule"],
                r["n"],
                f"{r['mean_d_calmar']:+.4f}",
                f"[{r['lo']:+.4f}, {r['hi']:+.4f}]",
                f"{r['mean_d_cagr'] * 100:+.2f}pp",
                f"{r['mean_d_mdd'] * 100:+.2f}pp",
                f"{r['win_calmar']:.1%}",
            ]
        )
    lines.append(
        table(
            rows,
            ["区间", "规则", "n", "ΔCalmar 均值", "95% CI", "ΔCAGR", "ΔMaxDD", "胜率"],
        )
    )

    lines.append("\n## 7. 行业层\n")
    rows = []
    for _, r in rot[rot.period == "full"].iterrows():
        is_row = rot[(rot.strategy == r["strategy"]) & (rot.period == "is")]
        oos_row = rot[(rot.strategy == r["strategy"]) & (rot.period == "oos")]
        rows.append(
            [
                r["strategy"],
                fmt_pct(r["cagr"]),
                fmt_pct(r["mdd"]),
                f"{r['calmar']:.3f}",
                f"{is_row['calmar'].iloc[0]:.3f}" if len(is_row) else "n/a",
                f"{oos_row['calmar'].iloc[0]:.3f}" if len(oos_row) else "n/a",
            ]
        )
    lines.append(
        table(
            rows,
            [
                "策略",
                "全样本 CAGR",
                "全样本 MaxDD",
                "Calmar",
                "IS Calmar",
                "OOS Calmar",
            ],
        )
    )

    lines.append("\n## 8. 预测力\n")
    rows = []
    for _, r in ic20.iterrows():
        rows.append(
            [
                r["signal"],
                f"{r['ic']:+.3f}",
                f"[{r['ci_lo']:+.3f}, {r['ci_hi']:+.3f}]",
                int(r["same_sign_eras"]),
            ]
        )
    lines.append(table(rows, ["信号", "IC(20d)", "95% CI", "同号段数"]))
    lines.append(
        f"\nReality Check：{rc['signals']} 个信号，max|t| = {rc['observed_max_abs_t']:.2f}，bootstrap p = {rc['bootstrap_p']:.2f}。\n"
    )
    lines.append("\nOOS 回归（20d）：\n")
    rows = [
        [r["model"], f"{r['oos_r2']:.4f}", f"{r['hit_rate']:.3f}"]
        for _, r in oos_reg.iterrows()
    ]
    lines.append(table(rows, ["模型", "OOS R²", "命中率"]))

    lines.append("\n## 9. 文件清单与校验\n")
    lines.append(
        "校验方式：`sha256sum -c SHA256SUMS`（文档/代码/图表/结果表）与 `sha256sum -c SHA256SUMS.data`（行情缓存）。\n"
    )
    lines.append(f"文档与结果文件：{len(docs)} 个；行情数据文件：{len(data)} 个。\n")
    lines.append("\n### 文档、代码、图表与结果\n")
    rows = [
        [str(p.relative_to(ROOT)), f"{p.stat().st_size:,}", sha256(p)[:16] + "…"]
        for p in docs
    ]
    lines.append(table(rows, ["文件", "字节", "SHA256 前缀"]))

    lines.append("\n## 10. 交叉检查步骤\n")
    lines.append(
        "```bash\ncd datahub/research/timing_000977\nsha256sum -c SHA256SUMS && sha256sum -c SHA256SUMS.data\n../../.venv/bin/python verify_results.py\n../../.venv/bin/python build_adjusted.py\n../../.venv/bin/python report.py\n../../.venv/bin/python cross_section.py\n../../.venv/bin/python risk_overlay.py\n```\n"
    )
    lines.append(
        "\n`verify_results.py` 会重算最终策略、基准与跨股票汇总，并与本清单中的文件逐项比对，输出 PASS/FAIL。\n"
    )

    lines.append("\n## 11. 已知局限\n")
    for item in (
        "单票样本（000977）约 5 个牛熊周期；跨股票样本 94 只且为当前上市股票（幸存者偏差）。",
        "重建价格序列有 3 个送转倍数为推断值（2006/2017/2020），现金分红未计入主序列。",
        "规则参数来自 000977 样本内研究，跨股票检验未重新拟合，但存在设计偏差。",
        "事件研究与 IC 使用重叠窗口，有效样本小于名义样本。",
        "未建模涨跌停无法成交、停牌、冲击成本、融券。",
    ):
        lines.append(f"- {item}\n")

    (ROOT / "RESULTS_MANIFEST.md").write_text("".join(lines), encoding="utf-8")

    with (ROOT / "SHA256SUMS").open("w", encoding="utf-8") as fh:
        for p in docs:
            fh.write(f"{sha256(p)}  {p.relative_to(ROOT)}\n")
    with (ROOT / "SHA256SUMS.data").open("w", encoding="utf-8") as fh:
        for p in data:
            fh.write(f"{sha256(p)}  {p.relative_to(ROOT)}\n")
    print(f"docs={len(docs)} data={len(data)} manifest written")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
