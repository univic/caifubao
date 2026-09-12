# -*- coding: utf-8 -*-
"""Target-portfolio export for the paper-first strategy runner (roadmap 2.1).

Pure projection of ONE persisted paper run into a human-checkable "target
holdings + rebalance list": which names are added (BUY), removed (SELL), or
unchanged (HOLD), with target weight, target amount, and the score that put them
there. No Mongo, no writes, no orders — the runner layer maps documents onto
these shapes, exactly like selection.py / nav.py.

Every export stays research/observation-grade. No model version has passed the
roadmap 0.3 promotion gate (>=120 immutable forward paper sessions plus a
separate promotion decision), so an export MUST NOT present its output as
tradable advice — see production-capability-roadmap invariants.
"""

from __future__ import annotations

import csv
import datetime
import io

# Fixed labels — not configurable: a caller must not be able to render an
# unpromoted model as tradable/actionable.
EXPORT_GRADE = "RESEARCH"
EXPORT_DISCLAIMER = (
    "Research / learning / demonstration MVP. Not investment advice. "
    "Paper-only: no real orders are generated or submitted."
)

SIDE_ORDER = {"BUY": 0, "SELL": 1, "HOLD": 2}
_REASON = {
    "BUY": "added_to_target",
    "SELL": "removed_from_target",
    "HOLD": "unchanged_in_target",
}
CSV_COLUMNS = [
    "side",
    "stock_code",
    "stock_name",
    "score",
    "percentile",
    "target_weight",
    "target_amount_cny",
    "reason",
]


def _date_text(value) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime.datetime):
        return value.date().isoformat()
    if isinstance(value, datetime.date):
        return value.isoformat()
    return str(value)


def _instant_text(value) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime.datetime):
        return value.isoformat()
    return str(value)


def _positive_number(value):
    """Return float(value) when it is a usable positive number, else None."""
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    if value <= 0:
        return None
    return float(value)


def resolve_base_nav(run: dict, override=None) -> tuple[float, str]:
    """Return ``(base_nav, source)`` for the run, or raise when none exists.

    Order: an explicit override, then the run's persisted NAV snapshot, then the
    configured ``initial_nav``. Amounts in one export must all come from this
    single value, and a run with no usable NAV fails closed rather than
    inventing a budget.
    """
    explicit = _positive_number(override)
    if explicit is not None:
        return explicit, "explicit"
    from_snapshot = _positive_number((run.get("nav_snapshot") or {}).get("nav"))
    if from_snapshot is not None:
        return from_snapshot, "nav_snapshot"
    from_config = _positive_number((run.get("config") or {}).get("initial_nav"))
    if from_config is not None:
        return from_config, "config.initial_nav"
    raise ValueError(
        "no base NAV available for export: pass base_nav, or provide a NAV "
        "snapshot or config.initial_nav on the run"
    )


def build_target_export(
    *,
    run: dict,
    scores: dict[str, dict] | None = None,
    names: dict[str, str] | None = None,
    base_nav=None,
    generated_at: datetime.datetime | None = None,
) -> dict:
    """Build the export payload for one COMPLETED paper run.

    ``run`` is a plain mapping of the persisted run (strategy_name,
    model_version, horizon, config_hash, date, execution_date, decision_at,
    evidence_kind, status, target_holdings, rebalance, nav_snapshot, config).
    ``scores``/``names`` are optional ``stock_code -> value`` lookups joined for
    human readability; a missing entry stays null, never invented.

    Returns metadata plus deterministically ordered ``rows`` (BUY, then SELL,
    then HOLD; each group sorted by stock code). Raises ValueError when the run
    is not COMPLETED, when a BUY/HOLD name has no target weight, or when no base
    NAV can be resolved.
    """
    status = run.get("status")
    if status != "COMPLETED":
        raise ValueError(
            f"cannot export a {status!r} strategy run; only COMPLETED runs are "
            "exportable (SKIPPED/FAILED/RUNNING carry no target portfolio)"
        )

    nav, nav_source = resolve_base_nav(run, base_nav)
    weights = {
        holding.get("stock_code"): holding.get("weight")
        for holding in (run.get("target_holdings") or [])
        if holding.get("stock_code")
    }
    score_map = scores or {}
    name_map = names or {}
    rebalance = run.get("rebalance") or {}

    def _make_row(side: str, code: str) -> dict:
        weight = None
        amount = None
        if side in ("BUY", "HOLD"):
            raw_weight = weights.get(code)
            if isinstance(raw_weight, bool) or not isinstance(raw_weight, (int, float)):
                raise ValueError(
                    f"{side} code {code} has no target weight in target_holdings"
                )
            weight = float(raw_weight)
            amount = round(weight * nav, 2)
        score_row = score_map.get(code) or {}
        return {
            "side": side,
            "stock_code": code,
            "stock_name": name_map.get(code),
            "score": score_row.get("score"),
            "percentile": score_row.get("percentile"),
            "target_weight": weight,
            "target_amount_cny": amount,
            "reason": _REASON[side],
        }

    rows = []
    for code in sorted(set(rebalance.get("added") or [])):
        rows.append(_make_row("BUY", code))
    for code in sorted(set(rebalance.get("removed") or [])):
        rows.append(_make_row("SELL", code))
    for code in sorted(set(rebalance.get("unchanged") or [])):
        rows.append(_make_row("HOLD", code))
    rows.sort(key=lambda row: (SIDE_ORDER[row["side"]], row["stock_code"]))

    now = generated_at or datetime.datetime.now(datetime.UTC)
    return {
        "strategy_name": run.get("strategy_name"),
        "model_version": run.get("model_version"),
        "horizon": run.get("horizon"),
        "config_hash": run.get("config_hash"),
        "date": _date_text(run.get("date")),
        "execution_date": _instant_text(run.get("execution_date")),
        "decision_at": _instant_text(run.get("decision_at")),
        "evidence_kind": run.get("evidence_kind") or "REPLAY",
        "status": status,
        "base_nav": nav,
        "base_nav_source": nav_source,
        "grade": EXPORT_GRADE,
        "disclaimer": EXPORT_DISCLAIMER,
        "generated_at": _instant_text(now),
        "counts": {
            "buy": sum(1 for row in rows if row["side"] == "BUY"),
            "sell": sum(1 for row in rows if row["side"] == "SELL"),
            "hold": sum(1 for row in rows if row["side"] == "HOLD"),
            "total": len(rows),
        },
        "rows": rows,
    }


def select_export_run(runs: list[dict], *, config_hash: str | None = None) -> dict:
    """Select the one exportable run view from candidates for a single key.

    ``runs`` are run views already narrowed to one (date, model_version,
    horizon). When ``config_hash`` is given the candidates are scoped to that
    configuration first, so a request can never fall back to an unrelated
    configuration's run. Among scoped candidates COMPLETED runs win, and more
    than one distinct COMPLETED config hash is ambiguous and fails closed. With
    no COMPLETED candidate the most recent scoped run is returned, so the
    caller's status check reports its actual status.
    """
    scoped = [
        run
        for run in runs
        if config_hash is None or run.get("config_hash") == config_hash
    ]
    if not scoped:
        raise ValueError("no completed paper run to export for this key")
    completed = [run for run in scoped if run.get("status") == "COMPLETED"]
    if completed:
        hashes = {run.get("config_hash") for run in completed}
        if len(hashes) > 1:
            raise ValueError(
                "ambiguous export: multiple COMPLETED runs match under different "
                f"config hashes {sorted(hashes)}; name the configuration to "
                "disambiguate"
            )
        return completed[0]
    return scoped[0]


def render_csv(export: dict) -> str:
    """Render the export as CSV, led by the compliance comment line.

    The label and disclaimer live in the artifact itself (a leading ``#``
    comment) so a copied file cannot lose its compliance context; the runner
    layer additionally prints the metadata block to stderr.
    """
    buffer = io.StringIO()
    buffer.write(
        f"# grade={export.get('grade')} config_hash={export.get('config_hash')} "
        f"evidence_kind={export.get('evidence_kind')} | {export.get('disclaimer')}\n"
    )
    writer = csv.DictWriter(
        buffer, fieldnames=CSV_COLUMNS, extrasaction="ignore", lineterminator="\n"
    )
    writer.writeheader()
    for row in export.get("rows") or []:
        writer.writerow(
            {key: ("" if row.get(key) is None else row.get(key)) for key in CSV_COLUMNS}
        )
    return buffer.getvalue()
