"""Offline snapshot evidence audit; never computes portfolio NAV or promotes candidates."""

import argparse
import hashlib
import json
from pathlib import Path

import numpy as np
import pandas as pd

KEYS = ["stock_code", "date"]
COLUMNS = [
    *KEYS,
    "open_hfq",
    "close_hfq",
    "eligibility",
    "actual_entry_date",
    "actual_exit_date",
    "actual_entry_open_hfq",
    "actual_exit_open_hfq",
]


def normalize(frame):
    frame = frame.copy()
    for name in ["date", "actual_entry_date", "actual_exit_date"]:
        if name in frame:
            frame[name] = pd.to_datetime(frame[name], utc=True, errors="raise")
    if frame[KEYS].isna().any().any():
        raise ValueError("null stock/date key")
    return frame


def usable(values):
    return np.isfinite(values) & values.gt(0)


def price_counts(left, right):
    valid = usable(left) & usable(right)
    return {
        "usable_pairs": int(valid.sum()),
        "unusable_pairs": int((~valid).sum()),
        "price_mismatches": int(
            (valid & ~np.isclose(left, right, rtol=1e-9, atol=1e-9)).sum()
        ),
    }


def compare_quotes(snapshot, reference):
    snapshot, reference = normalize(snapshot), normalize(reference)
    if any(f.duplicated(KEYS).any() for f in (snapshot, reference)):
        raise ValueError("duplicate quote keys prevent comparison")
    joined = reference.merge(
        snapshot[KEYS + ["open_hfq", "close_hfq"]],
        on=KEYS,
        how="left",
        suffixes=("_reference", "_snapshot"),
        indicator=True,
        validate="one_to_one",
    )
    matched = joined[joined["_merge"].eq("both")]
    return {
        "reference_rows": len(reference),
        "matched": len(matched),
        "unmatched": len(joined) - len(matched),
        "scope": "provided reference keys only; not full-universe certification",
        "fields": {
            c: price_counts(matched[c + "_reference"], matched[c + "_snapshot"])
            for c in ["open_hfq", "close_hfq"]
        },
    }


def compare_snapshots(snapshot, other):
    snapshot, other = normalize(snapshot[COLUMNS]), normalize(other[COLUMNS])
    if any(f.duplicated(KEYS).any() for f in (snapshot, other)):
        raise ValueError("duplicate snapshot keys prevent comparison")
    joined = snapshot.merge(
        other, on=KEYS, how="outer", suffixes=("_a", "_b"), indicator=True
    )
    both = joined[joined["_merge"].eq("both")]
    mismatches = {}
    for col in set(COLUMNS) - set(KEYS):
        a, b = both[col + "_a"], both[col + "_b"]
        mismatches[col] = int((~(a.eq(b) | (a.isna() & b.isna()))).sum())
    return {
        "matched": len(both),
        "snapshot_only": int(joined["_merge"].eq("left_only").sum()),
        "comparison_only": int(joined["_merge"].eq("right_only").sum()),
        "exact_field_mismatches": mismatches,
    }


def audit(frame, *, from_date=None, to_date=None, rate_cost=0.0035):
    frame = normalize(frame[COLUMNS])
    if not np.isfinite(rate_cost) or rate_cost < 0:
        raise ValueError("rate_cost must be finite and nonnegative")
    if (
        frame.eligibility.isna().any()
        or not frame.eligibility.isin([True, False]).all()
    ):
        raise ValueError("eligibility must contain booleans")
    valid = usable(frame.actual_entry_open_hfq) & usable(frame.actual_exit_open_hfq)
    chronology = (frame.date < frame.actual_entry_date) & (
        frame.actual_entry_date < frame.actual_exit_date
    )
    eligible = frame.eligibility.astype(bool)
    duplicates = int(frame.duplicated(KEYS, keep=False).sum())
    result = {
        "rows": len(frame),
        "duplicate_key_rows": duplicates,
        "eligible_unusable_execution_prices": int((eligible & ~valid).sum()),
        "eligible_invalid_chronology": int((eligible & ~chronology).sum()),
        "unusable_quote_prices": {
            c: int((~usable(frame[c])).sum()) for c in ["open_hfq", "close_hfq"]
        },
        "labels": None,
        "returns": None,
    }
    if duplicates:
        return result
    labels = {}
    for side in ["entry", "exit"]:
        date_col, price_col = f"actual_{side}_date", f"actual_{side}_open_hfq"
        present = frame[frame[date_col].notna()]
        joined = present[["stock_code", date_col, price_col]].merge(
            frame[KEYS + ["open_hfq"]],
            left_on=["stock_code", date_col],
            right_on=KEYS,
            how="left",
            indicator=True,
            validate="many_to_one",
        )
        matched = joined[joined["_merge"].eq("both")]
        labels[side] = {
            "missing_label_date": len(frame) - len(present),
            "matched": len(matched),
            "unmatched": len(present) - len(matched),
            **price_counts(matched[price_col], matched.open_hfq),
        }
    selected = eligible & valid & chronology
    if from_date:
        selected &= frame.date >= pd.to_datetime(from_date, utc=True)
    if to_date:
        selected &= frame.date <= pd.to_datetime(to_date, utc=True)
    rows = frame[selected]
    returns = rows.actual_exit_open_hfq / rows.actual_entry_open_hfq - 1 - rate_cost
    cohorts = returns.groupby(rows.date).mean()
    duration = (
        rows.actual_exit_date - rows.actual_entry_date
    ).dt.total_seconds() / 86400
    result.update(
        labels=labels,
        returns={
            "unit": "nominal H20 holding-period cohort return",
            "aggregation": "equal stock mean within signal date, then equal mean across signal dates",
            "horizon_convention": "20 per-stock quote observations after entry plus tradability roll-forward",
            "rate_cost": rate_cost,
            "cost_convention": "2*commission + sell duty + 2*slippage",
            "from_date": from_date,
            "to_date": to_date,
            "rows": len(rows),
            "signal_dates": len(cohorts),
            "mean_holding_period_cohort_return": float(cohorts.mean())
            if len(cohorts)
            else None,
            "holding_calendar_days": {
                name: float(duration.quantile(q)) if len(rows) else None
                for name, q in [("min", 0), ("median", 0.5), ("max", 1)]
            },
        },
    )
    return result


def identity(path):
    with path.open("rb") as stream:
        checksum = hashlib.file_digest(stream, "sha256").hexdigest()
    return {"name": path.name, "sha256": checksum}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--snapshot", type=Path, required=True)
    parser.add_argument("--reference-quotes", type=Path)
    parser.add_argument("--comparison-snapshot", type=Path)
    parser.add_argument("--from-date")
    parser.add_argument("--to-date")
    parser.add_argument("--rate-cost", type=float, default=0.0035)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args(argv)
    # Exclusive creation below also guards existing inputs, symlinks and hard links.
    if args.output and (args.output.exists() or args.output.is_symlink()):
        raise ValueError("output already exists; choose a new audit artifact")
    frame = pd.read_parquet(args.snapshot, columns=COLUMNS)
    result = audit(
        frame, from_date=args.from_date, to_date=args.to_date, rate_cost=args.rate_cost
    )
    result["snapshot"] = identity(args.snapshot)
    result["inspected_columns"] = COLUMNS
    result["price_tolerance"] = {"rtol": 1e-9, "atol": 1e-9}
    if args.reference_quotes:
        reference = pd.DataFrame(json.loads(args.reference_quotes.read_text()))
        result["reference_comparison"] = compare_quotes(frame, reference)
        result["reference"] = identity(args.reference_quotes)
    if args.comparison_snapshot:
        other = pd.read_parquet(args.comparison_snapshot, columns=COLUMNS)
        result["snapshot_comparison"] = compare_snapshots(frame, other)
        result["comparison_snapshot"] = identity(args.comparison_snapshot)
    encoded = json.dumps(result, indent=2, allow_nan=False) + "\n"
    if args.output:
        with args.output.open("x") as stream:
            stream.write(encoded)
    else:
        print(encoded, end="")
    return result


if __name__ == "__main__":
    main()
