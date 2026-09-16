"""Full-sample decile analysis for ranked_v1_h20 scores on dev MongoDB.

Runs inside the caifubao-datahub pod (env vars provide Mongo credentials).
No shell interpolation: script is base64-piped so $ operators survive intact.

Memory-aware for the 512Mi pod limit: small batches + explicit GC.
"""
from pymongo import MongoClient
import os
import datetime
import gc
import resource
import statistics
from collections import defaultdict

c = MongoClient(
    host=os.environ["MONGODB_HOST"],
    port=int(os.environ["MONGODB_PORT"]),
    username=os.environ["MONGODB_USER"],
    password=os.environ["MONGODB_PASS"],
    authSource="admin",
    serverSelectionTimeoutMS=5000,
    socketTimeoutMS=60000,
)
db = c[os.environ["MONGODB_NAME"]]

codes = [
    x
    for x in db.stock_score_predictions.distinct(
        "stock_code", {"model_version": "ranked_v1_h20"}
    )
    if not x.startswith("bj")
]
print("codes (ex-bj):", len(codes), flush=True)

rows = []
BATCH = 100
skipped_no_quote = 0
skipped_no_base = 0
skipped_no_later = 0
for i in range(0, len(codes), BATCH):
    batch_codes = codes[i : i + BATCH]
    closes = defaultdict(dict)
    for q in db.stock_daily_quote.find(
        {"code": {"$in": batch_codes}, "date": {"$gte": datetime.datetime(2026, 4, 1)}},
        {"code": 1, "date": 1, "close": 1},
    ):
        closes[q["code"]][q["date"]] = float(q["close"])
    try:
        for d in db.stock_score_predictions.find(
            {"model_version": "ranked_v1_h20", "stock_code": {"$in": batch_codes}},
            {"stock_code": 1, "date": 1, "percentile": 1, "target_date": 1},
        ):
            cs = closes.get(d.get("stock_code"))
            if not cs:
                skipped_no_quote += 1
                continue
            base = cs.get(d.get("date"))
            if base is None:
                skipped_no_base += 1
                continue
            # prefer the score doc's own target_date (verified-consistent);
            # fall back to +20 calendar days when absent.
            target_dt = d.get("target_date") or (
                d.get("date") + datetime.timedelta(days=20)
            )
            later = [x for x in cs if x >= target_dt]
            if not later:
                skipped_no_later += 1
                continue
            rows.append(
                (
                    float(d.get("percentile") or 0),
                    cs[min(later)] / base - 1,
                    d.get("date"),
                )
            )
    except Exception as e:
        print("batch %d partial error: %r" % (i // BATCH + 1, e), flush=True)
    del closes
    gc.collect()
    rss_mb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024.0
    print(
        "batch %d done, rows so far: %d, rss=%.0fMB"
        % (i // BATCH + 1, len(rows), rss_mb),
        flush=True,
    )

print(
    "skips: no_quote=%d no_base=%d no_later=%d"
    % (skipped_no_quote, skipped_no_base, skipped_no_later),
    flush=True,
)

# --- Spearman rank IC: percentile vs 20d return (sample-based, n <= 200k) ---
def spearman_rank_ic(pairs):
    xs_sorted = sorted(set(p[0] for p in pairs))
    ys_sorted = sorted(set(p[1] for p in pairs))
    rank = {v: i + 1 for i, v in enumerate(xs_sorted)}
    rrank = {v: i + 1 for i, v in enumerate(ys_sorted)}
    xr = [rank[p[0]] for p in pairs]
    yr = [rrank[p[1]] for p in pairs]
    n = len(pairs)
    mx = sum(xr) / n
    my = sum(yr) / n
    cov = sum((a - mx) * (b - my) for a, b in zip(xr, yr)) / (n - 1)
    sx = (sum((a - mx) ** 2 for a in xr) / (n - 1)) ** 0.5
    sy = (sum((b - my) ** 2 for b in yr) / (n - 1)) ** 0.5
    return cov / (sx * sy) if sx and sy else 0.0


ic = spearman_rank_ic([(p[0], p[1]) for p in rows])
print("=== Spearman IC (percentile vs 20d return): %.4f (n=%d) ===" % (ic, len(rows)))

# --- monthly stability: average 20d return by score month per decile ---
from collections import defaultdict as _dd

month_rows = _dd(list)
for pct, ret, dt in rows:
    month_rows[(dt.year, dt.month)].append((pct, ret))
print("=== monthly decile spread (HIGH10 - LOW10, 20d avg) ===")
for mk in sorted(month_rows):
    mr = sorted(month_rows[mk])
    nn = len(mr)
    dd_ = nn // 10
    low = sum(x[1] for x in mr[:dd_]) / dd_
    high = sum(x[1] for x in mr[-dd_:]) / dd_
    print(
        "month=%04d-%02d n=%d LOW10=%.2f%% HIGH10=%.2f%% spread=%.2f%%"
        % (mk[0], mk[1], nn, low * 100, high * 100, (high - low) * 100)
    )

rows.sort(key=lambda x: x[0])
n = len(rows)
if n >= 100:
    dec = n // 10
    print("=== decile 20d returns (n=%d) ===" % n)
    for j in range(10):
        seg = rows[j * dec : (j + 1) * dec] if j < 9 else rows[j * dec :]
        print(
            "D%d: pct %.3f-%.3f | n=%d avg=%.2f%%"
            % (
                j,
                seg[0][0],
                seg[-1][0],
                len(seg),
                statistics.mean(x[1] for x in seg) * 100,
            )
        )
    low10 = rows[:dec]
    high10 = rows[-dec:]
    print(
        "LOW10: %.2f%% | HIGH10: %.2f%% | spread: %.2f%%"
        % (
            statistics.mean(x[1] for x in low10) * 100,
            statistics.mean(x[1] for x in high10) * 100,
            (statistics.mean(x[1] for x in high10) - statistics.mean(x[1] for x in low10))
            * 100,
        )
    )

    # === S1 per-date series: equal-weight portfolio of lowest-20% percentile ===
    by_date = {}
    for pct, ret, dt in rows:
        by_date.setdefault(dt, []).append((pct, ret))
    dates = sorted(by_date)
    print("=== S1 per-date series (lowest-20%% vs market vs top-20%%, 20d) ===")
    cum_s1 = 1.0
    cum_mkt = 1.0
    hit = 0
    n_dates = 0
    drawdown = 0.0
    peak = 1.0
    for dt in dates:
        items = by_date[dt]
        items.sort(key=lambda x: x[0])
        k = max(1, len(items) // 5)
        s1 = statistics.mean(x[1] for x in items[:k])
        top = statistics.mean(x[1] for x in items[-k:])
        mkt = statistics.mean(x[1] for x in items)
        cum_s1 *= 1 + s1
        cum_mkt *= 1 + mkt
        peak = max(peak, cum_s1)
        drawdown = min(drawdown, cum_s1 / peak - 1)
        if s1 > mkt:
            hit += 1
        n_dates += 1
        if n_dates <= 5 or n_dates > len(dates) - 5:
            print(
                "date=%s n=%d S1=%.2f%% top20=%.2f%% mkt=%.2f%%"
                % (dt.strftime("%Y-%m-%d"), len(items), s1 * 100, top * 100, mkt * 100)
            )
    avg_s1 = (
        sum(
            statistics.mean(x[1] for x in by_date[d][: max(1, len(by_date[d]) // 5)])
            for d in dates
        )
        / n_dates
        * 100
    )
    print(
        "S1: dates=%d avg=%.2f%%/period cum=%.2f (%.1f%%) hit_rate=%.0f%% maxDD=%.2f%% | mkt cum=%.2f (%.1f%%)"
        % (
            n_dates,
            avg_s1,
            cum_s1,
            (cum_s1 - 1) * 100,
            hit / n_dates * 100,
            drawdown * 100,
            cum_mkt,
            (cum_mkt - 1) * 100,
        )
    )
else:
    print("insufficient rows:", n)
