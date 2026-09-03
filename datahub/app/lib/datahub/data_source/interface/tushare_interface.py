import datetime
import os
import time

import pandas

from app.lib.datahub.data_source.retry import call_with_retry

TUSHARE_TOKEN_ENV = "TUSHARE_TOKEN"
# tushare pro.daily returns at most 6000 rows per call (~23 years); request
# history in year windows so old listings never silently truncate their
# earliest bars. 18 calendar years stays well under the 6000-row cap.
WINDOW_YEARS = 18


def to_tushare_ts_code(code: str) -> str:
    """Map internal code (sh600519) to tushare ts_code (600519.SH)."""
    code = str(code)
    if code.startswith("sh"):
        return code[2:] + ".SH"
    if code.startswith("sz"):
        return code[2:] + ".SZ"
    if code.startswith("bj"):
        return code[2:] + ".BJ"
    raise ValueError(f"cannot map internal code {code!r} to a tushare ts_code")


def from_tushare_ts_code(ts_code: str) -> str:
    """Map tushare ts_code (600519.SH) back to internal code (sh600519)."""
    code, _, market = str(ts_code).partition(".")
    return market.lower() + code


def stock_basic_active():
    """Active A-share list via tushare pro.stock_basic (list_status='L')."""
    pro = _get_pro()
    return pro.stock_basic(exchange="", list_status="L", fields="ts_code,name")


def daily_by_trade_date(trade_date):
    """Full-market daily snapshot for one trade date (YYYYMMDD).

    Columns include ts_code, open, high, low, close, pre_close, change,
    pct_chg, vol, amount. Tushare omits suspended stocks for that date
    (absence = temporarily suspended).
    """
    pro = _get_pro()
    return pro.daily(trade_date=trade_date)


def _get_pro():
    token = os.getenv(TUSHARE_TOKEN_ENV, "").strip()
    if not token:
        raise RuntimeError(
            f"{TUSHARE_TOKEN_ENV} is not set; cannot fetch tushare history"
        )
    import tushare as ts

    ts.set_token(token)
    return ts.pro_api()


def tushare_daily(ts_code, start_date=None, end_date=None):
    """Return daily bars for one A-share via tushare pro daily.

    Columns: ts_code, trade_date (YYYYMMDD), open, high, low, close,
    pre_close, change, pct_chg, vol (手), amount (千元).
    Paginates by calendar-year windows to stay under the per-call row cap.
    """
    pro = _get_pro()
    start_date = start_date or "19900101"
    end_date = end_date or datetime.date.today().strftime("%Y%m%d")
    start_year = int(start_date[:4])
    end_year = int(end_date[:4])

    frames = []
    for window_start in range(start_year, end_year + 1, WINDOW_YEARS):
        window_end = min(window_start + WINDOW_YEARS - 1, end_year)
        window_start_date = (
            start_date if window_start == start_year else f"{window_start}0101"
        )
        window_end_date = end_date if window_end == end_year else f"{window_end}1231"
        if window_start_date > window_end_date:
            continue
        df = pro.daily(
            ts_code=ts_code,
            start_date=window_start_date,
            end_date=window_end_date,
        )
        if df is not None and not df.empty:
            frames.append(df)
        # pace EVERY call (not only between windows) to stay under tushare's
        # per-minute cap (300/min tier): ~0.25s/call keeps us at <=240/min
        # even for single-window incremental pulls
        time.sleep(0.25)

    if not frames:
        return pandas.DataFrame()
    return pandas.concat(frames, ignore_index=True)


def adj_factor(ts_code, start_date=None, end_date=None):
    """Return real after-adjustment factors for one A-share via pro.adj_factor.

    Columns: ts_code, trade_date (YYYYMMDD), adj_factor. adj_factor only
    changes on ex-dividend dates; it is constant otherwise. Paginates by
    calendar-year windows (same as tushare_daily) and paces every call.
    """
    pro = _get_pro()
    start_date = start_date or "19900101"
    end_date = end_date or datetime.date.today().strftime("%Y%m%d")
    start_year = int(start_date[:4])
    end_year = int(end_date[:4])

    frames = []
    for window_start in range(start_year, end_year + 1, WINDOW_YEARS):
        window_end = min(window_start + WINDOW_YEARS - 1, end_year)
        window_start_date = (
            start_date if window_start == start_year else f"{window_start}0101"
        )
        window_end_date = end_date if window_end == end_year else f"{window_end}1231"
        if window_start_date > window_end_date:
            continue
        df = call_with_retry(
            lambda: pro.adj_factor(
                ts_code=ts_code,
                start_date=window_start_date,
                end_date=window_end_date,
            ),
            label=f"tushare.adj_factor:{ts_code}:{window_start_date}-{window_end_date}",
        )
        if df is None or df.empty:
            raise RuntimeError(
                "tushare adj_factor returned no rows: "
                f"ts_code={ts_code} window={window_start_date}-{window_end_date}"
            )
        frames.append(df)
        # pace EVERY call to stay under the per-minute cap (same as daily)
        time.sleep(0.25)

    return pandas.concat(frames, ignore_index=True)


def adj_factor_by_trade_date(trade_date):
    """Return the full-market adj_factor snapshot for one trade date."""
    pro = _get_pro()
    df = call_with_retry(
        lambda: pro.adj_factor(trade_date=trade_date),
        label=f"tushare.adj_factor:{trade_date}",
    )
    if df is None or df.empty:
        raise RuntimeError(
            f"tushare adj_factor returned no rows: trade_date={trade_date}"
        )
    time.sleep(0.25)
    return df


def daily_basic_by_trade_date(trade_date):
    """Return the full-market daily_basic snapshot for one trade date.

    Columns include ts_code, trade_date, close, turnover_rate,
    volume_ratio, pe, pe_ttm, pb, ps, ps_ttm, dv_ratio, dv_ttm,
    total_share, float_share, free_share, total_mv (万元), circ_mv (万元).
    Values are computed at that trade date's close from the latest
    published financials — point-in-time by trade_date (no look-ahead).
    Tushare omits suspended stocks for that date (absence = suspended).
    """
    pro = _get_pro()
    df = call_with_retry(
        lambda: pro.daily_basic(trade_date=trade_date),
        label=f"tushare.daily_basic:{trade_date}",
    )
    if df is None or df.empty:
        raise RuntimeError(
            f"tushare daily_basic returned no rows: trade_date={trade_date}"
        )
    time.sleep(0.25)
    return df
