import datetime
import os
import time

import pandas

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
        if window_end < end_year:
            time.sleep(0.1)

    if not frames:
        return pandas.DataFrame()
    return pandas.concat(frames, ignore_index=True)
