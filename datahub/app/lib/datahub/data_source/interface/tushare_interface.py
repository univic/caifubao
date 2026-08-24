import os

TUSHARE_TOKEN_ENV = "TUSHARE_TOKEN"


def to_tushare_ts_code(code: str) -> str:
    """Map internal code (sh600519) to tushare ts_code (600519.SH)."""
    code = str(code)
    if code.startswith("sh"):
        return code[2:] + ".SH"
    if code.startswith("sz"):
        return code[2:] + ".SZ"
    if code.startswith("bj"):
        return code[2:] + ".BJ"
    return code + ".SH"


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
    """
    pro = _get_pro()
    kwargs = {"ts_code": ts_code}
    if start_date:
        kwargs["start_date"] = start_date
    if end_date:
        kwargs["end_date"] = end_date
    return pro.daily(**kwargs)
