import datetime
import logging
import os
import time
from typing import Any

import pandas
from requests.exceptions import ConnectionError, RequestException

from app.lib.datahub.data_source import interface
from app.lib.datahub.data_source.interface.baostock_interface import (
    BaostockInterfaceManager,
)
from app.lib.utilities import trading_day_helper, performance_helper, stock_code_helper


logger = logging.getLogger(__name__)


STOCK_HISTORY_SOURCE_ENV = "DATAHUB_STOCK_HISTORY_SOURCE"
SUPPORTED_STOCK_HISTORY_SOURCES = {"akshare", "baostock", "tushare"}

STOCK_UNIVERSE_SOURCE_ENV = "DATAHUB_STOCK_UNIVERSE_SOURCE"
SUPPORTED_STOCK_UNIVERSE_SOURCES = {"spot", "tushare"}


def get_stock_universe_source() -> str:
    source = os.getenv(STOCK_UNIVERSE_SOURCE_ENV, "spot").strip().lower()
    if source not in SUPPORTED_STOCK_UNIVERSE_SOURCES:
        raise ValueError(
            f"{STOCK_UNIVERSE_SOURCE_ENV} must be one of "
            f"{sorted(SUPPORTED_STOCK_UNIVERSE_SOURCES)}"
        )
    return source


TRANSIENT_NETWORK_MARKERS = (
    "Temporary failure in name resolution",
    "NameResolutionError",
    "Max retries exceeded",
    "Connection aborted",
    "RemoteDisconnected",
    "Read timed out",
    "ConnectTimeout",
    "Connection reset by peer",
    "每分钟最多",  # tushare rate-limit message: 抱歉，您每分钟最多访问该接口N次
    "频率超限",  # tushare rate-limit message: 您访问接口(daily)频率超限(300次/分钟)
)


def _is_retryable_market_data_error(error: Exception) -> bool:
    if isinstance(error, (ConnectionError, RequestException)):
        return True

    error_message = str(error)
    # Anti-bot HTML responses commonly surface as JSON decode errors (e.g.
    # akshare's demjson JSONDecodeError: "Can not decode value starting with
    # character '<'"). Treat them as transient like network errors: retry
    # before failing the run.
    if type(error).__name__ == "JSONDecodeError" or "Can not decode" in error_message:
        return True
    return any(marker in error_message for marker in TRANSIENT_NETWORK_MARKERS)


def _call_with_retry(
    fetcher, label: str, max_attempts: int = 3, base_delay: float = 1.0
):
    last_error = None
    for attempt in range(1, max_attempts + 1):
        try:
            return fetcher()
        except Exception as error:  # noqa: BLE001
            last_error = error
            if attempt >= max_attempts or not _is_retryable_market_data_error(error):
                logger.error(
                    "Market data request failed: source=%s attempt=%s/%s error=%s",
                    label,
                    attempt,
                    max_attempts,
                    error,
                )
                raise

            delay = base_delay * (2 ** (attempt - 1))
            logger.warning(
                "Market data request retrying: source=%s attempt=%s/%s delay=%.1fs error=%s",
                label,
                attempt,
                max_attempts,
                delay,
                error,
            )
            time.sleep(delay)

    raise last_error


def get_stock_history_source() -> str:
    source = os.getenv(STOCK_HISTORY_SOURCE_ENV, "akshare").strip().lower()
    if source not in SUPPORTED_STOCK_HISTORY_SOURCES:
        raise ValueError(
            f"{STOCK_HISTORY_SOURCE_ENV} must be one of "
            f"{sorted(SUPPORTED_STOCK_HISTORY_SOURCES)}"
        )
    return source


def stock_history_uses_baostock() -> bool:
    return get_stock_history_source() == "baostock"


def _normalize_akshare_stock_history(raw_df, code: str):
    if raw_df is None or raw_df.empty:
        return None

    column_mapping = {
        "日期": "date",
        "开盘": "open",
        "收盘": "close",
        "最高": "high",
        "最低": "low",
        "成交量": "volume",
        "成交额": "trade_amount",
        "涨跌幅": "change_rate",
        "涨跌额": "change_amount",
        "换手率": "turnover_rate",
    }
    normalized = raw_df.rename(columns=column_mapping).copy()
    required_columns = {"date", "open", "close", "high", "low", "volume"}
    missing_columns = required_columns - set(normalized.columns)
    if missing_columns:
        raise ValueError(
            "AkShare stock history response missing required columns: "
            f"{sorted(missing_columns)}"
        )

    normalized["date"] = pandas.to_datetime(normalized["date"])
    numeric_columns = [
        "open",
        "close",
        "high",
        "low",
        "volume",
        "trade_amount",
        "change_rate",
        "change_amount",
        "turnover_rate",
    ]
    for column in numeric_columns:
        if column not in normalized:
            normalized[column] = 0
        normalized[column] = pandas.to_numeric(
            normalized[column], errors="coerce"
        ).fillna(0)

    normalized["previous_close"] = (
        normalized["close"] - normalized["change_amount"]
    ).round(4)
    normalized["volume"] = normalized["volume"].astype("int64")
    normalized["code"] = code
    normalized["trade_status"] = 1
    normalized["peTTM"] = 0.0
    normalized["pbMRQ"] = 0.0
    normalized["psTTM"] = 0.0
    normalized["pcfNcfTTM"] = 0.0
    normalized["isST"] = 0

    canonical_columns = [
        "date",
        "code",
        "open",
        "high",
        "low",
        "close",
        "previous_close",
        "volume",
        "trade_amount",
        "turnover_rate",
        "change_rate",
        "change_amount",
        "trade_status",
        "peTTM",
        "pbMRQ",
        "psTTM",
        "pcfNcfTTM",
        "isST",
    ]
    return normalized[canonical_columns]


def _normalize_tushare_stock_history(raw_df, code: str):
    if raw_df is None or raw_df.empty:
        return None

    normalized = raw_df.rename(
        columns={
            "trade_date": "date",
            "pre_close": "previous_close",
            "pct_chg": "change_rate",
            "change": "change_amount",
            "vol": "volume",
        }
    ).copy()
    required_columns = {"date", "open", "close", "high", "low", "volume"}
    missing_columns = required_columns - set(normalized.columns)
    if missing_columns:
        raise ValueError(
            "Tushare stock history response missing required columns: "
            f"{sorted(missing_columns)}"
        )

    normalized["date"] = pandas.to_datetime(normalized["date"], format="%Y%m%d")
    numeric_columns = [
        "open",
        "close",
        "high",
        "low",
        "volume",
        "previous_close",
        "trade_amount",
        "change_rate",
        "change_amount",
        "turnover_rate",
    ]
    for column in numeric_columns:
        if column not in normalized:
            normalized[column] = 0
        normalized[column] = pandas.to_numeric(
            normalized[column], errors="coerce"
        ).fillna(0)

    # tushare amount 单位千元 -> 元（与 akshare/东财成交额一致）
    normalized["trade_amount"] = (
        pandas.to_numeric(normalized["amount"], errors="coerce").fillna(0) * 1000
        if "amount" in normalized
        else 0
    )
    normalized["volume"] = normalized["volume"].astype("int64")
    normalized["code"] = code
    normalized["trade_status"] = 1
    normalized["peTTM"] = 0.0
    normalized["pbMRQ"] = 0.0
    normalized["psTTM"] = 0.0
    normalized["pcfNcfTTM"] = 0.0
    normalized["isST"] = 0

    canonical_columns = [
        "date",
        "code",
        "open",
        "high",
        "low",
        "close",
        "previous_close",
        "volume",
        "trade_amount",
        "turnover_rate",
        "change_rate",
        "change_amount",
        "trade_status",
        "peTTM",
        "pbMRQ",
        "psTTM",
        "pcfNcfTTM",
        "isST",
    ]
    return normalized[canonical_columns].sort_values("date").reset_index(drop=True)


def get_a_stock_trade_date_hist():
    remote_data = _call_with_retry(
        interface.akshare_interface.get_trade_date_hist,
        label="trade_date_hist",
    )
    # convert to datetime
    r = remote_data["trade_date"].map(trading_day_helper.convert_date_to_datetime)
    return list(r)


@performance_helper.func_performance_timer
def get_zh_a_stock_index_spot():
    name_mapping = {
        "名称": "name",
        "代码": "code",
        "今开": "open",
        "最新价": "close",
        "最高": "high",
        "最低": "low",
        "成交量": "volume",
    }
    raw_df = _call_with_retry(
        interface.akshare_interface.zh_stock_index_spot,
        label="zh_stock_index_spot",
    )
    raw_df.rename(name_mapping, axis=1, inplace=True)
    raw_df.fillna(0, inplace=True)
    df = raw_df.loc[raw_df["name"] != ""]
    return df


@performance_helper.func_performance_timer
def get_zh_a_stock_spot():
    name_mapping = {
        "名称": "name",
        # '代码': 'code',        # will carry out code convert later
        "今开": "open",
        "昨收": "previous_close",
        "最新价": "close",
        "涨跌幅": "change_rate",
        "涨跌额": "change_amount",
        "最高": "high",
        "最低": "low",
        "成交量": "volume",
        "成交额": "trade_amount",
        "振幅": "amplitude",
        "换手率": "turnover_rate",
        "市盈率-动态": "peTTM",
        "市净率": "pbMRQ",
    }
    try:
        raw_df = _call_with_retry(
            interface.akshare_interface.stock_zh_a_spot_em,
            label="stock_zh_a_spot_em",
        )
    except (ConnectionError, RequestException) as e:
        logger.warning(
            f"ConnectionError occurred when calling stock_zh_a_spot_em(): {e}, falling back to stock_zh_a_spot()"
        )
        try:
            raw_df = _call_with_retry(
                interface.akshare_interface.stock_zh_a_spot,
                label="stock_zh_a_spot",
            )
        except (ConnectionError, RequestException) as e:
            logger.error(
                f"Fallback interface stock_zh_a_spot() also failed with ConnectionError: {e}"
            )
            raise
        except Exception as e:
            logger.error(
                f"Fallback interface stock_zh_a_spot() failed with unexpected error: {e}"
            )
            raise
    except Exception as e:
        logger.error(f"Unexpected error when calling stock_zh_a_spot_em(): {e}")
        raise
    df = raw_df[raw_df["名称"] != ""]
    df.fillna(0, inplace=True)
    df.rename(name_mapping, axis=1, inplace=True)  # rename column
    df["code"] = df["代码"].apply(stock_code_helper.add_market_prefix)
    return df


def _build_tushare_universe(as_of_date: str | None = None):
    """Build the stock universe from tushare (stock_basic + daily snapshot).

    Returns a DataFrame with code/name/close plus the full daily bar fields
    (open/high/low/volume/trade_amount/previous_close/change_amount/
    change_rate/turnover_rate); close == 0 means
    temporarily suspended (absent from the as-of daily snapshot — tushare
    omits suspended stocks), matching the spot path's suspension semantics.
    """
    trade_date = (
        as_of_date.replace("-", "")
        if as_of_date
        else datetime.date.today().strftime("%Y%m%d")
    )
    basic = _call_with_retry(
        lambda: interface.tushare_interface.stock_basic_active(),
        label="tushare_stock_basic",
    )
    daily = _call_with_retry(
        lambda: interface.tushare_interface.daily_by_trade_date(trade_date),
        label=f"tushare_daily:{trade_date}",
    )
    if basic is None or basic.empty:
        return pandas.DataFrame(
            columns=[
                "code",
                "name",
                "close",
                "open",
                "high",
                "low",
                "volume",
                "trade_amount",
                "previous_close",
                "change_amount",
                "change_rate",
                "turnover_rate",
            ]
        )
    if daily is None or daily.empty:
        # An empty snapshot would flag the whole market as suspended and
        # silently no-op the quote phase; fail loudly instead.
        raise RuntimeError(
            f"Tushare daily snapshot for {trade_date} is empty; "
            "cannot resolve the stock universe"
        )

    daily_map: dict[str, Any] = {}
    for _, row in daily.iterrows():
        ts_code = row.get("ts_code")
        if ts_code:
            daily_map[ts_code] = row

    def _num(daily_row, key):
        value = pandas.to_numeric(daily_row.get(key), errors="coerce")
        return 0.0 if pandas.isna(value) else float(value)

    rows = []
    for _, row in basic.iterrows():
        ts_code = row.get("ts_code")
        name = row.get("name", "")
        if not ts_code:
            continue
        code = interface.tushare_interface.from_tushare_ts_code(ts_code)
        daily_row = daily_map.get(ts_code)
        if daily_row is None:
            # tushare omits suspended stocks for the date -> all fields 0
            out = {
                "code": code,
                "name": name,
                "close": 0.0,
                "open": 0.0,
                "high": 0.0,
                "low": 0.0,
                "volume": 0.0,
                "trade_amount": 0.0,
                "previous_close": 0.0,
                "change_amount": 0.0,
                "change_rate": 0.0,
                "turnover_rate": 0.0,
            }
        else:
            out = {
                "code": code,
                "name": name,
                "close": _num(daily_row, "close"),
                "open": _num(daily_row, "open"),
                "high": _num(daily_row, "high"),
                "low": _num(daily_row, "low"),
                "volume": _num(daily_row, "vol"),
                # amount 千元 -> 元
                "trade_amount": _num(daily_row, "amount") * 1000,
                "previous_close": _num(daily_row, "pre_close"),
                "change_amount": _num(daily_row, "change"),
                "change_rate": _num(daily_row, "pct_chg"),
                "turnover_rate": 0.0,
            }
        rows.append(out)
    return pandas.DataFrame(rows)


def get_zh_a_stock_universe(as_of_date: str | None = None):
    """Resolve the stock universe per DATAHUB_STOCK_UNIVERSE_SOURCE.

    spot (default) -> eastmoney/sina spot list; tushare -> stock_basic +
    the as-of-date daily snapshot (full daily bar fields included).
    """
    if get_stock_universe_source() == "tushare":
        return _build_tushare_universe(as_of_date)
    return get_zh_a_stock_spot()


def get_zh_a_index_hist_daily_quote(
    code, start_date=None, end_date=None, incremental=True
):
    raw_df = _call_with_retry(
        lambda: interface.akshare_interface.stock_zh_index_daily(code),
        label=f"stock_zh_index_daily:{code}",
    )
    raw_df.fillna(0, inplace=True)
    if end_date:
        end_date = datetime.datetime.strptime(end_date, "%Y-%m-%d").date()
        raw_df = raw_df[raw_df.date <= end_date]
    if incremental and start_date:
        start_date = datetime.datetime.strptime(start_date, "%Y-%m-%d").date()
        df = raw_df[raw_df.date >= start_date].sort_index(axis=1, ascending=False)
    else:
        df = raw_df
    return df


def get_zh_a_stock_hist_daily_quote(code, start_date=None, end_date=None):
    normalized_end_date = (
        end_date.replace("-", "")
        if end_date
        else datetime.date.today().strftime("%Y%m%d")
    )
    if get_stock_history_source() == "akshare":
        symbol = code[2:] if code.startswith(("sh", "sz", "bj")) else code
        normalized_start_date = start_date.replace("-", "") if start_date else None
        raw_df = _call_with_retry(
            lambda: interface.akshare_interface.stock_zh_a_hist(
                symbol,
                start_date=normalized_start_date,
                end_date=normalized_end_date,
            ),
            label=f"akshare_stock_zh_a_hist:{code}",
        )
        normalized = _normalize_akshare_stock_history(raw_df, code)
        if normalized is None:
            return None
        return normalized[normalized["date"] <= pandas.to_datetime(normalized_end_date)]

    if get_stock_history_source() == "tushare":
        ts_code = interface.tushare_interface.to_tushare_ts_code(code)
        normalized_start_date = start_date.replace("-", "") if start_date else None
        raw_df = _call_with_retry(
            lambda: interface.tushare_interface.tushare_daily(
                ts_code,
                start_date=normalized_start_date,
                end_date=normalized_end_date,
            ),
            label=f"tushare_daily:{code}",
        )
        normalized = _normalize_tushare_stock_history(raw_df, code)
        if normalized is None:
            return None
        return normalized[normalized["date"] <= pandas.to_datetime(normalized_end_date)]

    name_mapping = {
        "preclose": "previous_close",
        "pcgChg": "change_rate",
        "amount": "trade_amount",
        "turn": "turnover_rate",
        "tradestatus": "trade_status",
    }
    float_columns = [
        "open",
        "high",
        "low",
        "close",
        "preclose",
        "volume",
        "amount",
        "turn",
        "pctChg",
        "peTTM",
        "pbMRQ",
        "psTTM",
        "pcfNcfTTM",
    ]
    int_columns = ["adjustflag", "tradestatus", "isST"]
    raw_df = _call_with_retry(
        lambda: BaostockInterfaceManager.get_zh_a_stock_hist_k_data(
            code,
            start_date,
            datetime.datetime.strptime(normalized_end_date, "%Y%m%d").strftime(
                "%Y-%m-%d"
            ),
        ),
        label=f"stock_hist_k_data:{code}",
    )
    if len(raw_df) > 0:
        raw_df.replace("", 0, inplace=True)  # replace empty cells
        raw_df.fillna(0, inplace=True)
        # perform type convert
        raw_df[float_columns] = raw_df[float_columns].astype("float")
        raw_df[int_columns] = raw_df[int_columns].astype("int")
        raw_df.rename(name_mapping, axis=1, inplace=True)  # rename column
        raw_df["date"] = pandas.to_datetime(raw_df["date"])
        raw_df = raw_df[raw_df["date"] <= pandas.to_datetime(normalized_end_date)]
        raw_df["code"] = raw_df["code"].apply(
            lambda x: x.replace(".", "")
        )  # replace the dot in the stock code
        return raw_df
    else:
        return None


if __name__ == "__main__":
    obj = interface.baostock_interface.BaostockInterfaceManager()
    conn = obj.establish_baostock_conn()
    o = get_zh_a_stock_hist_daily_quote(code="sh688555")
    print(o)
