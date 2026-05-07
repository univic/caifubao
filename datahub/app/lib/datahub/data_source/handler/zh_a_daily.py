import datetime
import logging
import time
from requests.exceptions import ConnectionError, RequestException

from app.lib.datahub.data_source import interface
from app.lib.datahub.data_source.interface.baostock_interface import (
    BaostockInterfaceManager,
)
from app.lib.utilities import trading_day_helper, performance_helper, stock_code_helper


logger = logging.getLogger(__name__)


TRANSIENT_NETWORK_MARKERS = (
    "Temporary failure in name resolution",
    "NameResolutionError",
    "Max retries exceeded",
    "Connection aborted",
    "RemoteDisconnected",
    "Read timed out",
    "ConnectTimeout",
    "Connection reset by peer",
)


def _is_retryable_market_data_error(error: Exception) -> bool:
    if isinstance(error, (ConnectionError, RequestException)):
        return True

    error_message = str(error)
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


def get_zh_a_index_hist_daily_quote(code, start_date=None, incremental=True):
    raw_df = _call_with_retry(
        lambda: interface.akshare_interface.stock_zh_index_daily(code),
        label=f"stock_zh_index_daily:{code}",
    )
    raw_df.fillna(0, inplace=True)
    if incremental and start_date:
        start_date = datetime.datetime.strptime(start_date, "%Y-%m-%d").date()
        df = raw_df[raw_df.date > start_date].sort_index(axis=1, ascending=False)
    else:
        df = raw_df
    return df


def get_zh_a_stock_hist_daily_quote(code, start_date=None):
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
        lambda: BaostockInterfaceManager.get_zh_a_stock_hist_k_data(code, start_date),
        label=f"stock_hist_k_data:{code}",
    )
    if len(raw_df) > 0:
        raw_df.replace("", 0, inplace=True)  # replace empty cells
        raw_df.fillna(0, inplace=True)
        # perform type convert
        raw_df[float_columns] = raw_df[float_columns].astype("float")
        raw_df[int_columns] = raw_df[int_columns].astype("int")
        raw_df.rename(name_mapping, axis=1, inplace=True)  # rename column
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
