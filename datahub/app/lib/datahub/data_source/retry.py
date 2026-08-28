import logging
import time

from requests.exceptions import ConnectionError, RequestException


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
    "每分钟最多",
    "频率超限",
)


def is_retryable_market_data_error(error: Exception) -> bool:
    if isinstance(error, (ConnectionError, RequestException)):
        return True

    error_message = str(error)
    if type(error).__name__ == "JSONDecodeError" or "Can not decode" in error_message:
        return True
    return any(marker in error_message for marker in TRANSIENT_NETWORK_MARKERS)


def call_with_retry(
    fetcher, label: str, max_attempts: int = 3, base_delay: float = 1.0
):
    last_error = None
    for attempt in range(1, max_attempts + 1):
        try:
            return fetcher()
        except Exception as error:  # noqa: BLE001
            last_error = error
            if attempt >= max_attempts or not is_retryable_market_data_error(error):
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
