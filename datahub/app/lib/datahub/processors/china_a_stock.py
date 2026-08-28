import time
import logging
import datetime
from typing import Any, Callable
from zoneinfo import ZoneInfo

import pandas
from pymongo import UpdateOne
from mongoengine import NotUniqueError
from app.lib.datahub.data_source.handler import zh_a_daily
from app.model.stock import (
    FinanceMarket,
    BasicStock,
    StockIndex,
    IndividualStock,
    StockDailyQuote,
    StockDataCapabilities,
)
from app.model.data_asset_status import STATUS_OK, STATUS_STALE

# from app.lib.task_controller import task_controller
from app.lib.datahub.data_source.interface.baostock_interface import (
    BaostockInterfaceManager,
)
from app.lib.factor_factory import FQFactorService, MovingAverageFactorService
from app.lib.utilities.progress_bar import progress_bar
from app.lib.utilities import trading_day_helper
from app.lib.utilities import data_asset_status_helper
from app.lib.utilities import data_capability_helper

logger = logging.getLogger(__name__)

# A full-market run aborts early once this many consecutive history pulls
# failed (not attributable to suspension). Soft-failing sources would
# otherwise burn the whole loop (one absorbed FAIL + sleep per stock) before
# the final validation failure.
HISTORY_FAILURE_CIRCUIT_LIMIT = 25

STATUS_WRITE_CHUNK_SIZE = 1000


def _chunked(values, chunk_size):
    for start in range(0, len(values), chunk_size):
        yield values[start : start + chunk_size]


def _build_index_quote_upsert_operation(index_obj, row):
    daily_quote = StockDailyQuote()
    daily_quote.code = index_obj.code
    daily_quote.stock = index_obj
    daily_quote.date = row["date"]
    daily_quote.open = row["open"]
    daily_quote.close = row["close"]
    daily_quote.high = row["high"]
    daily_quote.low = row["low"]
    daily_quote.volume = row["volume"]

    raw_doc = daily_quote.to_mongo().to_dict()
    raw_doc.pop("_id", None)

    return UpdateOne(
        {
            "code": raw_doc["code"],
            "date": raw_doc["date"],
        },
        {"$setOnInsert": raw_doc},
        upsert=True,
    )


def _build_stock_quote_upsert_operation(stock_obj, row):
    daily_quote = StockDailyQuote()
    daily_quote.code = stock_obj.code
    daily_quote.stock = stock_obj
    for column, value in row.items():
        setattr(daily_quote, column, value)
    daily_quote.amplitude = round(daily_quote.high - daily_quote.low, 2)
    daily_quote.change_amount = round(daily_quote.close - daily_quote.previous_close, 2)

    raw_doc = daily_quote.to_mongo().to_dict()
    raw_doc.pop("_id", None)
    return UpdateOne(
        {
            "code": raw_doc["code"],
            "date": raw_doc["date"],
        },
        {"$set": raw_doc},
        upsert=True,
    )


def _build_snapshot_quote_row(snapshot_row, expected_date, code):
    """Build one settled daily-bar row from a market snapshot row."""

    def _num(value):
        coerced = pandas.to_numeric(value, errors="coerce")
        return 0.0 if pandas.isna(coerced) else float(coerced)

    return {
        "date": expected_date,
        "code": code,
        "open": _num(snapshot_row.get("open", 0)),
        "high": _num(snapshot_row.get("high", 0)),
        "low": _num(snapshot_row.get("low", 0)),
        "close": _num(snapshot_row.get("close", 0)),
        "previous_close": _num(snapshot_row.get("previous_close", 0)),
        "volume": _num(snapshot_row.get("volume", 0)),
        "trade_amount": _num(snapshot_row.get("trade_amount", 0)),
        "turnover_rate": _num(snapshot_row.get("turnover_rate", 0)),
        "change_rate": _num(snapshot_row.get("change_rate", 0)),
        "change_amount": _num(snapshot_row.get("change_amount", 0)),
        "trade_status": 1,
    }


class ChinaAStock(object):
    def __init__(
        self,
        as_of_date=None,
        run_started_at=None,
        progress_callback: Callable[[str, str, dict, dict], None] | None = None,
    ):
        self.market_name = "ChinaAStock"
        self.market_code = "ZH-A"
        timezone = ZoneInfo(trading_day_helper.BEIJING_TIMEZONE)
        self.run_started_at = run_started_at or datetime.datetime.now(timezone)
        if self.run_started_at.tzinfo is None:
            self.run_started_at = self.run_started_at.replace(tzinfo=timezone)
        self.today = self.run_started_at.astimezone(timezone).date()
        if as_of_date and not isinstance(as_of_date, datetime.datetime):
            as_of_date = datetime.datetime.combine(as_of_date, datetime.time())
        self.explicit_as_of_date = as_of_date is not None
        self.most_recent_trading_day = as_of_date
        self.market = FinanceMarket.objects(name="ChinaAStock").first()
        self.trade_calendar = None
        self.result: dict = {
            "code": "GOOD",
            "message": "",
        }
        self.last_job_summary = None
        self._partial_phase_result = None
        self._progress_callback = progress_callback

    def run(self):
        phases = [
            ("check_prerequisite", self.check_prerequisite),
            ("check_index_data_integrity", self.check_index_data_integrity),
            ("check_stock_data_integrity", self.check_stock_data_integrity),
            ("mark_inactive_stocks", self.mark_inactive_stocks),
        ]
        return self._run_job("full_market_sync", phases)

    def run_index_job(self):
        phases = [
            ("check_prerequisite", self.check_prerequisite),
            ("check_index_data_integrity", self.check_index_data_integrity),
        ]
        return self._run_job("index_market_sync", phases)

    def run_stock_job(self):
        # Quote + factors only. Signals and scoring are produced by the
        # standalone signal_runner / scoring_runner CronJobs (18:30 / 18:35)
        # which gate on this job's persisted data — keeping the quote job
        # short so it cannot be killed by activeDeadlineSeconds mid-run.
        phases = [
            ("check_prerequisite", self.check_prerequisite),
            ("check_stock_data_integrity", self.check_stock_data_integrity),
            ("mark_inactive_stocks", self.mark_inactive_stocks),
            ("update_fq_factor", self.update_fq_factor),
            ("update_ma_factor", self.update_ma_factor),
        ]
        return self._run_job("stock_market_sync", phases)

    def run_stock_quote_job(self):
        phases = [
            ("check_prerequisite", self.check_prerequisite),
            ("check_stock_data_integrity", self.check_stock_data_integrity),
            ("mark_inactive_stocks", self.mark_inactive_stocks),
        ]
        return self._run_job("stock_quote_sync", phases)

    def _run_job(self, job_name, phases):
        summary = {
            "job_name": job_name,
            "market": self.market_name,
            "status": "SUCCESS",
            "failed_phase": None,
            "pulled_total": 0,
            "written_total": 0,
            "validated_total": 0,
            "as_of_date": None,
            "phase_stats": {},
        }
        current_phase = None
        try:
            for phase_name, phase_func in phases:
                current_phase = phase_name
                self._partial_phase_result = None
                logger.info(
                    "Datahub phase started: market=%s job=%s phase=%s",
                    self.market_name,
                    job_name,
                    phase_name,
                )
                phase_result = phase_func(allow_update=True)
                phase_summary = self._normalize_phase_result(phase_result)
                summary["phase_stats"][phase_name] = phase_summary
                summary["pulled_total"] += phase_summary["pulled_count"]
                summary["written_total"] += phase_summary["written_count"]
                summary["validated_total"] += phase_summary["validated_count"]
                if self.most_recent_trading_day:
                    summary["as_of_date"] = self.most_recent_trading_day.isoformat()
                logger.info(
                    "Datahub phase completed: market=%s job=%s phase=%s pulled=%s written=%s",
                    self.market_name,
                    job_name,
                    phase_name,
                    phase_summary["pulled_count"],
                    phase_summary["written_count"],
                )
                if self._progress_callback is not None:
                    try:
                        self._progress_callback(
                            job_name, phase_name, phase_summary, summary
                        )
                    except Exception:
                        logger.exception(
                            "Progress callback failed for job=%s phase=%s; continuing",
                            job_name,
                            phase_name,
                        )
            return summary
        except Exception:
            summary["status"] = "FAILED"
            summary["failed_phase"] = current_phase
            partial_result = getattr(self, "_partial_phase_result", None)
            if current_phase and partial_result is not None:
                phase_summary = self._normalize_phase_result(partial_result)
                summary["phase_stats"][current_phase] = phase_summary
                summary["pulled_total"] += phase_summary["pulled_count"]
                summary["written_total"] += phase_summary["written_count"]
                summary["validated_total"] += phase_summary["validated_count"]
            raise
        finally:
            if self.most_recent_trading_day:
                summary["as_of_date"] = self.most_recent_trading_day.isoformat()
            self.last_job_summary = summary
            logger.info(
                "Datahub job summary: market=%s job=%s status=%s failed_phase=%s pulled_total=%s written_total=%s phase_stats=%s",
                self.market_name,
                job_name,
                summary["status"],
                summary["failed_phase"] or "-",
                summary["pulled_total"],
                summary["written_total"],
                summary["phase_stats"],
            )

    @staticmethod
    def _normalize_phase_result(phase_result):
        if phase_result is None:
            return {"pulled_count": 0, "written_count": 0, "validated_count": 0}
        return {
            "pulled_count": phase_result.get("pulled_count", 0),
            "written_count": phase_result.get("written_count", 0),
            "validated_count": phase_result.get("validated_count", 0),
        }

    def check_prerequisite(self, allow_update=False):
        self.check_market_data_existence()
        self.check_trade_calendar_integrity()
        self.check_default_value_integrity()
        return {"pulled_count": 0, "written_count": 0}

    def check_market_data_existence(self):
        # check the existence of basic market data
        if not self.market:
            logger.info(
                f"Stock Market {self.market_name} - Local market data not found, initializing"
            )
            new_market = FinanceMarket()
            new_market.name = self.market_name
            new_market.code = self.market_code
            new_market.save()
            self.market = new_market
        else:
            logger.info(f"Stock Market {self.market.name} - Local market data check OK")

    def check_trade_calendar_integrity(self):
        if self.market.trade_calendar:
            local_data_tail = self.market.trade_calendar[-1]
            today = datetime.datetime.today()
            if local_data_tail < today:
                logger.info(
                    f"Stock Market {self.market.name} - Updating trade calendar"
                )
                trade_calendar = zh_a_daily.get_a_stock_trade_date_hist()
                self.market.trade_calendar = trade_calendar
                self.market.save()
            else:
                logger.info(
                    f"Stock Market {self.market.name} - Trade calendar check OK"
                )
        else:
            trade_calendar = zh_a_daily.get_a_stock_trade_date_hist()
            self.market.trade_calendar = trade_calendar
            self.market.save()

    def check_index_data_integrity(self, allow_update=False):

        local_index_list = StockIndex.objects(market=self.market)
        remote_index_list = zh_a_daily.get_zh_a_stock_index_spot()
        status = self.check_data_integrity(
            obj_type="index",
            local_data_list=local_index_list,
            remote_data_df=remote_index_list,
            hist_handler="get_hist_index_quote_data",
            allow_update=allow_update,
        )
        return status

    def check_stock_data_integrity(self, allow_update=False):
        # Resolve the frozen trading day before the universe snapshot so
        # default runs (no explicit --as-of-date) snapshot the right date.
        self.perform_date_check()
        if zh_a_daily.stock_history_uses_baostock():
            baostock_conn_mgr = BaostockInterfaceManager()
            baostock_conn_mgr.establish_baostock_conn()
        local_stock_list = IndividualStock.objects(market=self.market)
        remote_stock_list = zh_a_daily.get_zh_a_stock_universe(
            as_of_date=(
                self.most_recent_trading_day.strftime("%Y-%m-%d")
                if self.most_recent_trading_day
                else None
            )
        )
        status = self.check_data_integrity(
            obj_type="stock",
            local_data_list=local_stock_list,
            remote_data_df=remote_stock_list,
            hist_handler="get_hist_stock_quote_data",
            allow_update=allow_update,
        )
        return status

    def check_default_value_integrity(self):
        logger.info("Datahub - ChinaAStock - Checking default field value integrity")
        query_set = BasicStock.objects(active_status__exists=False)
        if len(query_set) > 0:
            query_set.update(set__active_status=0)
            logger.info(
                f"Datahub - ChinaAStock - Fixed {len(query_set)} unset active_status."
            )
        else:
            logger.info(
                "Datahub - ChinaAStock - Default field value integrity check OK."
            )
        updated_capability_count = 0
        for stock in BasicStock.objects():
            if self.sync_stock_data_capabilities(stock):
                updated_capability_count += 1
        if updated_capability_count:
            logger.info(
                "Datahub - ChinaAStock - Synced data capabilities for %s stocks.",
                updated_capability_count,
            )

    @staticmethod
    def _capabilities_for_code(code):
        payload = data_capability_helper.default_capability_payload(code)
        return StockDataCapabilities(**payload)

    @classmethod
    def sync_stock_data_capabilities(cls, stock_obj):
        expected = data_capability_helper.default_capability_payload(stock_obj.code)
        current = getattr(stock_obj, "data_capabilities", None)
        current_payload = {
            capability: getattr(current, capability, None) if current else None
            for capability in data_capability_helper.SUPPORTED_CAPABILITIES
        }
        if current_payload == expected:
            return False
        stock_obj.data_capabilities = cls._capabilities_for_code(stock_obj.code)
        stock_obj.save()
        return True

    def check_data_integrity(
        self,
        obj_type,
        local_data_list,
        remote_data_df,
        hist_handler,
        allow_update=False,
        bulk_insert=False,
    ):
        logger.info(
            f"Stock Market {self.market.name} - "
            f"Checking local {obj_type} data integrity, data update: {allow_update}"
        )
        status_code = "GOOD"
        status_msg = ""
        written_quote_count = 0
        validated_quote_count = 0
        required_quote_attempt_count = 0
        failed_quote_codes = []
        check_counter_dict = {
            "GOOD": 0,
            "UPD": 0,
            "INC": 0,
            "FULL": 0,
            "WARN": 0,
            "NEW": 0,
            "SKIP": 0,
        }
        upd_counter_dict = {
            "GOOD": 0,
            "UPD": 0,
            "INC": 0,
            "FULL": 0,
            "WARN": 0,
            "NEW": 0,
            "SKIP": 0,
        }
        self.perform_date_check()
        local_data_dict = {stock.code: stock for stock in local_data_list}
        local_data_num = local_data_list.count()
        remote_data_num = len(remote_data_df)
        if allow_update and remote_data_num == 0:
            self._partial_phase_result = {
                "pulled_count": 0,
                "written_count": 0,
                "validated_count": 0,
            }
            raise RuntimeError(f"Remote {obj_type} spot list is empty")

        def update_partial_phase_result():
            self._partial_phase_result = {
                "pulled_count": remote_data_num,
                "written_count": written_quote_count,
                "validated_count": validated_quote_count,
            }

        update_partial_phase_result()

        def is_allowed_suspension_gap(is_suspended, result):
            # A temporarily suspended stock may legitimately be STALE (its last
            # bar predates the as-of date, and the history pull over a fully
            # suspended window returns empty -> code FAIL). The missing row is
            # attributable to the suspension, so tolerate it regardless of the
            # pull's code; NO_DATA for a stock with no history still fails.
            return (
                is_suspended
                and result.get("freshness_status", result.get("status")) == STATUS_STALE
            )

        remote_code_set = (
            set(remote_data_df["code"].tolist()) if "code" in remote_data_df else set()
        )
        local_code_set = set(local_data_dict.keys())
        local_only_codes = sorted(local_code_set - remote_code_set)
        # prepare the progress bar
        prog_bar = progress_bar()

        # Batched quote-phase state: freshness refreshes and settled snapshot
        # writes are collected while the loop walks the remote rows and
        # flushed once afterwards, replacing per-stock aggregate/upsert round
        # trips with a handful of bulk calls. Flag decisions stay per-stock
        # and read each code's pre-run status document, which is exactly what
        # the previous sequential per-stock loop observed.
        pending_snapshot_writes: list[tuple[Any, Any]] = []
        pending_refresh_targets: dict[str, Any] = {}
        suspension_by_code: dict[str, bool] = {}
        consecutive_history_failures = 0
        latest_quote_date_by_code: dict[str, Any] = {}
        if allow_update:
            quote_status_docs = data_asset_status_helper.read_quote_status_map(
                sorted(local_code_set | remote_code_set)
            )
            latest_quote_date_by_code = {
                code: doc.latest_data_date for code, doc in quote_status_docs.items()
            }

        if local_only_codes:
            logger.warning(
                "Stock Market %s - Remote %s spot list is missing %s local codes. sample=%s",
                self.market.name,
                obj_type,
                len(local_only_codes),
                local_only_codes[:10],
            )
            if allow_update and obj_type == "stock":
                for code in local_only_codes:
                    stock_obj = local_data_dict[code]
                    if (
                        stock_obj.active_status != 0
                        or not data_capability_helper.stock_supports(
                            stock_obj, "daily_quote"
                        )
                    ):
                        continue
                    pending_refresh_targets[code] = stock_obj

        # check the existence of the stock list
        if local_data_num > 0:
            # check the existence of each stock
            for i, remote_stock_item in remote_data_df.iterrows():
                code = remote_stock_item["code"]
                name = remote_stock_item["name"]
                # stock_obj = local_data_list(code=code).first()
                stock_obj = local_data_dict.get(code)
                prog_bar_msg: str = ""
                if stock_obj:
                    if allow_update and stock_obj.active_status == 1:
                        logger.info(
                            "Restoring %s-%s to active because it is present in the spot list",
                            code,
                            stock_obj.name,
                        )
                        stock_obj.active_status = 0
                        stock_obj.save()
                    if allow_update:
                        self.sync_stock_data_capabilities(stock_obj)
                    # check the quote data freshness of each index
                    flag = self.check_data_freshness(
                        stock_obj,
                        precomputed_latest_date=latest_quote_date_by_code.get(code),
                    )
                    if flag != "SKIP":
                        is_temporarily_suspended = (
                            obj_type == "stock" and remote_stock_item["close"] == 0
                        )
                        check_counter_dict[flag] += 1
                        if allow_update:
                            self.perform_stock_name_check(stock_obj, name)
                            if flag in ["UPD", "INC", "FULL"]:
                                if not is_temporarily_suspended:
                                    required_quote_attempt_count += 1
                                prog_bar_msg = (
                                    f"Doing {flag} update for {code} - {name}"
                                )
                                if (
                                    flag == "UPD"
                                    and not is_temporarily_suspended
                                    and obj_type == "stock"
                                    and zh_a_daily.get_stock_universe_source()
                                    == "tushare"
                                ):
                                    # one trading day behind: queue the settled
                                    # as-of row from the market snapshot (only
                                    # for the tushare universe, whose
                                    # daily(trade_date) bar is the settled
                                    # as-of bar; the real-time spot path keeps
                                    # the history fallback). Rows are written
                                    # in one bulk call after the loop.
                                    pending_snapshot_writes.append(
                                        (stock_obj, remote_stock_item)
                                    )
                                    suspension_by_code[code] = False
                                else:
                                    hist_result = self.get_hist_quote_data(
                                        stock_obj=stock_obj,
                                        hist_quote_handler=hist_handler,
                                    )
                                    written_quote_count += hist_result.get(
                                        "written_count", 0
                                    )
                                    validated_quote_count += hist_result.get(
                                        "validated_count",
                                        hist_result.get("written_count", 0),
                                    )
                                    update_partial_phase_result()
                                    quote_validation_failed = (
                                        hist_result.get("code") != "GOOD"
                                        or hist_result.get("freshness_status")
                                        != STATUS_OK
                                    )
                                    if (
                                        quote_validation_failed
                                        and not is_allowed_suspension_gap(
                                            is_temporarily_suspended, hist_result
                                        )
                                    ):
                                        failed_quote_codes.append(code)
                                        consecutive_history_failures += 1
                                    else:
                                        consecutive_history_failures = 0
                                        if quote_validation_failed:
                                            logger.info(
                                                "Allowing stale quote for temporarily suspended stock %s",
                                                code,
                                            )
                                    if (
                                        consecutive_history_failures
                                        >= HISTORY_FAILURE_CIRCUIT_LIMIT
                                    ):
                                        raise RuntimeError(
                                            "Stock history source appears unavailable: "
                                            f"{consecutive_history_failures} consecutive "
                                            f"failed history pulls; sample={failed_quote_codes[-10:]}"
                                        )
                            elif flag in ["GOOD", "WARN"]:
                                pending_refresh_targets[code] = stock_obj
                                suspension_by_code[code] = is_temporarily_suspended
                    else:
                        logger.debug(
                            f"Stock Market {self.market.name} - Skipped quote data update for {code}-{name}"
                        )
                    upd_counter_dict[flag] += 1
                else:
                    is_temporarily_suspended = (
                        obj_type == "stock" and remote_stock_item["close"] == 0
                    )
                    prog_bar_msg = f"Get quote info for new stock {code} - {name}"
                    check_counter_dict["NEW"] += 1
                    if allow_update:
                        if not is_temporarily_suspended:
                            required_quote_attempt_count += 1
                        # create absent stock index and create data retrieve task.
                        new_stock_result = self.handle_new_stock(
                            obj_type=obj_type, code=code, name=name
                        )
                        written_quote_count += new_stock_result.get("written_count", 0)
                        validated_quote_count += new_stock_result.get(
                            "validated_count",
                            new_stock_result.get("written_count", 0),
                        )
                        update_partial_phase_result()
                        new_quote_validation_failed = new_stock_result.get(
                            "code"
                        ) not in ["GOOD", "SKIP"] or (
                            new_stock_result.get("code") == "GOOD"
                            and new_stock_result.get("freshness_status") != STATUS_OK
                        )
                        if (
                            new_quote_validation_failed
                            and not is_allowed_suspension_gap(
                                is_temporarily_suspended, new_stock_result
                            )
                        ):
                            failed_quote_codes.append(code)
                            consecutive_history_failures += 1
                        else:
                            consecutive_history_failures = 0
                        if (
                            consecutive_history_failures
                            >= HISTORY_FAILURE_CIRCUIT_LIMIT
                        ):
                            raise RuntimeError(
                                "Stock history source appears unavailable: "
                                f"{consecutive_history_failures} consecutive "
                                f"failed history pulls; sample={failed_quote_codes[-10:]}"
                            )
                        upd_counter_dict["NEW"] += 1
                prog_bar(i, remote_data_num, prog_bar_msg)
            msg_str = (
                f"Stock Market {self.market.name} - "
                f"Checked {local_data_num} local {obj_type} data with {remote_data_num} remote data，"
                f"- Up to date:          {check_counter_dict['GOOD']} "
                f"- One day behind:    {check_counter_dict['UPD']} "
                f"- Need incremental update: {check_counter_dict['INC']}"
                f"- No local data:  {check_counter_dict['FULL']} "
                f"- With warning: {check_counter_dict['WARN']}"
                f"- Skipped: {check_counter_dict['SKIP']}"
            )
            logger.info(msg_str)
            logger.info(
                "Data integrity summary: market=%s object_type=%s local_count=%s remote_count=%s check_counters=%s update_counters=%s",
                self.market.name,
                obj_type,
                local_data_num,
                remote_data_num,
                check_counter_dict,
                upd_counter_dict,
            )
            status_msg += msg_str
            # if allow_update:
            #     msg_str = (f'Stock Market {self.market.name} - update attempt for {obj_type} data are as follows: '
            #                f'- Update with spot data:  {upd_counter_dict["UPD"]} '
            #                f'- Incremental update: {upd_counter_dict["INC"]}'
            #                f'- Get full quote data:  {upd_counter_dict["FULL"]} '
            #                f'- New stock:  {upd_counter_dict["NEW"]} '
            #                f'- With warning: {upd_counter_dict["WARN"]}'
            #                f'- Skipped: {check_counter_dict["SKIP"]}'
            #                )
            #     logger.info(msg_str)
            #     status_msg += msg_str
            # else:
            #     msg_str = f'Stock Market {self.market.name} - no update attempt was made for {obj_type} data.'
            #     logger.info(msg_str)
            #     status_msg += msg_str
        else:
            if allow_update:
                for i, remote_stock_item in remote_data_df.iterrows():
                    code = remote_stock_item["code"]
                    name = remote_stock_item["name"]
                    is_temporarily_suspended = (
                        obj_type == "stock" and remote_stock_item["close"] == 0
                    )
                    if not is_temporarily_suspended:
                        required_quote_attempt_count += 1
                    new_stock_result = self.handle_new_stock(
                        obj_type=obj_type, code=code, name=name
                    )
                    written_quote_count += new_stock_result.get("written_count", 0)
                    validated_quote_count += new_stock_result.get(
                        "validated_count",
                        new_stock_result.get("written_count", 0),
                    )
                    update_partial_phase_result()
                    new_quote_validation_failed = new_stock_result.get("code") not in [
                        "GOOD",
                        "SKIP",
                    ] or (
                        new_stock_result.get("code") == "GOOD"
                        and new_stock_result.get("freshness_status") != STATUS_OK
                    )
                    if new_quote_validation_failed and not is_allowed_suspension_gap(
                        is_temporarily_suspended, new_stock_result
                    ):
                        failed_quote_codes.append(code)
                        consecutive_history_failures += 1
                    else:
                        consecutive_history_failures = 0
                    if consecutive_history_failures >= HISTORY_FAILURE_CIRCUIT_LIMIT:
                        raise RuntimeError(
                            "Stock history source appears unavailable: "
                            f"{consecutive_history_failures} consecutive "
                            f"failed history pulls; sample={failed_quote_codes[-10:]}"
                        )
                    check_counter_dict["NEW"] += 1
                    upd_counter_dict["NEW"] += 1
                    prog_bar(i, remote_data_num)

        snapshot_written_count = 0
        snapshot_validated_count = 0
        if allow_update and (pending_snapshot_writes or pending_refresh_targets):
            snapshot_written_count, snapshot_statuses = (
                self._flush_batched_quote_updates(
                    pending_snapshot_writes, pending_refresh_targets
                )
            )
            written_quote_count += snapshot_written_count
            snapshot_codes = {
                stock_obj.code for stock_obj, _ in pending_snapshot_writes
            }
            for code, status_value in snapshot_statuses.items():
                if status_value == STATUS_OK:
                    if code in snapshot_codes:
                        snapshot_validated_count += 1
                    continue
                if is_allowed_suspension_gap(
                    suspension_by_code.get(code, False), {"status": status_value}
                ):
                    logger.info(
                        "Allowing stale quote for temporarily suspended stock %s",
                        code,
                    )
                    continue
                failed_quote_codes.append(code)
            validated_quote_count += snapshot_validated_count
            update_partial_phase_result()

        status = {
            "code": status_code,
            "msg": status_msg,
            "pulled_count": remote_data_num,
            "written_count": written_quote_count,
            "validated_count": validated_quote_count,
        }
        self._partial_phase_result = status
        if (
            allow_update
            and obj_type == "stock"
            and local_data_num == 0
            and required_quote_attempt_count > 0
            and validated_quote_count == 0
        ):
            raise RuntimeError(
                "Stock quote refresh attempted updates but wrote zero quote rows "
                "(validated zero source rows)"
            )
        if allow_update and failed_quote_codes:
            raise RuntimeError(
                "Quote validation failed for "
                f"{len(failed_quote_codes)} symbols; sample={failed_quote_codes[:10]}"
            )
        return status

    def check_data_freshness(self, stock_obj, precomputed_latest_date=None):
        update_flag = ""
        if not data_capability_helper.stock_supports(stock_obj, "daily_quote"):
            return "SKIP"
        if precomputed_latest_date is not None:
            # Pre-read in one batched query before the loop. A code with no
            # status document (or a null latest date) passes None here, which
            # falls back to the single-code read below -- both return None.
            most_recent_quote_date = precomputed_latest_date
        else:
            most_recent_quote_date = data_asset_status_helper.read_asset_latest_date(
                code=stock_obj.code,
                object_type=stock_obj.object_type,
                asset_type="quote",
                asset_name="daily_quote",
            )
        if stock_obj.active_status == 0:
            if most_recent_quote_date:
                # determine time difference
                time_diff = trading_day_helper.determine_trading_date_diff(
                    self.market.trade_calendar,
                    most_recent_quote_date,
                    self.most_recent_trading_day,
                )
                # create data update task
                if time_diff == 0:
                    update_flag = "GOOD"
                elif time_diff == 1:
                    update_flag = "UPD"  # Just update it with the most recent daily quote (difference of only 1 day)
                elif time_diff > 1:
                    # Need the whole history quote data to do the incremental update (difference of more than 1 day)
                    update_flag = "INC"
                else:
                    logger.warning(
                        f"Stock Market {self.market.name} - {stock_obj.code} Quote date ahead of time!"
                    )
                    update_flag = "WARN"
            else:
                # no quote data at all
                update_flag = "FULL"
        else:
            update_flag = "SKIP"
        return update_flag

    def handle_new_stock(self, obj_type, code, name):
        """
        handle new stock or index, will create a master data and a task, which get its quote data
        :param obj_type: stock or index
        :param code:
        :param name:
        :return:
        """
        logger.info(
            f"Stock Market {self.market.name} - Initializing local data for {code}-{name}"
        )
        new_stock_obj = None
        object_type = ""
        quote_result = {"written_count": 0}
        if obj_type == "stock":
            new_stock_obj = IndividualStock()
            object_type = "individual_stock"
        elif obj_type == "index":
            new_stock_obj = StockIndex()
            object_type = "stock_index"
        else:
            logger.error(
                f"Stock Market {self.market.name} - Invalid category {obj_type}"
            )
        if code:
            new_stock_obj.code = code
            new_stock_obj.name = name
            new_stock_obj.object_type = object_type
            new_stock_obj.market = self.market
            new_stock_obj.data_capabilities = self._capabilities_for_code(code)
            try:
                new_stock_obj.save()
            except NotUniqueError:
                # A concurrent quote run (e.g. startup quote catch-up racing a
                # one-shot bootstrap) may have created this master record
                # already. Reuse the existing record instead of crashing the
                # whole run.
                logger.warning(
                    "Stock Market %s - Master data for %s-%s already exists, "
                    "reusing existing record",
                    self.market.name,
                    code,
                    name,
                )
                new_stock_obj = new_stock_obj.__class__.objects(code=code).first()
                if new_stock_obj is None:
                    raise RuntimeError(
                        f"Stock Market {self.market.name} - master data for "
                        f"{code}-{name} vanished after duplicate save"
                    )
            if obj_type == "stock" and data_capability_helper.stock_supports(
                new_stock_obj, "daily_quote"
            ):
                quote_result = self.get_hist_stock_quote_data(
                    code=code,
                    end_date=self.most_recent_trading_day.strftime("%Y-%m-%d"),
                )
            elif obj_type == "index":
                quote_result = self.get_hist_index_quote_data(
                    code=code,
                    end_date=self.most_recent_trading_day.strftime("%Y-%m-%d"),
                )
            else:
                quote_result = {
                    "code": "SKIP",
                    "message": "daily_quote is not supported for this stock",
                    "written_count": 0,
                }
            # task_kwarg = {
            #     'code': code
            # }
            # task_controller.create_task(name=task_name,
            #                             callback_package='datahub',
            #                             callback_module='processors',
            #                             callback_object='ChinaAStock',
            #                             callback_handler=handler,
            #                             kwargs=task_kwarg)
        else:
            logger.warning(
                f"Stock Market {self.market.name} - Empty stock code: {new_stock_obj.name}"
            )
        return quote_result

    @staticmethod
    def handle_new_quote(
        stock_obj, col_name_list, quote_row, quote_date=None, save_quote=False
    ):
        new_quote = StockDailyQuote()
        for col in col_name_list:
            setattr(new_quote, col, quote_row[col])
        new_quote.code = stock_obj.code
        new_quote.stock = stock_obj
        if quote_date:
            new_quote.date = quote_date
        if save_quote:
            try:
                new_quote.save()
                saved_count = 1
                data_asset_status_helper.refresh_quote_status(
                    stock_obj=stock_obj,
                    quote_model=StockDailyQuote,
                    last_job_name="stock_quote_sync",
                )
            except NotUniqueError:
                existing_quote = (
                    StockDailyQuote.objects(code=stock_obj.code)
                    .order_by("-date")
                    .first()
                )
                current_asset_date = data_asset_status_helper.read_asset_latest_date(
                    code=stock_obj.code,
                    object_type=stock_obj.object_type,
                    asset_type="quote",
                    asset_name="daily_quote",
                )
                saved_count = 0
                logger.warning(
                    "%s-%s-quote-daily_quote-%s encountered NotUniqueError when trying to upsert quote data. "
                    "existing_latest_quote=%s current_asset_date=%s",
                    stock_obj.code,
                    stock_obj.name,
                    quote_date,
                    existing_quote.date if existing_quote else None,
                    current_asset_date,
                )
        else:
            saved_count = 0
        stock_obj.save()
        return new_quote, saved_count

    def get_hist_quote_data(self, stock_obj, hist_quote_handler, force_upd=False):
        start_date = None
        start_date_str = None
        most_recent_quote_date = data_asset_status_helper.read_asset_latest_date(
            code=stock_obj.code,
            object_type=stock_obj.object_type,
            asset_type="quote",
            asset_name="daily_quote",
        )
        if most_recent_quote_date:
            start_date = trading_day_helper.next_trading_day(
                self.market.trade_calendar, most_recent_quote_date
            )
            start_date_str = start_date.strftime("%Y-%m-%d")
        kwarg_dict = {
            "code": stock_obj.code,
        }
        # logger.info(task_name)
        if start_date:
            kwarg_dict["start_date"] = start_date.strftime("%Y-%m-%d")
        func = getattr(self, hist_quote_handler)
        end_date_str = self.most_recent_trading_day.strftime("%Y-%m-%d")
        result = func(
            code=stock_obj.code,
            start_date=start_date_str,
            end_date=end_date_str,
        )
        if result["code"] != "GOOD":
            logger.warning(
                f"Something went wrong when trying to get historic quote data for {stock_obj.code} - {stock_obj.name}\n{result['message']}"
            )
        return result

        # task_controller.create_task(name=task_name,
        #                             callback_package='datahub',
        #                             callback_module='markets',
        #                             callback_object='zh_a_stock_market',
        #                             callback_handler=hist_quote_handler,
        #                             kwargs=kwarg_dict)

    def _flush_batched_quote_updates(
        self,
        pending_snapshot_writes: list[tuple[Any, Any]],
        pending_refresh_targets: dict[str, Any],
    ) -> tuple[int, dict[str, str]]:
        """Write queued snapshot rows and refresh quote statuses in batches.

        Consistency ordering: every queued quote write is committed first,
        then per-code stats are aggregated from the persisted quotes, and
        only then are the status records upserted. Freshness therefore keeps
        being derived from committed Mongo data, exactly like the per-stock
        write -> aggregate -> upsert sequence it replaces.

        Returns (written_count, status_by_code).
        """
        written_count = 0
        snapshot_stock_by_code = {
            stock_obj.code: stock_obj for stock_obj, _ in pending_snapshot_writes
        }

        if pending_snapshot_writes:
            operations = [
                _build_stock_quote_upsert_operation(
                    stock_obj,
                    pandas.Series(
                        _build_snapshot_quote_row(
                            snapshot_row,
                            self.most_recent_trading_day,
                            stock_obj.code,
                        )
                    ),
                )
                for stock_obj, snapshot_row in pending_snapshot_writes
            ]
            for chunk in _chunked(operations, STATUS_WRITE_CHUNK_SIZE):
                result = StockDailyQuote._get_collection().bulk_write(
                    chunk, ordered=False
                )
                written_count += result.upserted_count + result.modified_count

        statuses: dict[str, str] = {}
        codes = list(pending_refresh_targets) + [
            code
            for code in snapshot_stock_by_code
            if code not in pending_refresh_targets
        ]
        if not codes:
            return written_count, statuses

        stats_by_code = data_asset_status_helper.aggregate_stats_by_code(
            StockDailyQuote._get_collection(),
            match={"code": {"$in": codes}},
            code_field="code",
        )
        calculated_at = datetime.datetime.now()
        records = []
        for code in codes:
            stock_obj = (
                pending_refresh_targets.get(code) or snapshot_stock_by_code[code]
            )
            records.append(
                data_asset_status_helper.build_quote_status_record(
                    code=code,
                    object_type=getattr(stock_obj, "object_type", "individual_stock"),
                    stats_row=stats_by_code.get(code),
                    expected_latest_date=self.most_recent_trading_day,
                    trade_calendar=self.market.trade_calendar,
                    last_job_name="stock_quote_sync",
                    calculated_at=calculated_at,
                )
            )
        data_asset_status_helper.bulk_upsert_asset_status(records)
        statuses = {record["code"]: record["status"] for record in records}
        return written_count, statuses

    # @performance_helper.func_performance_timer
    def get_hist_stock_quote_data(
        self, code, start_date=None, end_date=None, force_insert=False
    ):
        status_code = "GOOD"
        status_msg = None
        written_count = 0
        validated_count = 0
        freshness_status = None
        stock_obj = (
            IndividualStock.objects(code=code)
            .only("code", "name", "object_type", "data_capabilities")
            .first()
        )
        if stock_obj and not data_capability_helper.stock_supports(
            stock_obj, "daily_quote"
        ):
            status_code = "SKIP"
            status_msg = (
                f"Skip historical quote update for unsupported daily_quote stock {code}"
            )
            logger.info("Datahub - ChinaAStock - %s", status_msg)
            return {
                "code": status_code,
                "message": status_msg,
                "written_count": written_count,
                "validated_count": validated_count,
                "freshness_status": freshness_status,
            }
        try:
            if not stock_obj:
                stock_obj = (
                    IndividualStock.objects(code=code)
                    .only("code", "name", "object_type", "data_capabilities")
                    .first()
                )
            quote_df = zh_a_daily.get_zh_a_stock_hist_daily_quote(
                code, start_date=start_date, end_date=end_date
            )
            if isinstance(quote_df, pandas.DataFrame):
                if not quote_df.empty:
                    operations = [
                        _build_stock_quote_upsert_operation(stock_obj, row)
                        for _, row in quote_df.iterrows()
                    ]
                    result = StockDailyQuote._get_collection().bulk_write(
                        operations,
                        ordered=False,
                    )
                    written_count = result.upserted_count + result.modified_count
                    validated_count = len(operations)
                    expected_latest_date = (
                        datetime.datetime.strptime(end_date, "%Y-%m-%d")
                        if end_date
                        else None
                    )
                    freshness = data_asset_status_helper.refresh_quote_status(
                        stock_obj=stock_obj,
                        quote_model=StockDailyQuote,
                        last_job_name="stock_quote_sync",
                        expected_latest_date=expected_latest_date,
                        trade_calendar=self.market.trade_calendar,
                    )
                    freshness_status = freshness["status"]
                    stock_obj.save()
                else:
                    status_code = "FAIL"
                    status_msg = "No available data for update"
                    logger.warning(
                        "Stock Market ChinaAStock - Failed to fetch historical quote "
                        f"data for {code}-{stock_obj.name}"
                    )
                    freshness = data_asset_status_helper.refresh_quote_status(
                        stock_obj=stock_obj,
                        quote_model=StockDailyQuote,
                        last_job_name="stock_quote_sync",
                        expected_latest_date=(
                            datetime.datetime.strptime(end_date, "%Y-%m-%d")
                            if end_date
                            else None
                        ),
                        trade_calendar=self.market.trade_calendar,
                    )
                    freshness_status = freshness["status"]
                    time.sleep(0.5)  # reduce the query frequency
            else:
                status_code = "FAIL"
                status_msg = "No available data for update"
                freshness = data_asset_status_helper.refresh_quote_status(
                    stock_obj=stock_obj,
                    quote_model=StockDailyQuote,
                    last_job_name="stock_quote_sync",
                    expected_latest_date=(
                        datetime.datetime.strptime(end_date, "%Y-%m-%d")
                        if end_date
                        else None
                    ),
                    trade_calendar=self.market.trade_calendar,
                )
                freshness_status = freshness["status"]
        # except KeyError:
        #     status_code = 'FAIL'
        #     status_msg = 'the interface did not return valid dataframe, possibly due to no quote data'
        finally:
            pass
        # except Exception as e:
        #     status_code = 'ERR'
        #     status_msg = ';'.join(traceback.format_exception(e))
        status = {
            "code": status_code,
            "message": status_msg,
            "written_count": written_count,
            "validated_count": validated_count,
            "freshness_status": freshness_status,
        }
        return status

    def get_hist_index_quote_data(
        self,
        code,
        start_date=None,
        end_date=None,
        force_insert=False,
        bulk_insert=True,
    ):
        """

        :param code:
        :param start_date:
        :param end_date:
        :param force_insert: only works when bulk insert is false!
        :param bulk_insert:
        :return:
        """
        status_code = "GOOD"
        status_msg = None
        written_count = 0
        validated_count = 0
        freshness_status = None
        try:
            index_obj = StockIndex.objects(code=code).only("code", "name").first()
            quote_df = zh_a_daily.get_zh_a_index_hist_daily_quote(
                code, start_date=start_date, end_date=end_date
            )
            if index_obj:
                if not quote_df.empty:
                    bulk_insert_list = []
                    for i, row in quote_df.iterrows():
                        if bulk_insert:
                            bulk_insert_list.append(
                                _build_index_quote_upsert_operation(index_obj, row)
                            )
                        else:
                            daily_quote = StockDailyQuote()
                            daily_quote.code = index_obj.code
                            daily_quote.stock = index_obj
                            daily_quote.date = row["date"]
                            daily_quote.open = row["open"]
                            daily_quote.close = row["close"]
                            daily_quote.high = row["high"]
                            daily_quote.low = row["low"]
                            daily_quote.volume = row["volume"]
                            daily_quote.save()
                    if bulk_insert:
                        result = StockDailyQuote._get_collection().bulk_write(
                            bulk_insert_list,
                            ordered=False,
                        )
                        written_count = result.upserted_count
                    else:
                        written_count = len(quote_df)
                    validated_count = len(quote_df)
                    freshness = data_asset_status_helper.refresh_quote_status(
                        stock_obj=index_obj,
                        quote_model=StockDailyQuote,
                        last_job_name="index_market_sync",
                        expected_latest_date=(
                            datetime.datetime.strptime(end_date, "%Y-%m-%d")
                            if end_date
                            else None
                        ),
                        trade_calendar=self.market.trade_calendar,
                    )
                    freshness_status = freshness["status"]
                    index_obj.save(force_insert=force_insert)
                else:
                    status_code = "FAIL"
                    status_msg = "No available data for update"
            else:
                status_code = "FAIL"
                status_msg = "INDEX CODE CAN NOT BE FOUND IN LOCAL DB"
            # time.sleep(0.5)    # reduce the query frequency
        except KeyError:
            status_code = "FAIL"
            status_msg = "the interface did not return valid dataframe, possibly due to no quote data"
        # except Exception as e:
        #     status_code = 'ERR'
        #     status_msg = ';'.join(traceback.format_exception(e))
        status = {
            "code": status_code,
            "message": status_msg,
            "written_count": written_count,
            "validated_count": validated_count,
            "freshness_status": freshness_status,
        }
        return status

    def perform_date_check(self):
        latest_complete_trading_day = (
            trading_day_helper.determine_latest_complete_trading_date(
                self.market.trade_calendar,
                self.run_started_at,
            )
        )
        if self.most_recent_trading_day is None:
            self.most_recent_trading_day = latest_complete_trading_day
        if self.most_recent_trading_day is None or latest_complete_trading_day is None:
            raise RuntimeError("No complete trading day is available for quote refresh")
        if self.explicit_as_of_date and (
            self.most_recent_trading_day not in self.market.trade_calendar
            or self.most_recent_trading_day > latest_complete_trading_day
        ):
            raise ValueError(
                "Explicit as_of_date must be a completed market trading day"
            )
        logger.info(
            "Stock Market %s - Date check resolved today=%s most_recent_trading_day=%s calendar_tail=%s",
            self.market.name,
            self.today,
            self.most_recent_trading_day,
            self.market.trade_calendar[-3:]
            if self.market and self.market.trade_calendar
            else [],
        )

    @staticmethod
    def perform_stock_name_check(stock_obj, curr_name):
        if stock_obj.name != curr_name and curr_name not in stock_obj.pre_name:
            stock_obj.pre_name.append(stock_obj.name)
            stock_obj.name = curr_name
            stock_obj.save()

    @staticmethod
    def update_active_status(inactive_item_dict, local_data_collection):

        # update inactive status
        inactive_code_list = list(inactive_item_dict.keys())
        inactive_item_list = list(
            local_data_collection(code__in=inactive_code_list, active_status=0)
        )
        for item in inactive_item_list:
            logger.info(
                f"No quote data for {item.code}-{item.name}, setting active status to 1(inactive)"
            )
            item.active_status = 1
            item.save()

    def mark_inactive_stocks(self, allow_update=False):
        updated_count = BasicStock.objects(
            name__icontains="退", active_status=0
        ).update(set__active_status=2)
        logger.info(f"Marked {updated_count} stocks as inactive.")
        return {"pulled_count": 0, "written_count": updated_count}

    def update_fq_factor(self, allow_update=False):
        logger.info("Datahub - ChinaAStock - Updating FQ factor after stock quote sync")
        service = FQFactorService()
        return service.update_market(market=self.market)

    def update_ma_factor(self, allow_update=False):
        logger.info("Datahub - ChinaAStock - Updating MA factors after FQ factor sync")
        service = MovingAverageFactorService()
        return service.update_market(market=self.market)


if __name__ == "__main__":
    from app.lib.datahub.data_source import interface

    interface.baostock_interface.establish_baostock_conn()
    obj = ChinaAStock()
    o = obj.get_hist_index_quote_data(
        code="sh000061", force_insert=True, bulk_insert=False
    )
    print(o)
