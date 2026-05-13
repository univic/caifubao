import time
import logging
import datetime
import traceback

import pandas
from pymongo import UpdateOne
from mongoengine import NotUniqueError, BulkWriteError
from app.lib.datahub.data_source.handler import zh_a_daily
from app.model.stock import (
    FinanceMarket,
    BasicStock,
    StockIndex,
    IndividualStock,
    StockDailyQuote,
    StockDataCapabilities,
)

# from app.lib.task_controller import task_controller
from app.lib.datahub.data_source.interface.baostock_interface import (
    BaostockInterfaceManager,
)
from app.lib.factor_factory import FQFactorService, MovingAverageFactorService
from app.lib.signal_factory import MovingAverageSignalService
from app.lib.scoring_engine.scoring_service import StockScoringService
from app.lib.utilities.progress_bar import progress_bar
from app.lib.utilities import trading_day_helper
from app.lib.utilities import data_asset_status_helper
from app.lib.utilities import data_capability_helper

logger = logging.getLogger(__name__)


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
            "_cls": raw_doc["_cls"],
            "code": raw_doc["code"],
            "date": raw_doc["date"],
        },
        {"$setOnInsert": raw_doc},
        upsert=True,
    )


class ChinaAStock(object):
    def __init__(self):
        self.market_name = "ChinaAStock"
        self.market_code = "ZH-A"
        self.today = datetime.date.today()
        self.most_recent_trading_day = None
        self.market = FinanceMarket.objects(name="ChinaAStock").first()
        self.trade_calendar = None
        self.result: dict = {
            "code": "GOOD",
            "message": "",
        }
        self.last_job_summary = None

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
        phases = [
            ("check_prerequisite", self.check_prerequisite),
            ("check_stock_data_integrity", self.check_stock_data_integrity),
            ("mark_inactive_stocks", self.mark_inactive_stocks),
            ("update_fq_factor", self.update_fq_factor),
            ("update_ma_factor", self.update_ma_factor),
            ("update_signals", self.update_signals),
            ("update_scoring", self.update_scoring),
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
            "phase_stats": {},
        }
        current_phase = None
        try:
            for phase_name, phase_func in phases:
                current_phase = phase_name
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
                logger.info(
                    "Datahub phase completed: market=%s job=%s phase=%s pulled=%s written=%s",
                    self.market_name,
                    job_name,
                    phase_name,
                    phase_summary["pulled_count"],
                    phase_summary["written_count"],
                )
            return summary
        except Exception:
            summary["status"] = "FAILED"
            summary["failed_phase"] = current_phase
            raise
        finally:
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
            return {"pulled_count": 0, "written_count": 0}
        return {
            "pulled_count": phase_result.get("pulled_count", 0),
            "written_count": phase_result.get("written_count", 0),
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
        baostock_conn_mgr = BaostockInterfaceManager()
        baostock_conn_mgr.establish_baostock_conn()
        local_stock_list = IndividualStock.objects(market=self.market)
        remote_stock_list = zh_a_daily.get_zh_a_stock_spot()
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
        remote_data_col_list = remote_data_df.columns.tolist()
        remote_code_set = (
            set(remote_data_df["code"].tolist()) if "code" in remote_data_df else set()
        )
        local_code_set = set(local_data_dict.keys())
        local_only_codes = sorted(local_code_set - remote_code_set)
        # prepare quote list for bulk insert
        new_quote_instance_list = []
        inactive_item_dict = {}
        # prepare the progress bar
        prog_bar = progress_bar()

        if local_only_codes:
            logger.warning(
                "Stock Market %s - Remote %s spot list is missing %s local codes. sample=%s",
                self.market.name,
                obj_type,
                len(local_only_codes),
                local_only_codes[:10],
            )

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
                    if allow_update:
                        self.sync_stock_data_capabilities(stock_obj)
                    # check the quote data freshness of each index
                    flag = self.check_data_freshness(stock_obj)
                    if flag != "SKIP":
                        # if close price is not 0, add the item to the inactive list,
                        if remote_stock_item["close"] == 0:
                            inactive_item_dict[code] = stock_obj
                            logger.info(
                                f"Adding {code}-{stock_obj.name} to inactive list"
                            )
                        check_counter_dict[flag] += 1
                        if allow_update:
                            self.perform_stock_name_check(stock_obj, name)
                            if flag == "UPD":
                                prog_bar_msg = f"Updating quote info for {code} - {name} with spot data"
                                quote_date = self.most_recent_trading_day
                                save_quote = not bulk_insert
                                new_quote, saved_count = self.handle_new_quote(
                                    stock_obj,
                                    remote_data_col_list,
                                    remote_stock_item,
                                    quote_date,
                                    save_quote=save_quote,
                                )
                                new_quote_instance_list.append(new_quote)
                                written_quote_count += saved_count
                            elif flag in ["INC", "FULL"]:
                                prog_bar_msg = (
                                    f"Doing {flag} update for {code} - {name}"
                                )
                                hist_result = self.get_hist_quote_data(
                                    stock_obj=stock_obj, hist_quote_handler=hist_handler
                                )
                                written_quote_count += hist_result.get(
                                    "written_count", 0
                                )
                    else:
                        logger.debug(
                            f"Stock Market {self.market.name} - Skipped quote data update for {code}-{name}"
                        )
                    upd_counter_dict[flag] += 1
                else:
                    prog_bar_msg = f"Get quote info for new stock {code} - {name}"
                    check_counter_dict["NEW"] += 1
                    if allow_update:
                        # create absent stock index and create data retrieve task.
                        new_stock_result = self.handle_new_stock(
                            obj_type=obj_type, code=code, name=name
                        )
                        written_quote_count += new_stock_result.get("written_count", 0)
                        upd_counter_dict["NEW"] += 1
                prog_bar(i, remote_data_num, prog_bar_msg)
            if bulk_insert:
                # do bulk insert
                StockDailyQuote.objects.insert(new_quote_instance_list, load_bulk=False)
                written_quote_count += len(new_quote_instance_list)
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
            self.update_active_status(inactive_item_dict, local_data_list)
        else:
            if allow_update:
                for i, remote_stock_item in remote_data_df.iterrows():
                    code = remote_stock_item["code"]
                    name = remote_stock_item["name"]
                    self.handle_new_stock(obj_type=obj_type, code=code, name=name)
                    prog_bar(i, remote_data_num)
        status = {
            "code": status_code,
            "msg": status_msg,
            "pulled_count": remote_data_num,
            "written_count": written_quote_count,
        }
        return status

    def check_data_freshness(self, stock_obj):
        update_flag = ""
        if not data_capability_helper.stock_supports(stock_obj, "daily_quote"):
            return "SKIP"
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
            new_stock_obj.save()
            if obj_type == "stock" and data_capability_helper.stock_supports(
                new_stock_obj, "daily_quote"
            ):
                quote_result = self.get_hist_stock_quote_data(code=code)
            elif obj_type == "index":
                quote_result = self.get_hist_index_quote_data(code=code)
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
        result = func(code=stock_obj.code, start_date=start_date_str)
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

    # @performance_helper.func_performance_timer
    def get_hist_stock_quote_data(self, code, start_date=None, force_insert=False):
        status_code = "GOOD"
        status_msg = None
        written_count = 0
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
            }
        try:
            if not stock_obj:
                stock_obj = (
                    IndividualStock.objects(code=code)
                    .only("code", "name", "object_type", "data_capabilities")
                    .first()
                )
            quote_df = zh_a_daily.get_zh_a_stock_hist_daily_quote(
                code, start_date=start_date
            )
            if isinstance(quote_df, pandas.DataFrame):
                if not quote_df.empty:
                    # get column names of the df
                    bulk_insert_list = []
                    col_name_list = quote_df.columns.tolist()
                    date_of_quote = quote_df["date"].max()
                    for i, row in quote_df.iterrows():
                        daily_quote = StockDailyQuote()
                        daily_quote.code = stock_obj.code
                        daily_quote.stock = stock_obj
                        for col in col_name_list:
                            setattr(daily_quote, col, row[col])
                        daily_quote.amplitude = round(
                            daily_quote.high - daily_quote.low, 2
                        )
                        daily_quote.change_amount = round(
                            daily_quote.close - daily_quote.previous_close, 2
                        )

                        bulk_insert_list.append(daily_quote)
                    try:
                        # do bulk insert
                        StockDailyQuote.objects.insert(
                            bulk_insert_list, load_bulk=False
                        )
                        written_count = len(bulk_insert_list)
                        data_asset_status_helper.refresh_quote_status(
                            stock_obj=stock_obj,
                            quote_model=StockDailyQuote,
                            last_job_name="stock_quote_sync",
                        )
                        stock_obj.save()
                    except NotUniqueError:
                        logger.warning(
                            f"{stock_obj.code}-{stock_obj.name}-{'quote'}-{'daily_quote'}-{date_of_quote} encountered NotUniqueError when trying to upsert freshness meta"
                        )
                    except BulkWriteError as e:
                        logger.warning(
                            f"{stock_obj.code}-{stock_obj.name}-{'quote'}-{'daily_quote'}-{date_of_quote} encountered BulkWriteError {traceback.format_exception(e)} when trying to upsert freshness meta"
                        )
                else:
                    status_code = "FAIL"
                    status_msg = "No available data for update"
                    logger.warning(
                        f"Stock Market ChinaAStock - Failed to fetch historical quote data "
                        f"for {code}-{stock_obj.name}, changing active status to 1(inactive)"
                    )
                    stock_obj.active_status = 1
                    stock_obj.save()
                    time.sleep(0.5)  # reduce the query frequency
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
        }
        return status

    @staticmethod
    def get_hist_index_quote_data(
        code, start_date=None, end_date=None, force_insert=False, bulk_insert=True
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
        try:
            index_obj = StockIndex.objects(code=code).only("code", "name").first()
            quote_df = zh_a_daily.get_zh_a_index_hist_daily_quote(
                code, start_date=start_date
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
                    data_asset_status_helper.refresh_quote_status(
                        stock_obj=index_obj,
                        quote_model=StockDailyQuote,
                        last_job_name="index_market_sync",
                    )
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
        }
        return status

    def perform_date_check(self):
        # determine the closest trading day
        today = datetime.date.today()
        if self.today != today or self.most_recent_trading_day is None:
            self.today = today
            self.most_recent_trading_day = (
                trading_day_helper.determine_closest_trading_date(
                    self.market.trade_calendar
                )
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

    def update_signals(self, allow_update=False):
        logger.info("Datahub - ChinaAStock - Updating signals after MA factors sync")
        service = MovingAverageSignalService()
        return service.update_market(market=self.market)

    def update_scoring(self, allow_update=False):
        logger.info("Datahub - ChinaAStock - Updating stock scores")
        service = StockScoringService()
        # By default score for today/closest trading day
        result = service.score_all_stocks()
        return {
            "pulled_count": result["scored_count"],
            "written_count": result["scored_count"],
        }


if __name__ == "__main__":
    from app.lib.datahub.data_source import interface

    interface.baostock_interface.establish_baostock_conn()
    obj = ChinaAStock()
    o = obj.get_hist_index_quote_data(
        code="sh000061", force_insert=True, bulk_insert=False
    )
    print(o)
