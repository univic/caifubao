import logging
import datetime
import pandas as pd
from app.lib.db_watcher.mongoengine_tool import mongo_watcher
from app.model.factor import FactorDataEntry
from app.model.stock import BasicStock, StockDailyQuote
from app.model.data_freshness import DataFreshnessMeta


logger = logging.getLogger(__name__)


class DataIntegrityKeeper(object):
    def __init__(self):
        self.module_name = "DataIntegrityKeeper"

    def run(self):
        logger.info(f"Starting {self.module_name}")
        self.keep_factor_distinct_by_date()

    def check_stock_quote_missing(self):
        """
        检查股票行情数据是否存在缺失
        """
        logger.info(f"{self.module_name} - Running processor check_stock_quote_missing")
        from app.model.stock import FinanceMarket, StockDailyQuote, BasicStock
        from app.utilities import freshness_meta_helper

        # 找到名为 ChinaAStock 的 FinanceMarket 对象
        market = FinanceMarket.objects(name="ChinaAStock").first()
        if not market:
            logger.error("未找到名为 ChinaAStock 的 FinanceMarket 对象")
            return

        # 取出 trade_calendar 属性
        trade_calendar = market.trade_calendar
        if not trade_calendar:
            logger.error("ChinaAStock 的 trade_calendar 属性为空")
            return

        # 获取所有股票
        stocks = BasicStock.objects()

        for stock in stocks:
            # 获取该股票的所有日线行情数据
            quotes = StockDailyQuote.objects(code=stock.code).order_by("+date")
            if not quotes:
                continue

            # 获取最早和最新的行情日期
            earliest_quote_date = quotes[0].date
            latest_quote_date = quotes[-1].date

            # 确定在最早和最新日期之间的交易日
            start_index = trade_calendar.index(earliest_quote_date)
            end_index = trade_calendar.index(latest_quote_date)
            relevant_trade_days = trade_calendar[start_index : end_index + 1]

            # 构建已有的行情日期集合
            quote_dates = set([quote.date for quote in quotes])

            # 检查是否有中间缺失的行情信息
            missing_dates = [
                date for date in relevant_trade_days if date not in quote_dates
            ]
            if missing_dates:
                # 找到第一次缺失断点出现前的最后一个交易日
                first_missing_index = relevant_trade_days.index(missing_dates[0])
                last_valid_date = relevant_trade_days[first_missing_index - 1]

                # 记录日志
                logger.warning(
                    f"股票 {stock.code} - {stock.name} 存在{len(missing_dates)}中间缺失的行情信息，缺失日期：{missing_dates}，将 freshness_meta 日期重置到 {last_valid_date}"
                )

                # 重置 freshness_meta 日期
                freshness_meta_helper.upsert_freshness_meta(
                    code=stock.code,
                    object_type=stock.object_type,
                    meta_type="quote",
                    meta_name="daily_quote",
                    dt=last_valid_date,
                )

    # ... existing code ...

    def check_meta_consistency(self):
        """
        检查因子数据的meta数据是否一致
        """
        logger.info(f"{self.module_name} - Running processor check_meta_consistency")
        basic_stocks = BasicStock.objects.all()
        data = [
            {
                "code": stock.code,
                "name": stock.name,
                "object_type": stock.object_type,
                "active_status": stock.active_status,
            }
            for stock in basic_stocks
        ]
        df_stock_list = pd.DataFrame(data)

        logger.info(f"{self.module_name} - Fetching latest stock daily quote dates")
        aggregate_pipeline = [
            {"$project": {"code": 1, "date": 1, "_id": 0}},
            {
                "$sort": {
                    "code": 1,
                    "date": -1,
                }
            },
            {"$group": {"_id": "$code", "latest_date": {"$max": "$date"}}},
            {"$project": {"code": "$_id", "quote_date": "$latest_date", "_id": 0}},
        ]
        results = StockDailyQuote.objects.aggregate(aggregate_pipeline)
        df_latest_stock_daily_quote = pd.DataFrame(list(results))

        logger.info(f"{self.module_name} - Fetching data freshness meta dates")
        aggregate_pipeline = [
            {
                "$group": {
                    "_id": {
                        "code": "$code",
                        "meta_name": "$meta_name",
                        "meta_type": "$meta_type",
                    },
                    "freshness_datetime": {"$first": "$freshness_datetime"},
                }
            },
            {
                "$project": {
                    "code": "$_id.code",
                    "meta_name": "$_id.meta_name",
                    "meta_type": "$_id.meta_type",
                    "freshness_datetime": 1,
                    "_id": 0,
                }
            },
        ]
        results = DataFreshnessMeta.objects.aggregate(aggregate_pipeline)
        df_freshness_meta_dates = pd.DataFrame(list(results))
        df_freshness_meta_dates["meta_type-name"] = (
            "meta-"
            + df_freshness_meta_dates["meta_type"]
            + "-"
            + df_freshness_meta_dates["meta_name"]
        )
        # find all factor names from meta data
        meta_factor_name_list = (
            df_freshness_meta_dates[df_freshness_meta_dates["meta_type"] == "factor"][
                "meta_name"
            ]
            .unique()
            .tolist()
        )
        df_pivot_freshness_meta_dates = df_freshness_meta_dates.pivot(
            index="code", columns="meta_type-name", values="freshness_datetime"
        )

        logger.info(f"{self.module_name} - Fetching factor data entry dates")
        aggregate_pipeline = [
            {"$sort": {"stock_code": 1, "name": 1, "date": -1}},
            {
                "$group": {
                    "_id": {"stock_code": "$stock_code", "name": "$name"},
                    "latest_date": {"$max": "$date"},
                }
            },
            {
                "$project": {
                    "stock_code": "$_id.stock_code",
                    "name": "$_id.name",
                    "date": "$latest_date",
                    "_id": 0,
                }
            },
        ]
        results = FactorDataEntry.objects.aggregate(aggregate_pipeline)
        df = pd.DataFrame(list(results))
        df.rename(columns={"stock_code": "code"}, inplace=True)
        df["factor_name"] = "factor_date-" + df["name"]
        factor_names_list = df["name"].unique().tolist()
        consolidated_factor_name_list = list(
            set(meta_factor_name_list) | set(factor_names_list)
        )
        df_factor_data_dates = df.pivot(
            index="code", columns="factor_name", values="date"
        )

        merged_df = pd.merge(
            df_stock_list, df_latest_stock_daily_quote, on="code", how="left"
        )
        merged_df = pd.merge(
            merged_df, df_pivot_freshness_meta_dates, on="code", how="left"
        )
        merged_df = pd.merge(merged_df, df_factor_data_dates, on="code", how="left")

        meta_disagree_indication_col_list = []
        # compare the entry date and meta date for daily quote
        merged_df["quote-daily_quote-disagree"] = (
            merged_df["quote_date"] != merged_df["meta-quote-daily_quote"]
        )

        # compare the entry date and meta date for each factor

        meta_disagree_indication_col_list.append("quote-daily_quote-disagree")
        for name in consolidated_factor_name_list:
            meta_prefix = "meta-factor-"
            entry_prefix = "factor_date-"
            col_name = f"factor-{name}-disagree"
            meta_disagree_indication_col_list.append(col_name)
            meta_col_name = meta_prefix + name
            entry_col_name = entry_prefix + name
            if (
                meta_col_name in merged_df.columns
                and entry_col_name in merged_df.columns
            ):
                merged_df[col_name] = (
                    merged_df[meta_col_name] != merged_df[entry_col_name]
                )
            elif entry_col_name not in merged_df.columns:
                merged_df[col_name] = True
                merged_df[entry_col_name] = pd.NaT
            elif meta_col_name not in merged_df.columns:
                merged_df[col_name] = True
                merged_df[meta_col_name] = pd.NaT

        # filter the stocks that have meta disagree
        pass

        # for stock_code in factor_meta_disagree_df.iterrows():
        #     for col_name in meta_disagree_indication_col_list:
        #
        # pass

    def keep_freshness_meta_distinct(self):
        """
        检查因子数据的meta数据是否唯一
        :return:
        """
        logger.info(
            f"{self.module_name} - Running processor keep_freshness_meta_distinct"
        )
        pipeline = [
            {
                "$group": {
                    "_id": {
                        "meta_name": "$meta_name",
                        "meta_type": "meta_type$",
                        "code": "$code",
                        "object_type": "$object_type",
                    },
                    "doc_ids": {"$push": "$_id"},  # 收集每个组的文档 ID
                    "count": {"$sum": 1},  # 统计每个组的文档数量
                }
            },
            {
                "$match": {
                    "count": {"$gt": 1}  # 只保留重复的组
                }
            },
        ]
        duplicates = list(DataFreshnessMeta.objects.aggregate(pipeline))
        if len(duplicates) > 0:
            logger.info(
                f"{self.module_name} - Found {len(duplicates)} duplicate factor entry"
            )
            deletion_count = 0
            # commence deletion
            for dup in duplicates:
                # 保留第一个文档，删除其他文档
                doc_ids_to_delete = dup["doc_ids"][1:]  # 跳过第一个文档
                deletion_count += len(doc_ids_to_delete)
                DataFreshnessMeta.objects.filter(id__in=doc_ids_to_delete).delete()
                logger.info(
                    f"{self.module_name} - Deleted {len(doc_ids_to_delete)} duplicate DataFreshnessMeta entry for "
                    f"{dup['_id']['code']}-{dup['_id']['meta_name']}"
                )
            logger.info(
                f"{self.module_name} - Deleted {deletion_count} duplicate DataFreshnessMeta entry"
            )
        else:
            logger.info(f"{self.module_name} - DataFreshnessMeta duplicate check OK")

    def keep_factor_distinct(self):
        """
        检查因子数据是否唯一
        :return:
        """
        logger.info(f"{self.module_name} - Running processor keep_factor_distinct")
        pipeline = [
            {
                "$group": {
                    "_id": {
                        "name": "$name",
                        "stock_code": "$stock_code",
                        "date": "$date",
                    },
                    "doc_ids": {"$push": "$_id"},  # 收集每个组的文档 ID
                    "count": {"$sum": 1},  # 统计每个组的文档数量
                }
            },
            {
                "$match": {
                    "count": {"$gt": 1}  # 只保留重复的组
                }
            },
        ]

        duplicates = list(FactorDataEntry.objects.aggregate(pipeline))
        if len(duplicates) > 0:
            logger.info(
                f"{self.module_name} - Found {len(duplicates)} duplicate factor entry"
            )
            deletion_count = 0
            # commence deletion
            for dup in duplicates:
                # 保留第一个文档，删除其他文档
                doc_ids_to_delete = dup["doc_ids"][1:]  # 跳过第一个文档
                deletion_count += len(doc_ids_to_delete)
                FactorDataEntry.objects.filter(id__in=doc_ids_to_delete).delete()
                logger.debug(
                    f"{self.module_name} - Deleted {len(doc_ids_to_delete)} duplicate factor entry for "
                    f"{dup.stock_code}-{dup.stock_name}-{dup.name}-{dup.date}"
                )
            logger.info(
                f"{self.module_name} - Deleted {deletion_count} duplicate factor entry"
            )
        else:
            logger.info(f"{self.module_name} - factor entry duplicate check OK")
        # print(duplicates)

    def keep_factor_distinct_by_date(self):
        """
        检查因子数据是否唯一
        :return:
        """
        logger.info(
            f"{self.module_name} - Running processor keep_factor_distinct_by_date"
        )
        end_date = datetime.datetime(1990, 12, 19)  # 结束日期
        delta = datetime.timedelta(days=1)  # 每次处理一天的数据

        # current_date = start_date
        current_date = datetime.datetime(2025, 2, 8)  # 结束日期
        while current_date > end_date:
            next_date = current_date - delta
            logger.info(
                f"{self.module_name} - Checking data from {current_date} to {next_date}"
            )
            # docs = FactorDataEntry.objects(date__gte=next_date, date__lt=current_date).hint([('date', -1)]).limit(1)
            docs = FactorDataEntry.objects(date=current_date).limit(1)
            print(docs.explain())
            # 使用字典记录已经处理过的 (name, stock_code, date) 组合
            seen = {}
            deletion_count = 0
            for doc in docs:
                key = (doc.name, doc.stock_code, doc.date)
                if key in seen:
                    # 如果已经存在，删除当前文档
                    doc.delete()
                    deletion_count += 1
                    logger.debug(
                        f"{self.module_name} - Deleted duplicate factor entry for "
                        f"{doc.stock_code}-{doc.stock_name}-{doc.name}-{doc.date}"
                    )
                else:
                    # 如果不存在，记录当前文档
                    seen[key] = doc.id

            current_date = next_date

    def remove_factor_data(self):

        FactorDataEntry.objects().delete()
        DataFreshnessMeta.objects(meta_type="factor").delete()

    def reset_stock_inactive_state(self):
        BasicStock.objects().update(active_status=0)
        logger.warning("Setting all stock active_status to 0")

    def remove_freshness_meta_by_dt(self):
        q = DataFreshnessMeta.objects(
            calculated_at__gt=datetime.datetime(2025, 2, 18, 0, 50)
        )
        q.delete()
        logger.info(f"deleted {len(q)} documents")
        pass

    def remove_meta_no_upd_flag(self):
        logger.warning("resetting NO_UPD flag in all meta data.")
        q = DataFreshnessMeta.objects(status="NO_UPD").update(unset__status=1)
        logger.info(f"reset {q} NO_UPD flags.")

    # def calibrate_data_freshness(self):
    #     prog_bar = progress_bar()
    #     stock_list = list(BasicStock.objects())
    #     stock_list_len = len(stock_list)
    #     result_dict = {
    #         "quote_meta_disagree": False,
    #         "factor_meta_disagree": False,
    #         "quote_factor_disagree": False,
    #     }
    #     logger.info(f"Executing data integrity check for {stock_list_len} stocks")
    #     # 获取所有 BasicStock 记录
    #     for i, basic_stock in enumerate(stock_list):
    #         prog_bar_msg = f"Checking quote data integrity of {basic_stock.code}-{basic_stock.name}"
    #
    #         prog_bar(i, stock_list_len, prog_bar_msg)
    #         code = basic_stock.code
    #
    #         # 查询 StockDailyQuote 中 code 最晚的 date 记录，作为基准日期
    #         latest_stock_daily_quote = StockDailyQuote.objects(code=code).order_by('-date').first()
    #         daily_quote_freshness_dt = read_freshness_meta(code=basic_stock.code,
    #                                                        object_type=basic_stock.object_type,
    #                                                        meta_type='quote',
    #                                                        meta_name='daily_quote')
    #
    #         # if not equal, fix the meta data
    #         if latest_stock_daily_quote != daily_quote_freshness_dt:
    #             logger.warning(f"{basic_stock-code}-{basic_stock.name}Quote data and meta data disagree, "
    #                            f"quote data: {latest_stock_daily_quote}, meta data {daily_quote_freshness_dt}")
    #             upsert_freshness_meta(code=basic_stock.code,
    #                                   object_type=basic_stock.object_type,
    #                                   meta_type='quote',
    #                                   meta_name='daily_quote',
    #                                   dt=latest_stock_daily_quote)
    #             logger.info(f"reset {basic_stock - code}-{basic_stock.name} meta data to {latest_stock_daily_quote}")
    #
    #         # Analysis the integrity of the factor data
    #         # 查询 FactorDataEntry 中 stock_code 相同的所有记录
    #         prog_bar_msg = f"Checking factor data integrity of {basic_stock.code}-{basic_stock.name}"
    #         factor_data_entries = FactorDataEntry.objects(stock_code=code)
    #         # 使用 defaultdict 来存储 name 和对应的 date 值
    #         factor_name_date_dict_a = defaultdict(list)
    #         for entry in factor_data_entries:
    #             factor_name_date_dict_a[entry.name].append(entry.date)
    #         keys_factor_a = set(factor_name_date_dict_a.keys())
    #
    #         # 查询DataFreshnessMeta中Factor记录
    #         factor_meta = DataFreshnessMeta.objects(code=code, meta_type="factor")
    #         factor_name_date_dict_b = defaultdict(list)
    #         for entry in factor_meta:
    #             factor_name_date_dict_b[entry.name].append(entry.date)
    #         keys_factor_b = set(factor_name_date_dict_b.keys())
    #
    #         # Check the factor names from the both side
    #         if keys_factor_a != keys_factor_b:
    #
    #
    #         # 获取最新的 date
    #         if latest_stock_daily_quote and latest_factor_data_entry:
    #             latest_date = max(latest_stock_daily_quote.date, latest_factor_data_entry.date)
    #         elif latest_stock_daily_quote:
    #             latest_date = latest_stock_daily_quote.date
    #         elif latest_factor_data_entry:
    #             latest_date = latest_factor_data_entry.date
    #         else:
    #             continue
    #
    #         # 查询 DataFreshnessMeta 中对应的记录
    #         meta_obj = DataFreshnessMeta.objects(code=code, meta_name=basic_stock.name).first()
    #
    #         if meta_obj:
    #             # 如果存在记录且 freshness_datetime 不一致，则更新
    #             if meta_obj.freshness_datetime != latest_date:
    #                 meta_obj.freshness_datetime = latest_date
    #                 meta_obj.calculated_at = datetime.datetime.now()
    #                 meta_obj.save()
    #                 print(f"Updated DataFreshnessMeta for code={code}, meta_name={basic_stock.name}")
    #         else:
    #             # 如果不存在记录，则创建新记录
    #             new_meta_obj = DataFreshnessMeta(
    #                 code=code,
    #                 meta_name=basic_stock.name,
    #                 freshness_datetime=latest_date,
    #                 calculated_at=datetime.datetime.now(),
    #                 object_type="basic_stock",
    #                 meta_type="daily_data",
    #                 status="OPEN"
    #             )
    #             new_meta_obj.save()
    #             print(f"Created DataFreshnessMeta for code={code}, meta_name={basic_stock.name}")


data_integrity_keeper = DataIntegrityKeeper()


if __name__ == "__main__":
    # Establish DB Connection
    mongo_watcher.initialize()
    mongo_watcher.get_db_connection()
    obj = DataIntegrityKeeper()
    obj.keep_factor_distinct()
