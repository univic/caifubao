"""Tushare on-exchange fund (ETF/LOF) market-data service.

Two responsibilities, both behind explicit operator commands:

``EtfMarketService``
    Maintain the ``exchange_traded_fund`` universe and persist daily OHLCV
    quotes into the shared ``stock_daily_quote`` collection (``object_type``
    stays ``etf`` on the master document, so stock universes are unaffected).

``EtfFQFactorService``
    Reuse the tested stock FQ pipeline, but read the distribution/split factor
    from tushare ``pro.fund_adj`` instead of ``pro.adj_factor``. The derived
    ``fq_factor`` / ``*_hfq`` fields therefore mean the same thing for funds:
    a distribution cannot masquerade as a price reversal.

The service never fabricates a quote for a fund that tushare omits on a trade
date (absence = not traded), and it never writes an FQ batch that failed
validation.
"""

from __future__ import annotations

import datetime
import logging

import pandas
from caifubao_contracts.time import beijing_now_naive, utc_now_naive
from pymongo import UpdateOne

from app.lib.datahub.data_source.handler import zh_etf_daily
from app.lib.datahub.data_source.interface import tushare_interface
from app.lib.factor_factory.fq_factor import FQFactorService
from app.lib.utilities import data_asset_status_helper
from app.model.stock import EtfDailyQuote, ExchangeTradedFund

logger = logging.getLogger(__name__)

ETF_OBJECT_TYPE = "etf"
ETF_QUOTE_JOB = "etf_quote_sync"
ETF_FQ_JOB = "etf_fq_factor_sync"
ETF_HISTORY_JOB = "etf_history_sync"


class EtfFQFactorService(FQFactorService):
    """Stock FQ pipeline with the fund distribution factor as its source."""

    def __init__(
        self,
        quote_model=EtfDailyQuote,
        fund_model=ExchangeTradedFund,
    ):
        super().__init__(quote_model=quote_model, stock_model=fund_model)

    def _load_adj_factor_df(self, code: str, quote_df: pandas.DataFrame):
        ts_code = tushare_interface.to_tushare_ts_code(code)
        start = quote_df.index.min().strftime("%Y%m%d")
        end = quote_df.index.max().strftime("%Y%m%d")
        raw = tushare_interface.fund_adj(ts_code, start, end)
        if raw is None or raw.empty:
            raise RuntimeError(f"fund_adj returned no rows for {code}")
        return raw

    @staticmethod
    def _load_market_adj_factor_snapshot(target_date):
        trade_date = pandas.Timestamp(target_date).strftime("%Y%m%d")
        return tushare_interface.fund_adj_by_trade_date(trade_date)


class EtfMarketService:
    """Universe + quote + FQ maintenance for exchange-traded funds."""

    def __init__(
        self,
        fund_model=ExchangeTradedFund,
        quote_model=EtfDailyQuote,
    ):
        self.fund_model = fund_model
        self.quote_model = quote_model
        self.fq_service = EtfFQFactorService(
            quote_model=quote_model, fund_model=fund_model
        )

    # -- universe ------------------------------------------------------------
    def sync_universe(self) -> dict:
        """Upsert the fund master list; delisted funds become inactive.

        Every row is written; ``active_status`` encodes ``status`` so the
        quote/FQ plan never touches a delisted fund.
        """
        rows = zh_etf_daily.normalize_fund_universe(tushare_interface.fund_basic_all())
        operations = [
            UpdateOne(
                {"code": row["code"]},
                {"$set": self._universe_values(row)},
                upsert=True,
            )
            for row in rows
        ]
        if operations:
            self.fund_model._get_collection().bulk_write(operations, ordered=False)
        written = len(operations)
        logger.info("ETF universe synced: fetched=%s written=%s", len(rows), written)
        return {
            "pulled_count": len(rows),
            "written_count": written,
            "active_count": sum(
                1 for row in rows if (row.get("status") or "L").upper() == "L"
            ),
        }

    @staticmethod
    def _universe_values(row: dict) -> dict:
        """Master-document fields for one normalized fund_basic row."""
        status = (row.get("status") or "L").upper()
        return {
            "name": row["name"],
            "fund_type": row.get("fund_type"),
            "benchmark": row.get("benchmark"),
            "management_fee": row.get("management_fee"),
            "list_date": row.get("list_date"),
            "active_status": 0 if status == "L" else 2,
            "object_type": ETF_OBJECT_TYPE,
            "data_capabilities": {"daily_quote": True, "fq_factor": True},
        }

    def _active_funds(self) -> dict[str, object]:
        return {
            fund.code: fund
            for fund in self.fund_model.objects(active_status=0).only(
                "code", "name", "object_type", "data_capabilities", "list_date"
            )
        }

    # -- quotes --------------------------------------------------------------
    @staticmethod
    def _quote_operation(fund, row: dict) -> UpdateOne:
        daily_quote = EtfDailyQuote()
        daily_quote.code = fund.code
        for column, value in row.items():
            setattr(daily_quote, column, value)
        raw_doc = daily_quote.to_mongo().to_dict()
        raw_doc.pop("_id", None)
        return UpdateOne(
            {"code": raw_doc["code"], "date": raw_doc["date"]},
            {"$set": raw_doc},
            upsert=True,
        )

    def _write_quotes(self, funds: dict[str, object], rows: list[dict]) -> int:
        operations = [
            self._quote_operation(funds[row["code"]], row)
            for row in rows
            if row["code"] in funds
        ]
        if operations:
            self.quote_model._get_collection().bulk_write(operations, ordered=False)
        return len(operations)

    def _refresh_quote_statuses(self, codes: list[str], expected_latest_date) -> None:
        collection = self.quote_model._get_collection()
        stats = data_asset_status_helper.aggregate_stats_by_code(
            collection,
            match={"code": {"$in": codes}},
            code_field="code",
        )
        calculated_at = utc_now_naive()
        records = [
            data_asset_status_helper.build_quote_status_record(
                code=code,
                object_type=ETF_OBJECT_TYPE,
                stats_row=stats.get(code),
                expected_latest_date=expected_latest_date,
                last_job_name=ETF_QUOTE_JOB,
                calculated_at=calculated_at,
            )
            for code in codes
        ]
        if records:
            data_asset_status_helper.bulk_upsert_asset_status(records)

    # -- daily sync ----------------------------------------------------------
    def sync_daily(self, target_date) -> dict:
        """Persist one trade date's ETF quotes and then their FQ fields.

        Quotes are written before FQ so a fund with a missing ``fund_adj`` row
        fails the FQ phase without discarding that day's prices; the FQ phase
        itself is all-or-nothing and refreshes no status on failure.
        """
        target = pandas.Timestamp(target_date).normalize()
        trade_date = target.strftime("%Y%m%d")
        self.sync_universe()
        funds = self._active_funds()
        rows = zh_etf_daily.normalize_tushare_fund_snapshot(
            tushare_interface.fund_daily_by_trade_date(trade_date),
            trade_date,
        )
        unknown = sorted({row["code"] for row in rows} - set(funds))
        if unknown:
            raise RuntimeError(
                "fund_daily snapshot contains codes missing from the ETF "
                f"universe: count={len(unknown)} examples={unknown[:10]}"
            )
        written = self._write_quotes(funds, rows)
        codes = [row["code"] for row in rows]
        self._refresh_quote_statuses(codes, target.to_pydatetime())
        fq_result = self.fq_service.update_codes_from_market_snapshots(
            {code: target.to_pydatetime() for code in codes}
        )
        logger.info(
            "ETF daily sync: date=%s quotes=%s fq_written=%s",
            target.date(),
            written,
            fq_result.get("written_count"),
        )
        return {
            "target_date": target.date().isoformat(),
            "quotes_pulled": len(rows),
            "quotes_written": written,
            "fq": fq_result,
        }

    # -- history backfill ----------------------------------------------------
    def backfill_code(self, code: str) -> dict:
        fund = self.fund_model.objects(code=code).first()
        if fund is None:
            raise RuntimeError(f"ETF fund not found: code={code}")
        start = fund.list_date or datetime.datetime(1990, 1, 1)
        start_date = pandas.Timestamp(start).strftime("%Y%m%d")
        end_date = beijing_now_naive().date().strftime("%Y%m%d")
        raw = tushare_interface.tushare_fund_daily(code, start_date, end_date)
        rows = zh_etf_daily.normalize_tushare_fund_history(raw, code)
        if rows is None or rows.empty:
            raise RuntimeError(f"fund_daily returned no history for {code}")
        written = self._write_quotes({code: fund}, rows.to_dict("records"))
        self._refresh_quote_statuses([code], rows["date"].max().to_pydatetime())
        fq_result = self.fq_service.update_code(code)
        logger.info(
            "ETF history backfilled: code=%s quotes=%s fq=%s",
            code,
            written,
            fq_result.get("written_count"),
        )
        return {
            "code": code,
            "status": "GOOD",
            "quotes_written": written,
            "fq_written": int(fq_result.get("written_count", 0) or 0),
        }

    def backfill_all(self) -> dict:
        results = []
        failed = []
        for code in sorted(self._active_funds()):
            try:
                results.append(self.backfill_code(code))
            except Exception:
                failed.append(code)
                logger.exception("ETF history backfill failed: code=%s", code)
        return {
            "pulled_count": len(results) + len(failed),
            "written_count": sum(
                int(result.get("quotes_written", 0)) for result in results
            ),
            "failed_count": len(failed),
            "failed_codes": failed,
        }

    # -- status --------------------------------------------------------------
    def status(self, code: str | None = None) -> dict:
        funds = list(
            self.fund_model.objects(**{"code": code} if code else {}).only(
                "code", "name", "active_status"
            )
        )
        codes = [fund.code for fund in funds]
        quote_stats = data_asset_status_helper.aggregate_stats_by_code(
            self.quote_model._get_collection(),
            match={"code": {"$in": codes}},
            code_field="code",
        )
        fq_stats = data_asset_status_helper.aggregate_stats_by_code(
            self.quote_model._get_collection(),
            match={
                "code": {"$in": codes},
                "fq_factor": {"$exists": True, "$ne": None},
            },
            code_field="code",
        )
        return {
            "fund_count": len(funds),
            "active_count": sum(1 for fund in funds if fund.active_status == 0),
            "with_quotes": len(quote_stats),
            "with_fq": len(fq_stats),
            "latest_quote_date": (
                max(
                    (row.get("latest_data_date") for row in quote_stats.values()),
                    default=None,
                )
            ),
            "missing_fq_codes": sorted(set(quote_stats) - set(fq_stats))[:20],
        }
