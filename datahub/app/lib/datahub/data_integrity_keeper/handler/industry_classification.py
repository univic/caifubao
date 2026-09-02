# -*- coding: utf-8 -*-
"""Industry classification sync pipeline.

Fetches CSRC (证监会) industry data from baostock's ``query_stock_industry`` and
upserts into StockIndustryClassification. Baostock's industry API returns the
CSRC classification (e.g. ``J66货币金融服务``); the legacy ``*_sw_*`` field names
on the model are kept for compatibility even though the data is CSRC, not Shenwan
(申万). Runs monthly via cronjob to keep classifications up to date.
"""

import datetime
import logging

from app.model.industry import StockIndustryClassification

logger = logging.getLogger(__name__)


def sync_industry_classification(
    dry_run: bool = False,
    force_update: bool = False,
) -> dict:
    """Fetch CSRC industry data from baostock and upsert.

    For stocks already classified, only updates if the existing record is older
    than 30 days (or force_update is True). For new stocks, always inserts.

    Returns a summary dict with counts.
    """
    import baostock as bs

    conn = bs.login()
    if conn.error_code != "0":
        logger.error("Error connecting Baostock: %s", conn.error_msg)
        return {"status": "FAILED", "error": conn.error_msg}

    try:
        rs = bs.query_stock_industry()
        if rs.error_code != "0":
            logger.error("Failed to query stock industry: %s", rs.error_msg)
            return {"status": "FAILED", "error": rs.error_msg}

        now = datetime.datetime.now(datetime.UTC)
        cutoff = now - datetime.timedelta(days=30)
        counts = {"total": 0, "new": 0, "updated": 0, "skipped": 0, "errors": 0}

        while rs.next():
            row = rs.get_row_data()
            # Baostock row: [updateDate, code, code_name, industry,
            # industryClassification]. The industry column is CSRC format.
            if len(row) < 4 or not row[1]:
                continue

            code = row[1].strip()
            name = row[2].strip() if len(row) > 2 else ""
            industry_raw = row[3].strip() if len(row) > 3 else ""

            # CSRC string is "CODE名称", e.g. "J66货币金融服务".
            l1_code, l1_name = _parse_csrc_industry(industry_raw)
            l2_code = l2_name = None

            if not l1_name:
                counts["skipped"] += 1
                continue

            # Check existing
            existing = StockIndustryClassification.objects(stock_code=code).first()

            if existing and not force_update:
                last_sync = existing.last_synced_at
                if last_sync and last_sync.replace(tzinfo=datetime.UTC) > cutoff:
                    counts["skipped"] += 1
                    continue

                if dry_run:
                    counts["updated"] += 1
                    continue

                # Check if industry actually changed
                changed = (
                    existing.industry_name_sw_l1 != l1_name
                    or existing.industry_name_sw_l2 != l2_name
                )
                if changed:
                    existing.industry_change_log = (
                        existing.industry_change_log or []
                    ) + [
                        {
                            "timestamp": now.isoformat(),
                            "previous_l1": existing.industry_name_sw_l1,
                            "previous_l2": existing.industry_name_sw_l2,
                            "new_l1": l1_name,
                            "new_l2": l2_name,
                        }
                    ]

                existing.industry_name_sw_l1 = l1_name
                existing.industry_code_sw_l1 = l1_code
                existing.industry_name_sw_l2 = l2_name
                existing.industry_code_sw_l2 = l2_code
                existing.last_synced_at = now
                existing.save()
                counts["updated"] += 1
            elif not existing:
                if dry_run:
                    counts["new"] += 1
                    continue

                doc = StockIndustryClassification(
                    stock_code=code,
                    stock_name=name,
                    industry_code_sw_l1=l1_code,
                    industry_name_sw_l1=l1_name,
                    industry_code_sw_l2=l2_code,
                    industry_name_sw_l2=l2_name,
                    assigned_at=now,
                )
                doc.save()
                counts["new"] += 1
            else:
                counts["skipped"] += 1

            counts["total"] += 1

        summary = {
            "status": "GOOD",
            "total_processed": counts["total"],
            "new_classifications": counts["new"],
            "updated_classifications": counts["updated"],
            "skipped": counts["skipped"],
            "errors": counts["errors"],
            "dry_run": dry_run,
        }
        logger.info("Industry sync completed: %s", summary)
        return summary

    except Exception as exc:
        logger.exception("Industry sync failed: %s", exc)
        return {"status": "FAILED", "error": str(exc)}
    finally:
        bs.logout()


def _parse_csrc_industry(raw: str) -> tuple[str | None, str | None]:
    """Parse baostock's CSRC industry string into (code, name).

    CSRC strings look like "J66货币金融服务" (leading code + Chinese name).
    Returns (None, None) when the string is empty or malformed.
    """
    import re

    if not raw or not raw.strip():
        return None, None
    text = raw.strip()
    match = re.match(r"^([A-Za-z]+\d+)(.*)$", text)
    if not match or not match.group(2).strip():
        return None, None
    return match.group(1), match.group(2).strip()


def get_industry_coverage_stats() -> dict:
    """Return coverage statistics for industry data quality page."""
    total = StockIndustryClassification.objects.count()
    by_l1 = StockIndustryClassification.objects.aggregate(
        [
            {"$group": {"_id": "$industry_name_sw_l1", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}},
        ]
    )
    industries = list(by_l1)
    return {
        "total_classified": total,
        "industry_count": len(industries),
        "industries": [
            {"name": item["_id"], "stock_count": item["count"]} for item in industries
        ],
        "last_sync": None,
    }
