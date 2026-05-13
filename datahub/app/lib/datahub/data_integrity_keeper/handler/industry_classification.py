# -*- coding: utf-8 -*-
"""Industry classification sync pipeline.

Fetches Shenwan (申万) industry data from baostock and upserts into
StockIndustryClassification. Runs monthly via cronjob to keep classifications
up to date.
"""

import datetime
import logging

from app.model.industry import StockIndustryClassification

logger = logging.getLogger(__name__)


def sync_industry_classification(
    dry_run: bool = False,
    force_update: bool = False,
) -> dict:
    """Fetch Shenwan industry data from baostock and upsert.

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
            if len(row) < 3 or not row[0]:
                continue

            code = row[0].strip()
            name = row[1].strip() if len(row) > 1 else ""
            industry_name = row[2].strip() if len(row) > 2 else ""

            # Baostock returns industry as "申万一级行业-申万二级行业" format
            # Parse into L1 and L2
            l1_name, l2_name = _parse_shenwan_industry(industry_name)
            l1_code = _derive_industry_code(l1_name or "")

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
                existing.industry_code_sw_l2 = _derive_industry_code(l2_name or "")
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
                    industry_code_sw_l2=_derive_industry_code(l2_name or ""),
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


def _parse_shenwan_industry(raw: str) -> tuple[str | None, str | None]:
    """Parse baostock's industry string into L1 and L2 names.

    Baostock format: '银行业' (L1 only) or '银行-银行Ⅱ' (L1-L2)
    """
    if not raw or raw.strip() == "":
        return None, None

    if "-" in raw:
        parts = raw.split("-", 1)
        return parts[0].strip(), parts[1].strip() if len(parts) > 1 else None

    return raw.strip(), None


# A minimal mapping of common Shenwan L1 industry names to official codes.
# Full numeric codes would require referencing the official Shenwan industry
# code table. Populate entries here as codes are verified from baostock docs.
_INDUSTRY_CODE_MAP: dict[str, str] = {}


def _derive_industry_code(industry_name: str) -> str:
    """Return a deterministic code for the given industry name.

    Checks the _INDUSTRY_CODE_MAP for an official mapping first, otherwise
    falls back to CRC32 hash. Codes are deterministic and stable.
    """
    if not industry_name:
        return "UNKNOWN"
    if industry_name in _INDUSTRY_CODE_MAP:
        return _INDUSTRY_CODE_MAP[industry_name]

    # CRC32 produces a deterministic 0-2^32 range value from the name bytes
    import zlib

    code_val = zlib.crc32(industry_name.encode("utf-8")) & 0xFFFFFFFF
    code = f"SW{(code_val % 100000):05d}"
    return code


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
