# -*- coding: utf-8 -*-
"""Industry classification sync pipeline.

Fetches CSRC (证监会) industry data from baostock's ``query_stock_industry`` and
upserts into StockIndustryClassification. Baostock's industry API returns the
CSRC classification (e.g. ``J66货币金融服务``); the legacy ``*_sw_*`` field names
on the model are kept for compatibility even though the data is CSRC, not Shenwan
(申万). Runs monthly via cronjob to keep classifications up to date.
"""

import datetime
import json
import logging
import re

from app.model.industry import StockIndustryClassification

logger = logging.getLogger(__name__)

# Baostock returns industry codes as ``sh.600036``; every other collection and
# every scoring lookup uses ``sh600036``. Persisting the baostock form made all
# canonical lookups miss, so ingestion normalizes to the canonical form and a
# migration rewrites the legacy rows (industry-classification-code-normalization).
CANONICAL_CODE_PATTERN = re.compile(r"^(sh|sz|bj)\d{6}$")
LEGACY_CODE_PATTERN = re.compile(r"^([a-z]{2})\.(\d{6})$")
#: How many unrecognized keys a migration summary reports verbatim.
UNRECOGNIZED_SAMPLE_LIMIT = 20


def canonical_stock_code(code: str) -> str:
    """Return the canonical stock code shared with quotes and predictions."""
    return str(code or "").strip().replace(".", "")


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

            code = canonical_stock_code(row[1])
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


def normalize_stock_codes(dry_run: bool = False) -> dict:
    """Rewrite legacy separated ``stock_industry`` keys to the canonical form.

    Only keys matching the baostock shape ``xx.NNNNNN`` are rewritten; a key
    that is neither canonical nor that legacy shape is reported in
    ``unrecognized`` and left untouched rather than guessed at.

    Idempotent, and ``dry_run`` performs no write. The rewrite uses
    collection-level updates rather than ``save()``, so ``last_synced_at`` is
    not refreshed and ``assigned_at`` / ``industry_change_log`` keep their
    point-in-time meaning. A merge deliberately appends no change-log entry:
    the key changed, not the classification.
    """
    counts = {"scanned": 0, "renamed": 0, "merged": 0, "skipped": 0}
    unrecognized: list[str] = []

    for doc in StockIndustryClassification.objects():
        counts["scanned"] += 1
        code = str(getattr(doc, "stock_code", "") or "")
        legacy_match = LEGACY_CODE_PATTERN.match(code)
        if not legacy_match:
            if CANONICAL_CODE_PATTERN.match(code):
                counts["skipped"] += 1
            else:
                unrecognized.append(code)
            continue

        canonical = canonical_stock_code(code)
        existing = StockIndustryClassification.objects(stock_code=canonical).first()
        if existing is None:
            if not dry_run:
                StockIndustryClassification.objects(stock_code=code).update_one(
                    set__stock_code=canonical
                )
            counts["renamed"] += 1
        else:
            if not dry_run:
                _merge_legacy_into_canonical(existing, doc)
            counts["merged"] += 1

    summary = {
        "status": "GOOD",
        "scanned": counts["scanned"],
        "renamed": counts["renamed"],
        "merged": counts["merged"],
        "skipped": counts["skipped"],
        "unrecognized_count": len(unrecognized),
        "unrecognized": unrecognized[:UNRECOGNIZED_SAMPLE_LIMIT],
        "dry_run": dry_run,
    }
    logger.info("Industry code normalization completed: %s", summary)
    return summary


def _merge_legacy_into_canonical(canonical_doc, legacy_doc) -> None:
    """Fold a legacy separated-key record into its canonical record.

    The surviving record keeps the canonical classification: attaching an
    earlier legacy anchor to a *later* classification would let a historical
    date inherit a classification that did not exist yet. Two exceptions adopt
    the legacy anchor, because the classification is provably the same one:

    - the canonical record has no classification to anchor, or
    - both records carry the same L1 code and name (for example the fixed sync
      created a canonical row before the migration ran, so its ``assigned_at``
      is the deploy time rather than the classification's true start).

    Without the second rule, a sync that runs between deploy and migration
    would permanently hide the classification from every earlier date.
    """
    updates = {}
    if not canonical_doc.industry_code_sw_l1 and legacy_doc.industry_code_sw_l1:
        updates["set__industry_code_sw_l1"] = legacy_doc.industry_code_sw_l1
        updates["set__industry_name_sw_l1"] = legacy_doc.industry_name_sw_l1
        updates["set__industry_code_sw_l2"] = legacy_doc.industry_code_sw_l2
        updates["set__industry_name_sw_l2"] = legacy_doc.industry_name_sw_l2
        if legacy_doc.assigned_at:
            updates["set__assigned_at"] = legacy_doc.assigned_at
    elif _same_classification(canonical_doc, legacy_doc):
        earlier = _earlier_anchor(canonical_doc.assigned_at, legacy_doc.assigned_at)
        if earlier is not None and earlier != canonical_doc.assigned_at:
            updates["set__assigned_at"] = earlier

    canonical_log = list(canonical_doc.industry_change_log or [])
    merged_log = _merge_change_logs(
        canonical_doc.industry_change_log, legacy_doc.industry_change_log
    )
    if merged_log != canonical_log:
        updates["set__industry_change_log"] = merged_log

    if updates:
        StockIndustryClassification.objects(
            stock_code=canonical_doc.stock_code
        ).update_one(**updates)
    StockIndustryClassification.objects(stock_code=legacy_doc.stock_code).delete()


def _same_classification(left, right) -> bool:
    """Whether both records carry the same CSRC L1 classification."""
    code = getattr(left, "industry_code_sw_l1", None)
    if not code or getattr(right, "industry_code_sw_l1", None) != code:
        return False
    return (getattr(left, "industry_name_sw_l1", None) or "") == (
        getattr(right, "industry_name_sw_l1", None) or ""
    )


def _earlier_anchor(left, right):
    """The earlier of two point-in-time anchors, compared as calendar dates."""
    if left is None:
        return right
    if right is None:
        return left
    left_date = left.date() if isinstance(left, datetime.datetime) else left
    right_date = right.date() if isinstance(right, datetime.datetime) else right
    return left if left_date <= right_date else right


def _merge_change_logs(primary, secondary) -> list:
    """Union two change logs without inventing a new entry, oldest first."""
    merged = []
    seen = set()
    for entry in list(primary or []) + list(secondary or []):
        if not isinstance(entry, dict):
            continue
        key = json.dumps(entry, sort_keys=True, default=str)
        if key in seen:
            continue
        seen.add(key)
        merged.append(entry)
    merged.sort(key=lambda entry: str(entry.get("timestamp") or ""))
    return merged


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
