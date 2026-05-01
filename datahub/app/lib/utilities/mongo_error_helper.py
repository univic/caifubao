"""Helpers for dealing with MongoDB write exceptions."""

from __future__ import annotations

import re


_CODE_RE = re.compile(r"['\"]code['\"]:\s*(\d+)")


def _extract_bulk_write_codes_from_string(payload: str) -> list[int]:
    if "writeErrors" not in payload:
        return []
    return [int(match) for match in _CODE_RE.findall(payload)]


def _extract_bulk_write_details(error):
    details = getattr(error, "details", None)
    if details:
        return details

    # mongoengine.errors.BulkWriteError stores the payload in args[0]
    args = getattr(error, "args", ()) or ()
    if args and isinstance(args[0], dict):
        return args[0]

    return {}


def is_duplicate_only_bulk_write_error(error) -> bool:
    details = _extract_bulk_write_details(error)
    write_errors = details.get("writeErrors") or []
    if write_errors:
        return all(item.get("code") == 11000 for item in write_errors)

    args = getattr(error, "args", ()) or ()
    if args and isinstance(args[0], str):
        codes = _extract_bulk_write_codes_from_string(args[0])
        return bool(codes) and all(code == 11000 for code in codes)

    return False
