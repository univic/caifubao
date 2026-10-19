"""Bounded degradation policy for the daily quote / index refresh.

A refresh that validates almost the whole universe must keep the downstream
chain usable (factors, signals, scoring) instead of failing the run outright,
while a real outage (hundreds of failures) must still fail. This module owns
the policy so the processor, the runners and the signal gate share one
definition of "usable".

Quotes
------
A stock quote run persists ``DEGRADED`` (never ``SUCCESS``) when at least one
row validated, ``failed_count <= max(5, ceil(0.002 * attempted_symbols))``,
``coverage_rate = validated_symbols / attempted_symbols >= 0.99`` and the
consecutive-history circuit breaker did not trip. Anything above those caps,
zero validated rows, or the breaker still fails the run.

Indices
-------
The declared core indices must always refresh; a core failure fails the run.
A non-core index with no history at all (a structural absence) is *skipped*
instead of failed, and does not trip the circuit breaker. Skips that are not
in the persistent-absence allowlist mark the run ``DEGRADED``; skips that are
allowlisted are expected and leave the run clean, so a permanently absent
non-core index cannot hold the pipeline at ``DEGRADED`` forever.

Operator switches
-----------------
``DATAHUB_QUOTE_STRICT_VALIDATION=1`` disables degradation: any validation
failure fails the run (the historical strict default).

``DATAHUB_INDEX_SKIP_ALLOWLIST`` is a comma-separated list of non-core index
codes whose history is known to be permanently absent in this environment. It
extends :data:`DEFAULT_INDEX_SKIP_ALLOWLIST`.
"""

from __future__ import annotations

import math
import os
from typing import Any, Iterable

QUOTE_STRICT_VALIDATION_ENV = "DATAHUB_QUOTE_STRICT_VALIDATION"
INDEX_SKIP_ALLOWLIST_ENV = "DATAHUB_INDEX_SKIP_ALLOWLIST"

# At most this many failures are tolerated regardless of universe size.
QUOTE_FAILURE_ABSOLUTE_CAP = 5
# At most this fraction of the attempted symbols may fail.
QUOTE_FAILURE_RELATIVE_RATE = 0.002
# validated/attempted must stay at or above this rate to degrade instead.
QUOTE_COVERAGE_FLOOR = 0.99
# Persisted coverage precision (keeps the stored percentage round-trippable).
COVERAGE_RATE_PRECISION = 6

# Indices that must refresh for the benchmark / market-regime paths: a failure
# on any of them fails the index run.
CORE_INDEX_CODES = frozenset(
    {
        "sh000001",
        "sz399001",
        "sz399006",
        "sh000300",
        "sh000905",
    }
)

# Non-core indices with a *declared* persistent structural absence. Empty by
# default: add codes here only after confirming the source will never return
# their history, or set DATAHUB_INDEX_SKIP_ALLOWLIST per environment.
DEFAULT_INDEX_SKIP_ALLOWLIST: frozenset[str] = frozenset()

_TRUTHY_VALUES = frozenset({"1", "true", "yes", "on"})


def quote_strict_validation() -> bool:
    """True when any validation failure must fail the run (strict default)."""
    value = os.getenv(QUOTE_STRICT_VALIDATION_ENV, "").strip().lower()
    return value in _TRUTHY_VALUES


def core_index_codes() -> frozenset[str]:
    return CORE_INDEX_CODES


def index_skip_allowlist() -> frozenset[str]:
    """Declared plus environment-configured non-core index absence allowlist."""
    configured = {
        code.strip().lower()
        for code in os.getenv(INDEX_SKIP_ALLOWLIST_ENV, "").split(",")
        if code.strip()
    }
    return frozenset(DEFAULT_INDEX_SKIP_ALLOWLIST | configured)


def quote_failure_cap(attempted_symbols: int) -> int:
    """Tolerated failed-symbol count: ``max(5, ceil(0.002 * attempted))``."""
    attempted = max(int(attempted_symbols or 0), 0)
    return max(
        QUOTE_FAILURE_ABSOLUTE_CAP,
        math.ceil(QUOTE_FAILURE_RELATIVE_RATE * attempted),
    )


def coverage_rate(validated_symbols: int, attempted_symbols: int) -> float:
    attempted = max(int(attempted_symbols or 0), 0)
    if attempted <= 0:
        return 0.0
    validated = min(max(int(validated_symbols or 0), 0), attempted)
    return round(validated / attempted, COVERAGE_RATE_PRECISION)


def _unique(values: Iterable[Any]) -> list[str]:
    return sorted({str(value) for value in values if value})


def evaluate_validation(
    *,
    obj_type: str,
    attempted_symbols: int,
    validated_symbols: int,
    failed_codes: Iterable[Any] = (),
    attempted_codes: Iterable[Any] = (),
    skipped_codes: Iterable[Any] = (),
    enforce: bool = True,
    strict: bool | None = None,
    allowlist: Iterable[str] | None = None,
) -> dict[str, Any]:
    """Classify one quote/index phase into clean, degraded or failed.

    ``enforce`` is the phase's ``allow_update`` flag: a dry validation run is
    classified but never failed or degraded. The returned dict is what the
    runners persist under ``quote_validation`` / ``index_validation``.
    """
    is_index = obj_type == "index"
    attempted = max(int(attempted_symbols or 0), 0)
    validated = min(max(int(validated_symbols or 0), 0), attempted)
    skipped = _unique(skipped_codes)
    skipped_set = set(skipped)
    failed = [code for code in _unique(failed_codes) if code not in skipped_set]
    attempted_set = {str(code) for code in attempted_codes if code}
    strict_enabled = quote_strict_validation() if strict is None else bool(strict)
    allowed_skips = (
        index_skip_allowlist() if allowlist is None else frozenset(allowlist)
    )

    payload: dict[str, Any] = {
        "attempted_symbols": attempted,
        "validated_symbols": validated,
        "failed_count": len(failed),
        "failed_codes": failed,
        # Alias kept for the OpenSpec scenario wording; the fields above are
        # the canonical ones consumed by the gates.
        "failed_symbols": list(failed),
        "coverage_rate": coverage_rate(validated, attempted),
        "strict": strict_enabled,
        "degraded": False,
        "failure_reason": None,
    }
    if is_index:
        payload.update(
            {
                "skipped_indices": skipped,
                "skipped_count": len(skipped),
                "failed_indices": failed,
                "allowlisted_skips": [
                    code for code in skipped if code in allowed_skips
                ],
            }
        )
    else:
        payload.update(
            {
                "skipped_symbols": skipped,
                "skipped_count": len(skipped),
            }
        )

    if not enforce:
        return payload

    if is_index:
        if failed:
            # Core and non-core hard failures both keep the strict path.
            payload["failure_reason"] = "index_failure"
        elif any(code not in allowed_skips for code in skipped):
            payload["degraded"] = True
        return payload

    if not failed:
        return payload

    unattributed = [code for code in failed if code not in attempted_set]
    if unattributed:
        # A symbol that failed without being an attempted refresh cannot be
        # covered by the tolerance: keep the historical strict failure.
        payload["failure_reason"] = "unattributed_failure"
    elif strict_enabled:
        payload["failure_reason"] = "strict_validation"
    elif validated <= 0:
        payload["failure_reason"] = "zero_validated"
    elif len(failed) > quote_failure_cap(attempted):
        payload["failure_reason"] = "failure_cap_exceeded"
    elif payload["coverage_rate"] < QUOTE_COVERAGE_FLOOR:
        payload["failure_reason"] = "coverage_below_floor"
    else:
        payload["degraded"] = True
    return payload
