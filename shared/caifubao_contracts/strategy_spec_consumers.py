# -*- coding: utf-8 -*-
"""Consumption manifest for the strategy declaration — a pure completeness gate.

Why this exists
---------------
A declaration field that is validated, hashed into the declaration identity and
documented, but read by no consumer, is a **defect**: the spec claims a behaviour
it never produces. ``rebalance.cadence_days`` is the original case — it declares a
five-session cadence while every session re-plans the whole book.

The manifest records, for every allowed field path, either the module that reads
it (``consumed``) or the work item that will make it read (``unconsumed``). The
checker below fails when the two drift apart in either direction, so a field can
no longer be added silently and a status can no longer be claimed without a
consumer that actually mentions it.

This module is pure: it takes the contract's allowed-field table and the manifest
as arguments and reads only the files the manifest names. It never imports
datahub, Mongo or a broker.

Known limitation
----------------
The consumer check is textual: it requires the consuming module to mention the
field name. It cannot prove the module *acts* on the value. Binding each field to
a golden test that pins its effect is a separate work item
(``strategy-program-2026`` P0.3); until then this gate catches the two cheap
failures — a field with no owner and a claim with no code.
"""

from __future__ import annotations

import json
from pathlib import Path

CONSUMED = "consumed"
UNCONSUMED = "unconsumed"
MANIFEST_SCHEMA = "strategy-spec-consumers-v2"
#: Declaration versions a field may be consumed by.
KNOWN_VERSIONS = frozenset({"1", "2"})
#: Statuses pinned by tests; changing one is a deliberate contract change.
KNOWN_STATUSES = frozenset({CONSUMED, UNCONSUMED})


def leaf_paths(allowed: dict) -> list[str]:
    """Expand a validator ``ALLOWED`` table into leaf field paths.

    A name whose entry is a set of members is a block and is expanded
    recursively, so a nested block such as ``selection.exit`` contributes its own
    members rather than being treated as a leaf.
    """

    def walk(prefix: str) -> list[str]:
        block = allowed.get(prefix)
        if block is None:
            return [prefix]
        out: list[str] = []
        for name in sorted(block):
            child = f"{prefix}.{name}" if prefix else name
            out.extend(walk(child))
        return out

    return walk("")


def load_manifest(path: Path) -> dict:
    with Path(path).open(encoding="utf-8") as handle:
        manifest = json.load(handle)
    if manifest.get("schema") != MANIFEST_SCHEMA:
        raise ValueError(f"manifest schema must be {MANIFEST_SCHEMA}")
    if not isinstance(manifest.get("fields"), dict):
        raise ValueError("manifest.fields must be an object")
    return manifest


def check(manifest: dict, allowed: dict, root: Path) -> list[str]:
    """Return a list of problems; an empty list means the manifest is honest."""
    root = Path(root)
    problems: list[str] = []
    allowed_leaves = set(leaf_paths(allowed))
    declared = set(manifest.get("fields") or {})

    for missing in sorted(allowed_leaves - declared):
        problems.append(
            f"{missing}: allowed field has no manifest entry "
            "(add it with a consumer or mark it unconsumed)"
        )
    for extra in sorted(declared - allowed_leaves):
        problems.append(f"{extra}: manifest entry is not an allowed field")

    for path in sorted(allowed_leaves & declared):
        entry = manifest["fields"][path]
        status = entry.get("status")
        if status == UNCONSUMED:
            if not entry.get("tracked_by"):
                problems.append(f"{path}: unconsumed field must name tracked_by")
            continue
        if status != CONSUMED:
            problems.append(
                f"{path}: status must be one of {sorted(KNOWN_STATUSES)}, got {status!r}"
            )
            continue
        consumers = entry.get("consumers") or []
        if not consumers:
            problems.append(
                f"{path}: consumed field must name at least one consumer module"
            )
            continue
        versions = entry.get("consumed_by_versions")
        if not versions:
            problems.append(
                f"{path}: consumed field must name the declaration versions that read it"
            )
            continue
        unknown_versions = set(versions) - KNOWN_VERSIONS
        if unknown_versions:
            problems.append(
                f"{path}: unknown consumed_by_versions {sorted(unknown_versions)}"
            )
            continue
        leaf = path.rsplit(".", 1)[-1]
        for reference in consumers:
            target = (root / reference).resolve()
            if not target.is_file():
                problems.append(f"{path}: consumer file {reference!r} does not exist")
                continue
            if leaf not in target.read_text(encoding="utf-8"):
                problems.append(
                    f"{path}: consumer {reference!r} never mentions {leaf!r}"
                )
    return problems
