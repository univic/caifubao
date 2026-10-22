# -*- coding: utf-8 -*-
"""Contract parity between the backend and datahub model packages.

The two services each define mongoengine models for the same MongoDB
collections (the backend reads what datahub writes). Those definitions drifted
before: the same ``strategy_replays`` document required fields in datahub and
treated them as optional in the backend, and a connection-timeout fix landed on
one side only.

Rather than copying files between packages in CI (which made the tests run
against a *different* model set than the shipped image), this test parses both
source trees and fails when a shared class diverges in the parts that matter at
runtime:

* the MongoDB collection name,
* the set of field names,
* each field's base type,
* each field's ``unique`` flag.

Write-side-only attributes such as ``required`` are intentionally not compared:
the backend may relax them for legacy documents without changing what it reads.
"""

from __future__ import annotations

import ast
import re
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[3]
BACKEND_MODELS = REPO_ROOT / "backend" / "app" / "model"
DATAHUB_MODELS = REPO_ROOT / "datahub" / "app" / "model"

DOCUMENT_BASES = {"Document", "DynamicDocument", "EmbeddedDocument"}
FIELD_SUFFIX = "Field"


class ModelField:
    def __init__(self, name: str, type_name: str, unique: bool):
        self.name = name
        self.type_name = type_name
        self.unique = unique

    def __repr__(self) -> str:  # pragma: no cover - assertion output
        return f"{self.name}:{self.type_name}(unique={self.unique})"


def _is_document_class(node: ast.ClassDef) -> bool:
    for base in node.bases:
        if isinstance(base, ast.Name) and base.id in DOCUMENT_BASES:
            return True
        if isinstance(base, ast.Attribute) and base.attr in DOCUMENT_BASES:
            return True
    return False


def _collection_name(node: ast.ClassDef) -> str | None:
    for statement in node.body:
        if not isinstance(statement, ast.Assign):
            continue
        targets = [t.id for t in statement.targets if isinstance(t, ast.Name)]
        if "meta" not in targets or not isinstance(statement.value, ast.Dict):
            continue
        for key, value in zip(statement.value.keys, statement.value.values):
            if (
                isinstance(key, ast.Constant)
                and key.value == "collection"
                and isinstance(value, ast.Constant)
                and isinstance(value.value, str)
            ):
                return value.value
    return None


def _default_collection(class_name: str) -> str:
    """mongoengine's default collection name (CamelCase -> snake_case)."""
    return re.sub(r"(?<!^)(?=[A-Z])", "_", class_name).lower()


def _field_type(value: ast.AST) -> str | None:
    if not isinstance(value, ast.Call):
        return None
    func = value.func
    name = func.id if isinstance(func, ast.Name) else getattr(func, "attr", None)
    if not name or not name.endswith(FIELD_SUFFIX):
        return None
    return name


def _fields(node: ast.ClassDef) -> dict[str, ModelField]:
    fields: dict[str, ModelField] = {}
    for statement in node.body:
        if not isinstance(statement, ast.Assign):
            continue
        if len(statement.targets) != 1 or not isinstance(
            statement.targets[0], ast.Name
        ):
            continue
        type_name = _field_type(statement.value)
        if type_name is None:
            continue
        unique = False
        for keyword in statement.value.keywords:
            if keyword.arg == "unique" and isinstance(keyword.value, ast.Constant):
                unique = bool(keyword.value.value)
        fields[statement.targets[0].id] = ModelField(
            statement.targets[0].id, type_name, unique
        )
    return fields


def _load_package(directory: Path) -> dict[str, dict]:
    models: dict[str, dict] = {}
    for path in sorted(directory.glob("*.py")):
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in tree.body:
            if not isinstance(node, ast.ClassDef) or not _is_document_class(node):
                continue
            models[node.name] = {
                "collection": _collection_name(node) or _default_collection(node.name),
                "fields": _fields(node),
                "file": path.name,
            }
    return models


def _packages() -> tuple[dict[str, dict], dict[str, dict]]:
    if not BACKEND_MODELS.is_dir() or not DATAHUB_MODELS.is_dir():
        pytest.skip("model packages are not available in this checkout layout")
    return _load_package(BACKEND_MODELS), _load_package(DATAHUB_MODELS)


def test_shared_model_classes_define_the_same_collection_and_fields():
    backend, datahub = _packages()
    shared = sorted(set(backend) & set(datahub))
    assert shared, "expected the backend and datahub to share model classes"

    problems: list[str] = []
    for name in shared:
        backend_model = backend[name]
        datahub_model = datahub[name]
        if backend_model["collection"] != datahub_model["collection"]:
            problems.append(
                f"{name}: collection {backend_model['collection']!r} (backend/"
                f"{backend_model['file']}) != {datahub_model['collection']!r} "
                f"(datahub/{datahub_model['file']})"
            )
            continue

        backend_fields = backend_model["fields"]
        datahub_fields = datahub_model["fields"]
        if set(backend_fields) != set(datahub_fields):
            problems.append(
                f"{name}: field mismatch — only backend: "
                f"{sorted(set(backend_fields) - set(datahub_fields))}, only datahub: "
                f"{sorted(set(datahub_fields) - set(backend_fields))}"
            )
            continue

        for field_name in sorted(backend_fields):
            left = backend_fields[field_name]
            right = datahub_fields[field_name]
            if left.type_name != right.type_name:
                problems.append(
                    f"{name}.{field_name}: type {left.type_name} (backend) != "
                    f"{right.type_name} (datahub)"
                )
            if left.unique != right.unique:
                problems.append(
                    f"{name}.{field_name}: unique={left.unique} (backend) != "
                    f"{right.unique} (datahub)"
                )

    assert not problems, "backend/datahub model drift:\n" + "\n".join(problems)
