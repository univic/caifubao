# -*- coding: utf-8 -*-
"""The backend and datahub model modules must stay thin re-export shims.

The shared schema lives in ``shared/caifubao_contracts/models`` and is installed
into both images. Each service's ``app/model/<name>.py`` only re-exports it, so
there is exactly one definition and the collections cannot drift.

This test fails when a service reintroduces a local class or when a class added
to the shared module is not re-exported by one of the shims.
"""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[3]
SHARED_MODELS = REPO_ROOT / "shared" / "caifubao_contracts" / "models"
BACKEND_MODELS = REPO_ROOT / "backend" / "app" / "model"
DATAHUB_MODELS = REPO_ROOT / "datahub" / "app" / "model"

SHARED_MODULES = (
    "stock",
    "scoring",
    "signal",
    "factor",
    "industry",
    "factor_eval",
    "service_token",
    "strategy_replay",
)


def _public_classes(path: Path) -> set[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    return {node.name for node in tree.body if isinstance(node, ast.ClassDef)}


def _imported_names(path: Path) -> set[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    expected = f"caifubao_contracts.models.{path.stem}"
    names: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.module == expected:
            names.update(alias.name for alias in node.names)
    return names


def test_shared_package_has_the_expected_modules():
    if not SHARED_MODELS.is_dir():
        pytest.skip("shared contracts package is not available in this checkout")
    for module in SHARED_MODULES:
        assert (SHARED_MODELS / f"{module}.py").is_file()


def test_service_shims_only_reexport_shared_classes():
    if not SHARED_MODELS.is_dir():
        pytest.skip("shared contracts package is not available in this checkout")

    problems: list[str] = []
    for module in SHARED_MODULES:
        shared_classes = _public_classes(SHARED_MODELS / f"{module}.py")
        for service, directory in (
            ("backend", BACKEND_MODELS),
            ("datahub", DATAHUB_MODELS),
        ):
            shim = directory / f"{module}.py"
            if not shim.is_file():
                problems.append(f"{service}/{module}.py is missing")
                continue
            local_classes = _public_classes(shim)
            if local_classes:
                problems.append(
                    f"{service}/{module}.py defines classes locally: "
                    f"{sorted(local_classes)}"
                )
            imported = _imported_names(shim)
            missing = shared_classes - imported
            if missing:
                problems.append(
                    f"{service}/{module}.py does not re-export {sorted(missing)}"
                )
            stale = imported - shared_classes
            if stale:
                problems.append(
                    f"{service}/{module}.py re-exports unknown names {sorted(stale)}"
                )

    assert not problems, "model contract drift:\n" + "\n".join(problems)
