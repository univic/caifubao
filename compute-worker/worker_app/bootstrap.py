# -*- coding: utf-8 -*-
"""Import-path bootstrap for the compute-worker.

The worker runs against a *merged* ``app`` package: backend application code
plus the datahub model/research libraries it calls into. The container image
bakes that tree at ``/app/app``; a repository checkout builds it in a temporary
directory (backend code first, then datahub model/lib, then the backend
db_watcher restored on top).

This module is imported from ``worker_app/__init__.py`` so the path is ready
before any module imports ``app.*``.
"""

import atexit
import logging
import shutil
import sys
import tempfile
from pathlib import Path

logger = logging.getLogger(__name__)

_app_tree_dir: str | None = None


def prepare_app_tree() -> None:
    """Put the merged ``app`` package on ``sys.path`` (idempotent)."""
    global _app_tree_dir

    if _app_tree_dir is not None:
        return

    # Container image layout: the merged package sits next to worker_app
    # (/app/app next to /app/worker_app).
    worker_root = Path(__file__).resolve().parent.parent
    image_tree = worker_root / "app"
    if (image_tree / "__init__.py").is_file():
        if str(worker_root) not in sys.path:
            sys.path.insert(0, str(worker_root))
        _app_tree_dir = str(image_tree)
        return

    repo_root = Path(__file__).resolve().parents[2]
    backend_app = repo_root / "backend" / "app"
    datahub_app = repo_root / "datahub" / "app"
    if not backend_app.is_dir() or not datahub_app.is_dir():
        raise RuntimeError(
            "compute-worker needs a merged app package: expected /app/app in "
            "the container image or backend/app + datahub/app in a checkout"
        )

    target = Path(tempfile.mkdtemp(prefix="caifubao-compute-app-"))
    shutil.copytree(backend_app, target / "app")
    for subdir in ("model", "lib"):
        shutil.copytree(
            datahub_app / subdir, target / "app" / subdir, dirs_exist_ok=True
        )
    # Keep the backend connection layer (connect/server-selection timeouts) on
    # top of the datahub overlay.
    shutil.copytree(
        backend_app / "lib" / "db_watcher",
        target / "app" / "lib" / "db_watcher",
        dirs_exist_ok=True,
    )
    sys.path.insert(0, str(target))
    _app_tree_dir = str(target)
    atexit.register(shutil.rmtree, str(target), True)
    logger.info("Assembled merged app package at %s", target)
