import sys
from pathlib import Path

# The shared contracts package is installed in the image. In a source checkout
# it lives at <repo>/shared; add it when running from the repository so local
# development and tests work without an editable install.
try:  # pragma: no cover - import side effect only
    import caifubao_contracts  # noqa: F401
except ModuleNotFoundError:  # pragma: no cover
    _shared = Path(__file__).resolve().parents[2] / "shared"
    if _shared.is_dir():
        sys.path.insert(0, str(_shared))

# -*- coding: utf-8 -*-
# Author : univic
# Date: 2022-02-13


import logging
from app.lib.db_watcher.mongoengine_tool import mongo_watcher
from app.lib.web_server import create_web_app

logger = logging.getLogger(__name__)


def create_app():
    logger.info("Stellaris initializing")

    # Establish DB Connection
    mongo_watcher.initialize()
    mongo_watcher.get_db_connection()

    # Start web server
    app = create_web_app()

    return app


def check_workdir():
    pass
