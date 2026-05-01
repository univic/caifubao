# -*- coding: utf-8 -*-
# Author : univic
# Date: 2022-02-13

from app import create_app
from app.utilities.logger_utils import create_logger
from app.conf import app_config

logger = create_logger()


if __name__ == "__main__":
    app = create_app()
    host = app_config.FLASK_HOST
    port = app_config.FLASK_PORT
    debug = app_config.FLASK_DEBUG
    # Disable the Flask reloader in containers so the parent process doesn't
    # exit during startup and break Kubernetes rollout.
    app.run(host=host, port=port, debug=debug, use_reloader=False)
