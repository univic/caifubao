# -*- coding: utf-8 -*-
# Author : univic

import datetime
import os
from app.conf import BaseConfig


class DevConfig(BaseConfig):
    JWT_ACCESS_TOKEN_EXPIRES = datetime.timedelta(minutes=30)
    JWT_REFRESH_TOKEN_EXPIRES = datetime.timedelta(days=7)
    FLASK_DEBUG = True

    # Flask-Security password configuration
    # SECURITY_PASSWORD_HASH defaults to argon2, which requires SECURITY_PASSWORD_SALT
    # Using sha256_crypt for simpler deployment
    SECURITY_PASSWORD_SALT = os.getenv(
        "SECURITY_PASSWORD_SALT", "change-me-dev-password-salt"
    )
    SECURITY_PASSWORD_HASH = os.getenv("SECURITY_PASSWORD_HASH", "sha256_crypt")
