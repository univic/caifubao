# -*- coding: utf-8 -*-
# Author : univic
# Date: 2021-02-03

import os
from app.conf import BaseConfig


class TestConfig(BaseConfig):
    """Test configuration - uses in-memory SQLite-like defaults"""

    TESTING = True
    DEBUG = False

    # Flask-Security password configuration
    SECURITY_PASSWORD_SALT = os.getenv("SECURITY_PASSWORD_SALT", "test-salt")
    SECURITY_PASSWORD_HASH = os.getenv("SECURITY_PASSWORD_HASH", "sha256_crypt")

    # Override properties to allow test mode without real credentials
    @property
    def MONGODB_DB(self):
        return os.getenv("MONGODB_NAME", "test_db")

    @property
    def MONGODB_HOST(self):
        return os.getenv("MONGODB_HOST", "localhost")

    @property
    def MONGODB_PORT(self):
        return int(os.getenv("MONGODB_PORT", "27017"))

    @property
    def MONGODB_USERNAME(self):
        return os.getenv("MONGODB_USER", "")

    @property
    def MONGODB_PASSWORD(self):
        return os.getenv("MONGODB_PASS", "")
