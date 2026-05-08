# -*- coding: utf-8 -*-
# Author : univic

import os
import datetime
from app.conf import BaseConfig


REQUIRED_SECRET_FIELDS = (
    "SECRET_KEY",
    "JWT_SECRET_KEY",
    "SECURITY_PASSWORD_SALT",
)

FORBIDDEN_PRODUCTION_SECRET_VALUES = {
    "",
    "default-salt-change-in-production",
    "dev-secret-key-change-in-production",
    "dev-salt-change-in-production",
    "change-me-dev-password-salt",
    "test-secret-key",
    "test-salt",
}

FORBIDDEN_PRODUCTION_SECRET_MARKERS = (
    "change-me",
    "default",
)


class ProductionConfig(BaseConfig):
    # 生产环境下web端的url
    WEB_BASE_URL = ""
    JWT_ACCESS_TOKEN_EXPIRES = datetime.timedelta(minutes=30)
    JWT_REFRESH_TOKEN_EXPIRES = datetime.timedelta(days=7)

    def __init__(self):
        self.SECRET_KEY = os.getenv("SECRET_KEY", "")
        self.JWT_SECRET_KEY = os.getenv("JWT_SECRET_KEY", "")
        self.SECURITY_PASSWORD_SALT = os.getenv("SECURITY_PASSWORD_SALT", "")
        self.SECURITY_PASSWORD_HASH = os.getenv("SECURITY_PASSWORD_HASH", "argon2")
        self._validate_required_secrets()

    def _validate_required_secrets(self):
        invalid_fields = []
        for field in REQUIRED_SECRET_FIELDS:
            value = getattr(self, field, None)
            if value in FORBIDDEN_PRODUCTION_SECRET_VALUES or any(
                marker in value.lower()
                for marker in FORBIDDEN_PRODUCTION_SECRET_MARKERS
            ):
                invalid_fields.append(field)

        if invalid_fields:
            names = ", ".join(sorted(invalid_fields))
            raise ValueError(f"Production config requires non-default secrets: {names}")
