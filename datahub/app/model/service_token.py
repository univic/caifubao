# -*- coding: utf-8 -*-
# Author : Gemini CLI
# Date: 2026-04-16

import datetime

from mongoengine import DateTimeField, ListField, StringField

import app.lib.utilities.db_util as db_util
from app.lib.db_watcher.mongoengine_tool import db


class ServiceToken(db.Document):
    """
    Service Token for service-to-service authentication (e.g., OpenClaw)
    status:
    active - valid
    revoked - manually disabled
    expired - past expires_at
    """

    name = StringField(required=True, unique=True)
    token_hash = StringField(required=True)  # Hashed version of the token
    scopes = ListField(StringField(), default=["openclaw:data-read"])
    status = StringField(default="active", choices=["active", "revoked", "expired"])

    expires_at = DateTimeField()
    created_at = DateTimeField(default=lambda: datetime.datetime.now(datetime.UTC))

    # Tracking
    last_used_at = DateTimeField()
    last_used_ip = StringField()

    meta = {"collection": "service_tokens", "indexes": ["name", "status", "token_hash"]}

    def to_json(self):
        converted_dict = db_util.dbo_better_json(self)
        # Remove sensitive hash
        if "token_hash" in converted_dict:
            converted_dict.pop("token_hash")
        return converted_dict

    def to_summary_json(self):
        """Minimal info for logs"""
        return {"id": str(self.id), "name": self.name, "scopes": self.scopes}

    def is_valid(self, required_scope=None):
        if self.status != "active":
            return False, f"Token status is {self.status}"

        if self.expires_at and self.expires_at < datetime.datetime.now(datetime.UTC):
            return False, "Token has expired"

        if required_scope and required_scope not in self.scopes:
            return False, f"Token missing required scope: {required_scope}"

        return True, None
