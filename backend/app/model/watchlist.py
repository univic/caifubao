# -*- coding: utf-8 -*-
"""Watchlist model — user-defined stock watchlists."""

import datetime
from mongoengine import (
    DateTimeField,
    ListField,
    StringField,
    Document,
)


class Watchlist(Document):
    """A named collection of stock codes that a user tracks."""

    name = StringField(required=True)
    stock_codes = ListField(StringField(), default=list)
    user_id = StringField()

    created_at = DateTimeField(default=lambda: datetime.datetime.now(datetime.UTC))
    updated_at = DateTimeField()

    meta = {
        "collection": "watchlists",
        "indexes": [
            "user_id",
            ("user_id", "name"),
        ],
    }

    def save(self, *args, **kwargs):
        self.updated_at = datetime.datetime.now(datetime.UTC)
        return super(Watchlist, self).save(*args, **kwargs)
