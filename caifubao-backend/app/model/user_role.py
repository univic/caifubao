# -*- coding: utf-8 -*-
# Author : univic
# Date: 2021-02-03

from app.lib.database import db


class UserRole(db.Document):
    name = db.StringField(max_length=80, unique=True)
    description = db.StringField(max_length=255)
