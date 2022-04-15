# -*- coding: utf-8 -*-
# Author : univic
# Date: 2022-03-11

import datetime
from mongoengine import StringField, EmbeddedDocumentListField, DateTimeField, ReferenceField, ListField, \
    EmbeddedDocument, BooleanField, FloatField, IntField
from app.lib.database import db
from app.model.stock import Stock


class KwArgs(EmbeddedDocument):
    keyword = StringField()
    arg = StringField()


class DataRetriveTask(db.Document):
    """
    status: 0-created, 5-pending, 7-failed, 9-completed
    """
    callback_module = StringField(required=True)
    callback_handler = StringField(required=True)
    args = ListField(StringField())
    kwargs = EmbeddedDocumentListField('KwArgs')
    created_at = DateTimeField(default=datetime.datetime.now())
    priority = IntField(default=0)
    status = IntField(default=0)
    message = StringField()


