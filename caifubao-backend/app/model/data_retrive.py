# -*- coding: utf-8 -*-
# Author : univic
# Date: 2022-03-11

import datetime
from mongoengine import StringField, EmbeddedDocumentListField, DateTimeField, ListField, \
    EmbeddedDocument, IntField
from app.lib.database import db


class KwArg(EmbeddedDocument):
    keyword = StringField()
    arg = StringField()


class DataRetriveTask(db.Document):
    """
    status: CRTD-created, PEND-pending, FAIL-failed, COMP-completed
    """
    uid = StringField(required=True, unique=True)
    name = StringField(required=True)
    callback_module = StringField(required=True)
    callback_handler = StringField(required=True)
    args = ListField(StringField())
    kwargs = EmbeddedDocumentListField('KwArg')
    created_at = DateTimeField(default=datetime.datetime.now())
    processed_at = DateTimeField()
    completed_at = DateTimeField()
    priority = IntField(default=5)
    status = StringField(default='CRTD')
    message = StringField()

    meta = {'allow_inheritance': True}


class ScheduledDataUpdateTask(DataRetriveTask):
    scheduled_process_time = DateTimeField(required=True)