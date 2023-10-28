#!/usr/bin/env python
# -*- coding: utf-8 -*-

import datetime
from mongoengine import Document, StringField, EmbeddedDocumentListField, DateTimeField, ListField, \
    EmbeddedDocument, IntField


class KwArg(EmbeddedDocument):
    keyword = StringField()
    arg = StringField()


class Task(Document):
    """
    status: CRTD-created, PEND-pending, FAIL-failed, COMP-completed, ABORT-aborted
    """
    # meta = {'allow_inheritance': True}
    uid = StringField(required=True)
    name = StringField(required=True)
    desc = StringField()
    callback_package = StringField(required=True)
    callback_module = StringField(required=True)
    callback_object = StringField()
    callback_handler = StringField(required=True)
    callback_interface = StringField()
    priority = IntField(default=5)
    queue = StringField()
    # if not designated, task will be executed immediately
    scheduled_process_time = DateTimeField()
    # after that time the task will no longer be executed and be set as ABORT status
    # if left blank, the task will always be executed no matter how long it has been
    valid_before = DateTimeField()
    repeat_duration = StringField()
    repeat_amount = IntField()
    repeat_ends_at = DateTimeField()
    args = ListField(StringField())
    kwargs = EmbeddedDocumentListField('KwArg')
    status = StringField(default='CRTD')
    created_at = DateTimeField(default=datetime.datetime.now())
    processed_at = DateTimeField()
    completed_at = DateTimeField()
    exec_result = StringField()
    exec_msg = StringField()
