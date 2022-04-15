# -*- coding: utf-8 -*-
# Author : univic
# Date: 2021-01-31

import datetime
from mongoengine import StringField, ListField, ReferenceField, EmailField, DateTimeField, IntField
from app.lib.database import db
import app.utilities.db_util as db_util


class User(db.Document):
    """
    user_status:
    00 - await for activation
    10 - await for supplemental information
    20 - active
    98 - await for password reset
    99 - locked
    user_role:
    USER - Ordinary User
    ADM - Admin
    """
    username = StringField(required=True, unique=True)
    email = EmailField(required=True)
    password_hash = StringField(required=True)
    user_status = ListField(StringField(), default=['00'])
    time_registered = DateTimeField(default=datetime.datetime.now())
    confirmed_at = DateTimeField()
    employee_id = StringField()
    user_role = ListField(StringField(), default=['USER'])

    # Login tracking info
    last_login_at = DateTimeField()
    last_login_ip = StringField()
    current_login_at = DateTimeField()
    current_login_ip = StringField()
    login_count = IntField(default=0)

    def to_json(self):
        raw_data = self
        converted_dict = db_util.dbo_better_json(raw_data)
        output_dict = {
            'id': converted_dict['id'],
            'username': converted_dict['username'],
            'email': converted_dict['email']
        }
        return output_dict
