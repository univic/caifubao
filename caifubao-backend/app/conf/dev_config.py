# -*- coding: utf-8 -*-
# Author : univic

import datetime
from app.conf import BaseConfig


class DevConfig(BaseConfig):
    JWT_ACCESS_TOKEN_EXPIRES = datetime.timedelta(hours=8)
    # mongodb 配置
    MONGODB_SETTINGS = {
        'db': 'caifubao-dev',
        'host': '127.0.0.1',
        'port': 27017,
    }
    MAIL_CONFIG = {
        "sender_email": "xyzhgwf@hotmail.com",
        # "recipient_email_list": ["xyzhgwf@hotmail.com", "wangqi6981@outlook.com"],
        "recipient_email_list": ["xyzhgwf@hotmail.com"],
        "smtp_server_addr": "smtp.office365.com",
        "smtp_port": 587,
        "smtp_username": "xyzhgwf@hotmail.com",
        "smtp_password": "995630g17",
        "smtp_sender_display_name": "CAIFUBAO",
    }
