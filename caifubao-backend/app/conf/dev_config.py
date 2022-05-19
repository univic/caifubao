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
