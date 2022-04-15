# -*- coding: utf-8 -*-
# Author : univic


from app.config.dev_config import DevConfig


class ProductionConfig(DevConfig):

    # 生产环境下web端的url
    WEB_BASE_URL = ""

