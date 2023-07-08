# -*- coding: utf-8 -*-
# Author : univic
# Date: 2023-07-01


from app.lib.datahub import Datahub


class MainDispatcher(object):

    def __init__(self):
        pass

    @staticmethod
    def dispatch():
        datahub = Datahub()
        datahub.initialize()


