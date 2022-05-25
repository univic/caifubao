# -*- coding: utf-8 -*-
# Author : univic
# Date: 2022-01-15

from mongoengine import StringField, EmbeddedDocumentListField, DateTimeField, ReferenceField, ListField, \
    EmbeddedDocument, FloatField, IntField
from app.lib.db_tool.mongoengine_tool import db


class StockExchange(db.Document):
    name = StringField()
    code = StringField()
    region = StringField()
    trade_calendar = ListField(DateTimeField())
    market_list = ListField(ReferenceField('FinanceMarket'))


class FinanceMarket(db.Document):
    name = StringField()
    code = StringField()
    exchange = ReferenceField(StockExchange)
    overview = EmbeddedDocumentListField('MarketOverview')


class MarketOverview(EmbeddedDocument):
    date = DateTimeField()
    total_market_value = FloatField()
    average_pe = FloatField()
    transaction_volume = FloatField()
    transaction_amount = FloatField()
    report_date = DateTimeField()
    listing_number = IntField()
    turnover_rate = FloatField()
    circulating_market_value = FloatField()
    circulating_turnover_rate = FloatField()

