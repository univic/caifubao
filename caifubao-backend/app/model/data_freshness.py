import datetime
from mongoengine import Document, StringField, EmbeddedDocumentListField, DateTimeField, ReferenceField, ListField, \
    EmbeddedDocument, FloatField, IntField, EmbeddedDocumentField


class DataFreshnessMeta(Document):
    stock_code = StringField()
    type = StringField()
    name = StringField()
    freshness_datetime = DateTimeField()
    backtest_id = StringField()
    created_at = DateTimeField(default=datetime.datetime.now())
