import datetime
from mongoengine import Document, StringField, EmbeddedDocumentListField, DateTimeField, ReferenceField, ListField, \
    EmbeddedDocument, FloatField, IntField, EmbeddedDocumentField


class DataFreshnessMeta(Document):
    code = StringField()
    obj_type = StringField()
    meta_type = StringField()
    meta_name = StringField()
    freshness_datetime = DateTimeField()
    backtest_name = StringField()
    created_at = DateTimeField(default=datetime.datetime.now())
