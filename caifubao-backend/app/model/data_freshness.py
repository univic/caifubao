import datetime
from mongoengine import Document, StringField, EmbeddedDocumentListField, DateTimeField, ReferenceField, ListField, \
    EmbeddedDocument, FloatField, IntField, EmbeddedDocumentField


class DataFreshnessMeta(Document):
    stock_code = StringField()
    module_name = StringField()
    analyte_name = StringField()
    meta_type = StringField()
    meta_datetime = DateTimeField()
    backtest_id = StringField()
    created_at = DateTimeField(default=datetime.datetime.now())
