from mongoengine import DynamicDocument, StringField, EmbeddedDocumentListField, DateTimeField, ReferenceField, ListField, \
    EmbeddedDocument, FloatField, IntField, EmbeddedDocumentField
from app.model.stock import BasicStock


class FactorDataEntry(DynamicDocument):
    """
    category: stock, market
    """
    meta = {
        'indexes': [
            '-date',
            ('name', 'target_code')
        ]
    }
    name = StringField()
    stock = ReferenceField(BasicStock)
    stock_name = StringField()
    stock_code = StringField()
    category = StringField()
    date = DateTimeField()
    value = FloatField()
