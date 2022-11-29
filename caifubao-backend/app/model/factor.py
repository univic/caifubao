from mongoengine import DynamicDocument, StringField, EmbeddedDocumentListField, DateTimeField, ReferenceField, ListField, \
    EmbeddedDocument, FloatField, IntField, EmbeddedDocumentField


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
    stock_code = StringField(unique_with=['date', 'name'])
    category = StringField()
    date = DateTimeField()
    value = FloatField()
