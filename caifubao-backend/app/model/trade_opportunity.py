from mongoengine import Document, StringField, EmbeddedDocumentListField, DateTimeField, ReferenceField, ListField, \
    EmbeddedDocument, FloatField, IntField, EmbeddedDocumentField


class TradeOpportunity(Document):
    name = StringField()
    date = DateTimeField()
    stock_code = StringField()
    direction = StringField()          # LONG or SHORT
