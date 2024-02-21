from mongoengine import Document, StringField, EmbeddedDocumentListField, DateTimeField, ReferenceField, ListField, \
    EmbeddedDocument, FloatField, IntField, EmbeddedDocumentField


class Portfolio(Document):
    name = StringField()


class PortfolioTransaction(Document):
    pass
