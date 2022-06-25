from mongoengine import Document, StringField, EmbeddedDocumentListField, DateTimeField, ReferenceField, ListField, \
    EmbeddedDocument, FloatField, IntField, EmbeddedDocumentField


class Portfolio(Document):
    pass


class PortfolioTransaction(Document):
    pass


class PortfolioNetworth(Document):
    pass
