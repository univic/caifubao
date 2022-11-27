from mongoengine import Document, StringField, EmbeddedDocumentListField, DateTimeField, ReferenceField, ListField, \
    EmbeddedDocument, FloatField, IntField, EmbeddedDocumentField


class DataFreshnessMeta(Document):
    code = StringField()
    name = StringField()
    category = StringField()
    date = DateTimeField()
