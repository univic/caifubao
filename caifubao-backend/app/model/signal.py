from mongoengine import Document, StringField, EmbeddedDocumentListField, DateTimeField, ReferenceField, ListField, \
    EmbeddedDocument, FloatField, IntField, EmbeddedDocumentField


class Signal(Document):
    """
    category: 0: spot, 1: cont
    """
    code = StringField()
    name = StringField()
    category = StringField()


class SignalData(Document):
    meta = {
        'allow_inheritance': True,
        # 'indexes': [
        #     '-date',
        #     ('name', 'code')
        # ]
    }
    name = StringField(unique_with=['date', 'code'])
    date = DateTimeField()
    stock_code = StringField()


class SpotSignalData(SignalData):
    pass


class ContinuousSignalData(SignalData):
    active_status = StringField()
    expire_date = StringField()
